"""天翼云盘 —— HTTP 路由模块。

为本地 aiohttp 服务提供登录桥接 API:
  - GET  /login_bridge.html         登录桥接页面
  - POST /api/tianyi/login/check    快速检查登录态
  - POST /api/tianyi/login/qr/start 启动二维码会话
  - GET  /api/tianyi/login/qr/status 轮询二维码状态
  - POST /api/tianyi/login/qr/stop  停止二维码会话
  - GET  /api/tianyi/login/qr/image 返回二维码图片
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from collections import OrderedDict
from pathlib import Path
from typing import Any, Optional
from aiohttp import web

_LOGGER = logging.getLogger("NewFreedeck")

# ── 封面缓存 ─────────────────────────────

class CoverCache:
    """LRU 内存缓存：存储封面图片字节 + content-type。"""

    def __init__(self, max_items: int = 300, max_bytes: int = 80 * 1024 * 1024):
        self._store: OrderedDict[str, tuple[bytes, str, float]] = OrderedDict()
        self._max_items = max_items
        self._max_bytes = max_bytes
        self._cur_bytes = 0

    def get(self, key: str) -> tuple[bytes, str] | None:
        entry = self._store.get(key)
        if entry is None:
            return None
        self._store.move_to_end(key)
        return entry[:2]

    def set(self, key: str, data: bytes, content_type: str):
        if key in self._store:
            _, old_ct, _ = self._store[key]
            self._cur_bytes -= len(data)
        size = len(data)
        while (len(self._store) >= self._max_items or
               self._cur_bytes + size > self._max_bytes) and self._store:
            _, (old_data, _, _) = self._store.popitem(last=False)
            self._cur_bytes -= len(old_data)
        self._store[key] = (data, content_type, time.monotonic())
        self._cur_bytes += size
        self._store.move_to_end(key)

    def stats(self) -> dict:
        return {"items": len(self._store), "bytes": self._cur_bytes}

def _login_bridge_dir() -> Path:
    """返回 login_bridge.html 所在目录，优先用 Decky 注入的插件目录。"""
    try:
        import decky
        return Path(decky.DECKY_PLUGIN_DIR) / "defaults" / "tianyi_library_ui"
    except Exception:
        return Path(__file__).resolve().parents[2] / "defaults" / "tianyi_library_ui"

def _emulator_icons_dir() -> Path:
    """返回模拟器图标所在目录。"""
    try:
        import decky
        return Path(decky.DECKY_PLUGIN_DIR) / "dist" / "emulator_icons"
    except Exception:
        return Path(__file__).resolve().parents[2] / "dist" / "emulator_icons"


class TianyiHttpRoutes:
    """天翼 HTTP 路由处理器。

    绑定到 aiohttp Application 上。
    """

    def __init__(self, qr_login_manager: Any):
        self._qr = qr_login_manager
        self._cover_session: Any = None   # 共享 aiohttp ClientSession（连接池）
        self._cover_cache = CoverCache()
        self._cover_disk_dir: Path | None = None

    @staticmethod
    def _cover_disk_key(url: str) -> str:
        """URL → 安全文件名（SHA256 前 32 位 hex）。"""
        import hashlib
        return hashlib.sha256(url.encode()).hexdigest()[:32]

    def _cover_disk_path(self, url: str) -> Path:
        """获取封面磁盘路径（使用可写目录）。"""
        if self._cover_disk_dir is None:
            home = os.environ.get("DECKY_USER_HOME", os.path.expanduser("~"))
            base = Path(home) / ".local" / "share" / "NewFreedeck" / "cache" / "covers"
            base.mkdir(parents=True, exist_ok=True)
            self._cover_disk_dir = base
        key = self._cover_disk_key(url)
        # 两层子目录避免单目录文件过多
        sub = self._cover_disk_dir / key[:2]
        sub.mkdir(parents=True, exist_ok=True)
        return sub / key

    async def _ensure_cover_session(self):
        if self._cover_session is None or self._cover_session.closed:
            import aiohttp, ssl
            ssl_ctx = ssl.create_default_context()
            ssl_ctx.check_hostname = False
            ssl_ctx.verify_mode = ssl.CERT_NONE
            conn = aiohttp.TCPConnector(
                limit=20, limit_per_host=8, ttl_dns_cache=300,
                ssl=ssl_ctx,
            )
            self._cover_session = aiohttp.ClientSession(
                connector=conn,
                timeout=aiohttp.ClientTimeout(total=10),
            )

    def setup(self, app: web.Application) -> None:
        """注册所有路由到 aiohttp app。"""
        app.router.add_get("/login_bridge.html", self._handle_login_bridge)
        app.router.add_get("/login_bridge.js", self._handle_login_bridge_js)
        app.router.add_post("/api/tianyi/login/check", self._handle_login_check)
        app.router.add_post("/api/tianyi/login/qr/start", self._handle_qr_start)
        app.router.add_get("/api/tianyi/login/qr/status", self._handle_qr_status)
        app.router.add_post("/api/tianyi/login/qr/stop", self._handle_qr_stop)
        app.router.add_get("/api/tianyi/login/qr/image", self._handle_qr_image)
        app.router.add_get("/proxy/cover", self._handle_cover_proxy)
        app.router.add_get("/emulator_icons/{name}", self._handle_emulator_icon)
        _LOGGER.info("Tianyi HTTP routes registered")

    # ── 静态页面 ──────────────────────────

    async def _handle_login_bridge(self, _request: web.Request) -> web.Response:
        path = _login_bridge_dir() / "login_bridge.html"
        if not path.exists():
            return web.Response(text="login_bridge.html not found", status=404)
        return web.FileResponse(path)

    async def _handle_login_bridge_js(self, _request: web.Request) -> web.Response:
        path = _login_bridge_dir() / "login_bridge.js"
        if not path.exists():
            return web.Response(text="login_bridge.js not found", status=404)
        return web.FileResponse(path)

    # ── 模拟器图标 ──────────────────────────

    async def _handle_emulator_icon(self, request: web.Request) -> web.Response:
        """返回模拟器图标文件（webp/svg/png）。"""
        name = request.match_info.get("name", "")
        if not name or "/" in name or ".." in name:
            return web.Response(text="bad name", status=400)
        path = _emulator_icons_dir() / name
        if not path.exists():
            return web.Response(text="not found", status=404)
        return web.FileResponse(path)

    # ── 封面代理 ──────────────────────────

    async def _handle_cover_proxy(self, request: web.Request) -> web.Response:
        """代理封面图片 — 内存缓存 → 磁盘缓存 → 源站回源。"""
        url = request.query.get("url", "")
        if not url or not url.startswith("http"):
            return web.Response(text="bad url", status=400)

        cache_headers = {"Cache-Control": "public, max-age=86400"}

        # ① 内存缓存命中
        cached = self._cover_cache.get(url)
        if cached is not None:
            body, ct = cached
            return web.Response(body=body, content_type=ct, headers=cache_headers)

        # ② 磁盘缓存命中 → 升入内存 → 返回
        disk_path = self._cover_disk_path(url)
        if disk_path.exists():
            try:
                body = disk_path.read_bytes()
                ct = "image/png"
                self._cover_cache.set(url, body, ct)
                return web.Response(body=body, content_type=ct, headers=cache_headers)
            except Exception:
                pass

        # ③ 回源拉取 → 写内存 + 写磁盘 → 返回
        try:
            await self._ensure_cover_session()
            async with self._cover_session.get(url) as resp:
                body = await resp.read()
                ct = resp.content_type or "image/png"
                self._cover_cache.set(url, body, ct)
                try:
                    disk_path.parent.mkdir(parents=True, exist_ok=True)
                    with open(disk_path, "wb") as f:
                        f.write(body)
                except Exception:
                    pass
                return web.Response(body=body, content_type=ct, headers=cache_headers)
        except Exception as e:
            _LOGGER.warning("Cover proxy failed for %s: %s", url[:120], e)
            return web.Response(text=str(e), status=502)

    # ── 登录检查 ──────────────────────────

    async def _handle_login_check(self, request: web.Request) -> web.Response:
        try:
            data = await self._qr.check_login_state()
            return _json_ok(data)
        except Exception as exc:
            return _json_error(exc)

    # ── QR 启动 ───────────────────────────

    async def _handle_qr_start(self, _request: web.Request) -> web.Response:
        try:
            data = await self._qr.start_qr_login()
            return _json_ok(data)
        except Exception as exc:
            return _json_error(exc)

    # ── QR 状态 ───────────────────────────

    async def _handle_qr_status(self, request: web.Request) -> web.Response:
        session_id = str(request.query.get("session_id", "")).strip()
        try:
            data = await self._qr.poll_qr_login(session_id=session_id)
            return _json_ok(data)
        except Exception as exc:
            return _json_error(exc)

    # ── QR 停止 ───────────────────────────

    async def _handle_qr_stop(self, request: web.Request) -> web.Response:
        try:
            body = await request.json()
        except Exception:
            body = {}
        session_id = str(body.get("session_id", "")).strip()
        try:
            data = await self._qr.stop_qr_login(session_id=session_id)
            return _json_ok(data)
        except Exception as exc:
            return _json_error(exc)

    # ── QR 图片 ───────────────────────────

    async def _handle_qr_image(self, request: web.Request) -> web.Response:
        session_id = str(request.query.get("session_id", "")).strip()
        try:
            img_bytes, content_type = await self._qr.get_qr_image(session_id=session_id)
            return web.Response(body=img_bytes, content_type=content_type)
        except Exception as exc:
            _LOGGER.warning("QR image fetch failed: %s", exc)
            return _json_error(exc)


# ── JSON 辅助 ─────────────────────────────

def _json_ok(data: object) -> web.Response:
    import json
    return web.json_response({"status": "success", "data": data})


def _json_error(exc: Exception, status: int = 500) -> web.Response:
    import json
    return web.json_response(
        {"status": "error", "message": str(exc)},
        status=status,
    )
