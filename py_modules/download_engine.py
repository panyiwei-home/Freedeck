"""aria2 下载引擎 — 进程管理 + JSON-RPC 客户端。

基于 aria2-下载引擎方案.md 实现：
  - 内嵌 aria2c 二进制 (defaults/bin/aria2c)
  - JSON-RPC 2.0 over HTTP, 自动分配端口
  - 随机 rpc-secret 保证安全
  - 断点续传 / 分片下载 / 并发控制
"""

from __future__ import annotations

import os
import socket
import subprocess
import time
import uuid
import asyncio
import json as _json
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import aiohttp

# ── 常量 ──────────────────────────────────

ARIA2_CONF = {
    "continue": "true",
    "allow-overwrite": "true",
    "auto-file-renaming": "false",
    "max-concurrent-downloads": "5",
    "max-connection-per-server": "16",
    "split": "16",
    "min-split-size": "1M",
    "max-tries": "0",              # 无限重试
    "retry-wait": "5",
    "connect-timeout": "10",
    "timeout": "30",
    "file-allocation": "none",     # Steam Deck SD 卡不预分配
    "disk-cache": "32M",
    "check-integrity": "true",
    "always-resume": "true",
    "summary-interval": "0",       # 不输出摘要日志
}

UA = (
    "Mozilla/5.0 (X11; Linux x86_64; Valve Steam Gamepad) "
    "AppleWebKit/537.36 Chrome/126.0.6478.183 Safari/537.36"
)


# ── 数据模型 ──────────────────────────────

@dataclass
class DownloadState:
    """单个文件的下载状态。"""
    gid: str = ""
    task_id: str = ""
    game_id: str = ""
    game_title: str = ""
    file_name: str = ""
    file_size: int = 0
    status: str = "pending"   # pending | resolving | downloading | complete | failed | paused
    progress: float = 0.0     # 0–100
    speed: int = 0            # bytes/s
    completed: int = 0
    total: int = 0
    error: str = ""


# ── 工具函数 ──────────────────────────────

def _pick_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _binary_path(plugin_dir: str) -> str:
    """查找 aria2c 二进制。"""
    path = os.path.join(plugin_dir, "defaults", "bin", "aria2c")
    if os.path.isfile(path):
        return path
    raise RuntimeError(f"aria2c not found at {path}")


# ── aria2 JSON-RPC 客户端 ────────────────

class Aria2Rpc:
    """轻量 JSON-RPC 2.0 客户端。"""

    def __init__(self, url: str, secret: str):
        self._url = url
        self._secret = secret
        self._counter = 0

    async def call(self, method: str, params: Optional[List[Any]] = None) -> Any:
        self._counter += 1
        payload = {
            "jsonrpc": "2.0",
            "id": str(self._counter),
            "method": method,
            "params": [f"token:{self._secret}"] + (params or []),
        }
        timeout = aiohttp.ClientTimeout(total=12)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(self._url, json=payload) as resp:
                data = await resp.json(content_type=None)
        if not isinstance(data, dict):
            raise RuntimeError("aria2 响应格式异常")
        err = data.get("error")
        if err:
            msg = err.get("message", "unknown") if isinstance(err, dict) else str(err)
            raise RuntimeError(f"aria2 RPC error: {msg}")
        return data.get("result")

    async def health(self) -> bool:
        try:
            await self.call("aria2.getVersion")
            return True
        except Exception:
            return False

    # ── 便捷方法 ──

    async def add_uri(self, uri: str, dir_: str, out: str,
                      cookie: str = "", split: int = 16) -> str:
        headers = [
            f"User-Agent: {UA}",
            "Referer: https://cloud.189.cn/",
        ]
        if cookie:
            headers.append(f"Cookie: {cookie}")

        options = {
            "dir": dir_,
            "out": out,
            "header": headers,
            "split": str(min(split, 16)),
            "max-connection-per-server": "16",
            "continue": "true",
            "allow-overwrite": "true",
            "auto-file-renaming": "false",
        }
        result = await self.call("aria2.addUri", [[uri], options])
        gid = str(result or "").strip()
        if not gid:
            raise RuntimeError("aria2.addUri 返回空 gid")
        return gid

    async def tell_status(self, gid: str) -> dict:
        keys = ["status", "totalLength", "completedLength", "downloadSpeed",
                "errorMessage", "errorCode", "files"]
        result = await self.call("aria2.tellStatus", [gid, keys])
        return result if isinstance(result, dict) else {}

    async def tell_active(self) -> List[dict]:
        keys = ["gid", "status", "totalLength", "completedLength", "downloadSpeed"]
        result = await self.call("aria2.tellActive", [keys])
        return result if isinstance(result, list) else []

    async def pause(self, gid: str) -> None:
        await self.call("aria2.forcePause", [gid])

    async def resume(self, gid: str) -> None:
        await self.call("aria2.unpause", [gid])

    async def remove(self, gid: str) -> None:
        try:
            await self.call("aria2.remove", [gid])
        except Exception:
            pass
        try:
            await self.call("aria2.removeDownloadResult", [gid])
        except Exception:
            pass


# ── 进程管理器 ────────────────────────────

class DownloadEngine:
    """管理 aria2c 进程生命周期，对外暴露 Aria2Rpc。"""

    def __init__(self, plugin_dir: str, work_dir: str):
        self._plugin_dir = plugin_dir
        self._work_dir = os.path.realpath(work_dir)
        os.makedirs(self._work_dir, exist_ok=True)
        self._proc: Optional[subprocess.Popen] = None
        self.rpc: Optional[Aria2Rpc] = None

    @property
    def ready(self) -> bool:
        return self.rpc is not None

    async def start(self) -> None:
        bin_ = _binary_path(self._plugin_dir)
        try:
            mode = os.stat(bin_).st_mode
            if not (mode & 0o111):
                os.chmod(bin_, mode | 0o755)
        except Exception:
            pass

        port = _pick_port()
        secret = uuid.uuid4().hex
        session_file = os.path.join(self._work_dir, "aria2.session")

        args = [
            bin_,
            "--enable-rpc=true",
            "--rpc-listen-all=false",
            "--rpc-allow-origin-all=true",
            "--rpc-listen-port", str(port),
            "--rpc-secret", secret,
            "--dir", self._work_dir,
            "--continue=true",
            "--max-concurrent-downloads=5",
            "--max-connection-per-server=16",
            "--split=16",
            "--min-split-size=1M",
            "--max-tries=0",
            "--retry-wait=5",
            "--connect-timeout=10",
            "--timeout=30",
            "--file-allocation=none",
            "--disk-cache=32M",
            "--check-integrity=true",
            "--always-resume=true",
            "--allow-overwrite=true",
            "--auto-file-renaming=false",
            "--daemon=false",
            "--summary-interval=0",
            "--save-session", session_file,
            "--save-session-interval=15",
            "--input-file", session_file if os.path.exists(session_file) else "/dev/null",
            "--log", os.path.join(self._work_dir, "aria2.log"),
            "--log-level=notice",
        ]
        # CA 证书
        for ca in ("/etc/ssl/certs/ca-certificates.crt",
                   "/etc/pki/tls/certs/ca-bundle.crt",
                   "/etc/ssl/cert.pem"):
            if os.path.isfile(ca):
                args.extend(["--ca-certificate", ca])
                break

        self._proc = subprocess.Popen(
            args,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            cwd=self._work_dir,
        )

        rpc = Aria2Rpc(f"http://127.0.0.1:{port}/jsonrpc", secret)
        for _ in range(30):
            if await rpc.health():
                self.rpc = rpc
                return
            await asyncio.sleep(0.2)

        self.stop()
        raise RuntimeError("aria2c RPC 启动超时")

    def stop(self):
        if self.rpc:
            self.rpc = None
        if self._proc and self._proc.poll() is None:
            try:
                self._proc.terminate()
                self._proc.wait(timeout=3)
            except Exception:
                try:
                    self._proc.kill()
                except Exception:
                    pass
