# tianyi_service.py - 天翼下载业务编排
#
# 该模块串联目录、登录态、直链与下载任务。

from __future__ import annotations

import asyncio
import json
import os
import re
import shutil
import sqlite3
import ssl
import tempfile
import time
import uuid
from dataclasses import asdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple
from urllib.parse import parse_qs, quote, urlparse

import aiohttp
import decky
from yarl import URL

import config
from aria2_manager import Aria2Error, Aria2Manager
from game_catalog import GameCatalog, resolve_default_catalog_path
from seven_zip_manager import SevenZipError, SevenZipManager
from steam_shortcuts import (
    add_or_update_tianyi_shortcut,
    list_tianyi_shortcuts_sync,
    remove_tianyi_shortcut,
    resolve_tianyi_shortcut_sync,
)
from tianyi_client import (
    TianyiApiError,
    download_cloud_archive,
    fetch_access_token,
    fetch_download_url,
    get_user_account,
    list_cloud_archives,
    resolve_share,
    upload_archive_to_cloud,
)
from tianyi_store import TianyiInstalledGame, TianyiStateStore, TianyiTaskRecord


TASK_RETENTION_SECONDS = 7 * 24 * 3600
PANEL_TASK_REFRESH_TIMEOUT_SECONDS = 2.0
LOCAL_WEB_READY_TIMEOUT_SECONDS = 3.0
LOCAL_WEB_PROBE_TIMEOUT_SECONDS = 1.2
CAPTURE_DEFAULT_TIMEOUT_SECONDS = 240
CAPTURE_MIN_TIMEOUT_SECONDS = 30
CAPTURE_MAX_TIMEOUT_SECONDS = 600
CDP_ENDPOINT_PORTS = (8080, 9222)
TIANYI_HOST_KEYWORDS = ("cloud.189.cn", "h5.cloud.189.cn", "open.e.189.cn", "189.cn")
CAPTURE_LOOP_WINDOW = 8
CAPTURE_LOOP_CORE_HOSTS = ("cloud.189.cn", "h5.cloud.189.cn")
COOKIE_CAPTURE_SOURCES = ("cdp", "cookie_db")
COOKIE_DB_MAX_ROWS = 600
QR_LOGIN_SESSION_TIMEOUT_SECONDS = 300
QR_LOGIN_HTTP_TIMEOUT_SECONDS = 20
CATALOG_COVER_CACHE_TTL_SECONDS = 7 * 24 * 3600
CATALOG_COVER_NEGATIVE_TTL_SECONDS = 1800
CATALOG_COVER_HTTP_TIMEOUT_SECONDS = 6.0
CATALOG_COVER_SEARCH_LIMIT = 8
PROTONDB_HTTP_TIMEOUT_SECONDS = 4.5
HLTB_CACHE_TTL_SECONDS = 7 * 24 * 3600
HLTB_NEGATIVE_TTL_SECONDS = 60
HLTB_HTTP_TIMEOUT_SECONDS = 4.0
HLTB_SEARCH_LIMIT = 12
HLTB_TOKEN_URL = "https://howlongtobeat.com/api/finder/init"
HLTB_SEARCH_URL = "https://howlongtobeat.com/api/finder"
HLTB_LEGACY_SEARCH_URL = "https://howlongtobeat.com/api/search"
PANEL_POLL_MODE_ACTIVE = "active"
PANEL_POLL_MODE_IDLE = "idle"
PANEL_POLL_MODE_BACKGROUND = "background"
PANEL_TASK_REFRESH_ACTIVE_SECONDS = 1.0
PANEL_TASK_REFRESH_IDLE_SECONDS = 10.0
PANEL_TASK_REFRESH_BACKGROUND_SECONDS = 30.0
PANEL_INSTALLED_REFRESH_ACTIVE_SECONDS = 20.0
PANEL_INSTALLED_REFRESH_IDLE_SECONDS = 60.0
PANEL_INSTALLED_REFRESH_BACKGROUND_SECONDS = 120.0

QR_STATUS_SUCCESS = 0
QR_STATUS_WAITING = {-106}
QR_STATUS_SCANNED_WAIT_CONFIRM = {-11002}
QR_STATUS_EXPIRED = {-11001, -20099}
QR_STATUS_NEED_EXTRA_VERIFY = {-134}
QR_CA_CANDIDATE_FILES = (
    "/etc/ssl/certs/ca-certificates.crt",
    "/etc/pki/tls/certs/ca-bundle.crt",
    "/etc/ssl/cert.pem",
    "/etc/openssl/certs/ca-certificates.crt",
)
ARCHIVE_SUFFIXES = {
    ".zip",
    ".tar",
    ".tgz",
    ".tar.gz",
    ".tbz",
    ".tbz2",
    ".tar.bz2",
    ".txz",
    ".tar.xz",
    ".7z",
    ".rar",
}
CLOUD_SAVE_TASK_STAGES = {
    "idle",
    "scanning",
    "packaging",
    "uploading",
    "completed",
    "failed",
}
CLOUD_SAVE_RESTORE_TASK_STAGES = {
    "idle",
    "listing",
    "planning",
    "ready",
    "applying",
    "completed",
    "failed",
}
CLOUD_SAVE_DATE_FORMAT = "%Y%m%d_%H%M%S"
CLOUD_SAVE_UPLOAD_ROOT = "FreedeckCloudSaves"
CLOUD_SAVE_PROTON_BASE_DIRS = (
    ("Documents", "My Games"),
    ("Saved Games",),
    ("AppData", "Roaming"),
    ("AppData", "Local"),
    ("AppData", "LocalLow"),
)
CLOUD_SAVE_INSTALL_FALLBACK_DIRS = (
    "save",
    "saves",
    "saved",
    "userdata",
    "profiles",
)
CLOUD_SAVE_SCAN_MAX_DEPTH = 6
CLOUD_SAVE_SCAN_MAX_MATCHES = 32
CLOUD_SAVE_MAX_SOURCE_PATHS = 24
CLOUD_SAVE_RESTORE_CONFLICT_SAMPLES = 16
PLAYTIME_SESSION_MAX_SECONDS = 12 * 3600
PLAYTIME_STALE_SESSION_SECONDS = 3 * 24 * 3600

def _now_wall_ts() -> int:
    """返回当前 wall-clock 秒级时间戳。"""
    return int(time.time())


def _safe_int(value: Any, default: int = 0) -> int:
    """安全解析整数。"""
    try:
        return int(value)
    except Exception:
        return default


def _format_size_bytes(size_bytes: int) -> str:
    """将字节数格式化为易读文本。"""
    value = float(max(0, int(size_bytes or 0)))
    units = ["B", "KB", "MB", "GB", "TB"]
    unit = units[0]
    for unit in units:
        if value < 1024.0 or unit == units[-1]:
            break
        value /= 1024.0
    if unit == "B":
        return f"{int(value)} {unit}"
    return f"{value:.2f} {unit}"


def _format_playtime_seconds(total_seconds: int) -> str:
    """将累计游玩秒数格式化为可读文本。"""
    seconds = max(0, int(total_seconds or 0))
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    if hours > 0:
        return f"{hours} 小时 {minutes} 分钟"
    if minutes > 0:
        return f"{minutes} 分钟"
    return "0 分钟"


def _format_hours_value(hours_value: Any) -> str:
    """将小时数格式化为可读文本。"""
    try:
        value = float(hours_value)
    except Exception:
        value = 0.0
    if value <= 0:
        return "-"
    rounded = round(value, 1)
    if abs(rounded - round(rounded)) < 0.05:
        return f"{int(round(rounded))} 小时"
    return f"{rounded:.1f} 小时"


def _disk_free_bytes(path: str) -> int:
    """获取目标目录所在分区的可用空间。"""
    target = os.path.realpath(os.path.expanduser(str(path or "").strip()))
    if not target:
        raise ValueError("目录无效")
    os.makedirs(target, exist_ok=True)
    usage = shutil.disk_usage(target)
    return int(usage.free)


def _task_to_view(task: TianyiTaskRecord) -> Dict[str, Any]:
    """转换任务展示结构。"""
    return {
        "task_id": task.task_id,
        "game_id": task.game_id,
        "game_title": task.game_title,
        "file_name": task.file_name,
        "status": task.status,
        "progress": round(float(task.progress), 2),
        "speed": int(task.speed),
        "error_reason": task.error_reason,
        "install_status": task.install_status,
        "install_message": task.install_message,
        "installed_path": task.installed_path,
        "updated_at": task.updated_at,
    }


def _is_terminal(status: str) -> bool:
    """判断任务是否终态。"""
    return status in {"complete", "error", "removed"}


class LocalWebNotReadyError(RuntimeError):
    """本地网页未就绪异常，附带结构化诊断信息。"""

    def __init__(self, message: str, *, reason: str, diagnostics: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.reason = str(reason or "local_web_not_ready")
        self.diagnostics = diagnostics or {}


class TianyiService:
    """天翼下载业务入口。"""

    def __init__(self, plugin: Any):
        self.plugin = plugin
        plugin_dir = str(getattr(decky, "DECKY_PLUGIN_DIR", Path.cwd()))
        state_root = config.DECKY_SEND_DIR
        try:
            os.makedirs(state_root, exist_ok=True)
        except Exception:
            # 某些开发环境中家目录不可写，回退到插件目录下的临时目录。
            state_root = os.path.join(plugin_dir, ".tmp", "Decky-send")

        state_dir = os.path.join(state_root, "tianyi")
        state_file = os.path.join(state_dir, "state.json")
        self.store = TianyiStateStore(state_file)
        self.catalog = GameCatalog(resolve_default_catalog_path())
        self.aria2 = Aria2Manager(plugin_dir=plugin_dir, work_dir=os.path.join(state_dir, "aria2"))
        self.seven_zip = SevenZipManager(plugin_dir=plugin_dir)
        self._lock = asyncio.Lock()
        self._post_process_jobs: Dict[str, asyncio.Task] = {}

        # 登录采集状态机（内存态）。
        self._capture_state: Dict[str, Any] = {
            "stage": "idle",
            "message": "未开始",
            "reason": "",
            "next_action": "",
            "user_account": "",
            "updated_at": _now_wall_ts(),
            "diagnostics": {},
            "source_attempts": [],
            "success_source": "",
            "source_diagnostics": {},
        }
        self._capture_task: Optional[asyncio.Task] = None
        self._capture_lock = asyncio.Lock()
        self._qr_login_lock = asyncio.Lock()
        self._qr_login_state: Dict[str, Any] = {
            "session_id": "",
            "stage": "idle",
            "message": "未开始",
            "reason": "",
            "next_action": "",
            "user_account": "",
            "image_url": "",
            "expires_at": 0,
            "updated_at": _now_wall_ts(),
            "diagnostics": {},
        }
        self._qr_login_context: Optional[Dict[str, Any]] = None
        self._catalog_cover_cache: Dict[str, Dict[str, Any]] = {}
        self._catalog_cover_lock = asyncio.Lock()
        self._hltb_cache: Dict[str, Dict[str, Any]] = {}
        self._hltb_lock = asyncio.Lock()
        self._panel_cache_lock = asyncio.Lock()
        self._panel_tasks_cache: List[Dict[str, Any]] = []
        self._panel_tasks_cache_at = 0.0
        self._panel_installed_cache: Dict[str, Any] = {"total": 0, "preview": []}
        self._panel_installed_cache_at = 0.0
        self._panel_last_expensive_refresh_at = 0.0
        self._panel_last_mode = PANEL_POLL_MODE_IDLE
        self._panel_last_active_tasks = 0
        self._cloud_save_lock = asyncio.Lock()
        self._cloud_save_task: Optional[asyncio.Task] = None
        self._cloud_save_state: Dict[str, Any] = {
            "stage": "idle",
            "message": "未开始",
            "reason": "",
            "running": False,
            "progress": 0.0,
            "current_game": "",
            "total_games": 0,
            "processed_games": 0,
            "uploaded": 0,
            "skipped": 0,
            "failed": 0,
            "results": [],
            "diagnostics": {},
            "updated_at": _now_wall_ts(),
            "last_result": {},
        }
        self._cloud_save_restore_lock = asyncio.Lock()
        self._cloud_save_restore_state: Dict[str, Any] = {
            "stage": "idle",
            "message": "未开始",
            "reason": "",
            "running": False,
            "progress": 0.0,
            "target_game_id": "",
            "target_game_title": "",
            "target_game_key": "",
            "target_version": "",
            "selected_entry_ids": [],
            "selected_target_dir": "",
            "requires_confirmation": False,
            "conflict_count": 0,
            "conflict_samples": [],
            "restored_files": 0,
            "restored_entries": 0,
            "results": [],
            "diagnostics": {},
            "updated_at": _now_wall_ts(),
            "last_result": {},
        }
        self._cloud_save_restore_plan: Dict[str, Any] = {}
        self._playtime_lock = asyncio.Lock()
        self._playtime_sessions: Dict[str, Dict[str, Any]] = {}

    async def initialize(self) -> None:
        """初始化状态与目录。"""
        os.makedirs(os.path.dirname(self.store.state_file), exist_ok=True)
        await asyncio.to_thread(self.store.load)
        await asyncio.to_thread(self.catalog.load)
        if not self.store.settings.download_dir:
            default_dir = getattr(self.plugin, "downloads_dir", config.DOWNLOADS_DIR)
            self.store.set_settings(download_dir=default_dir)
        if not self.store.settings.install_dir:
            default_install = os.path.join(self.store.settings.download_dir or config.DOWNLOADS_DIR, "installed")
            os.makedirs(default_install, exist_ok=True)
            self.store.set_settings(install_dir=default_install)
        # 自动安装能力固定开启，避免 UI 配置分叉造成行为不一致。
        if not bool(self.store.settings.auto_install):
            self.store.set_settings(auto_install=True)
        self._cloud_save_state["last_result"] = dict(self.store.cloud_save_last_result or {})
        self._cloud_save_restore_state["last_result"] = dict(self.store.cloud_save_restore_last_result or {})
        await self._recover_playtime_sessions_from_store()

    async def shutdown(self) -> None:
        """关闭后台资源。"""
        async with self._capture_lock:
            if self._capture_task and not self._capture_task.done():
                self._capture_task.cancel()
                try:
                    await self._capture_task
                except BaseException:
                    pass
            self._capture_task = None
        async with self._qr_login_lock:
            await self._close_qr_login_context_locked()
        await self._finalize_active_playtime_sessions(reason="service_shutdown")
        await self._cancel_cloud_save_task()
        await self._clear_cloud_save_restore_plan()
        jobs = list(self._post_process_jobs.values())
        for job in jobs:
            if not job.done():
                job.cancel()
        for job in jobs:
            try:
                await job
            except BaseException:
                pass
        self._post_process_jobs.clear()
        await asyncio.to_thread(self.aria2.stop)

    def _normalize_panel_mode(self, context: Optional[Dict[str, Any]] = None) -> tuple[str, bool, bool]:
        """规范化面板轮询模式。"""
        payload = context if isinstance(context, dict) else {}
        mode = str(payload.get("poll_mode", "") or "").strip().lower()
        visible = bool(payload.get("visible", True))
        has_focus = bool(payload.get("has_focus", True))
        if mode not in {PANEL_POLL_MODE_ACTIVE, PANEL_POLL_MODE_IDLE, PANEL_POLL_MODE_BACKGROUND}:
            mode = PANEL_POLL_MODE_BACKGROUND if not visible else PANEL_POLL_MODE_IDLE
        if mode != PANEL_POLL_MODE_BACKGROUND and not visible:
            mode = PANEL_POLL_MODE_BACKGROUND
        return mode, visible, has_focus

    def _panel_task_refresh_window(self, mode: str, active_tasks: int) -> float:
        """按模式与活跃任务数量返回任务刷新窗口。"""
        if mode == PANEL_POLL_MODE_BACKGROUND:
            return PANEL_TASK_REFRESH_BACKGROUND_SECONDS
        if active_tasks > 0:
            return PANEL_TASK_REFRESH_ACTIVE_SECONDS
        return PANEL_TASK_REFRESH_IDLE_SECONDS

    def _panel_installed_refresh_window(self, mode: str, active_tasks: int) -> float:
        """按模式与活跃任务数量返回安装列表刷新窗口。"""
        if mode == PANEL_POLL_MODE_BACKGROUND:
            return PANEL_INSTALLED_REFRESH_BACKGROUND_SECONDS
        if active_tasks > 0:
            return PANEL_INSTALLED_REFRESH_ACTIVE_SECONDS
        return PANEL_INSTALLED_REFRESH_IDLE_SECONDS

    def _count_active_tasks(self, tasks: Sequence[Dict[str, Any]]) -> int:
        """统计非终态任务数量。"""
        count = 0
        for item in tasks:
            status = str((item or {}).get("status", "")).strip().lower()
            if status and not _is_terminal(status):
                count += 1
        return count

    def _invalidate_panel_cache(self, *, tasks: bool = False, installed: bool = False, all_data: bool = False) -> None:
        """失效面板缓存，确保关键变更后可及时刷新。"""
        if all_data or tasks:
            self._panel_tasks_cache = []
            self._panel_tasks_cache_at = 0.0
        if all_data or installed:
            self._panel_installed_cache = {"total": 0, "preview": []}
            self._panel_installed_cache_at = 0.0
        if all_data or tasks or installed:
            self._panel_last_expensive_refresh_at = 0.0

    async def get_panel_state(self, *, request_context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """返回 Decky 面板状态。"""
        async with self._panel_cache_lock:
            now = time.monotonic()
            requested_mode, visible, has_focus = self._normalize_panel_mode(request_context)

            cached_tasks = list(self._panel_tasks_cache)
            if not cached_tasks:
                cached_tasks = [_task_to_view(task) for task in list(self.store.tasks)]

            active_tasks = self._count_active_tasks(cached_tasks)
            effective_mode = requested_mode
            if requested_mode != PANEL_POLL_MODE_BACKGROUND:
                effective_mode = PANEL_POLL_MODE_ACTIVE if active_tasks > 0 else PANEL_POLL_MODE_IDLE

            task_window = self._panel_task_refresh_window(effective_mode, active_tasks)
            installed_window = self._panel_installed_refresh_window(effective_mode, active_tasks)
            tasks_refreshed = False
            installed_refreshed = False

            tasks = cached_tasks
            if now - float(self._panel_tasks_cache_at or 0.0) >= float(task_window):
                try:
                    tasks = await asyncio.wait_for(
                        self.refresh_tasks(sync_aria2=True, persist=False),
                        timeout=PANEL_TASK_REFRESH_TIMEOUT_SECONDS,
                    )
                    self._panel_tasks_cache = list(tasks)
                    self._panel_tasks_cache_at = now
                    tasks_refreshed = True
                except Exception as exc:
                    config.logger.warning("Panel tasks refresh fallback to cache: %s", exc)
                    tasks = list(self._panel_tasks_cache) if self._panel_tasks_cache else list(cached_tasks)

            active_tasks = self._count_active_tasks(tasks)
            if requested_mode != PANEL_POLL_MODE_BACKGROUND:
                effective_mode = PANEL_POLL_MODE_ACTIVE if active_tasks > 0 else PANEL_POLL_MODE_IDLE
                task_window = self._panel_task_refresh_window(effective_mode, active_tasks)
                installed_window = self._panel_installed_refresh_window(effective_mode, active_tasks)

            installed_cached = dict(self._panel_installed_cache or {"total": 0, "preview": []})
            installed_cached["preview"] = list(installed_cached.get("preview") or [])
            installed = installed_cached
            if now - float(self._panel_installed_cache_at or 0.0) >= float(installed_window):
                installed = self._build_installed_summary(limit=60, persist=False)
                self._panel_installed_cache = {
                    "total": int(installed.get("total", 0) or 0),
                    "preview": list(installed.get("preview") or []),
                }
                self._panel_installed_cache_at = now
                installed_refreshed = True
            elif not installed_cached.get("preview"):
                installed = self._build_installed_summary(limit=60, persist=False)
                self._panel_installed_cache = {
                    "total": int(installed.get("total", 0) or 0),
                    "preview": list(installed.get("preview") or []),
                }
                self._panel_installed_cache_at = now
                installed_refreshed = True

            if tasks_refreshed or installed_refreshed:
                self._panel_last_expensive_refresh_at = now

            self._panel_last_mode = effective_mode
            self._panel_last_active_tasks = active_tasks

            last_expensive_at = float(self._panel_last_expensive_refresh_at or 0.0)
            last_task_at = float(self._panel_tasks_cache_at or 0.0)
            last_installed_at = float(self._panel_installed_cache_at or 0.0)

            power_diagnostics = {
                "requested_mode": requested_mode,
                "effective_mode": effective_mode,
                "visible": bool(visible),
                "has_focus": bool(has_focus),
                "active_tasks": int(active_tasks),
                "task_refresh_interval_seconds": float(task_window),
                "installed_refresh_interval_seconds": float(installed_window),
                "tasks_refreshed": bool(tasks_refreshed),
                "installed_refreshed": bool(installed_refreshed),
                "last_expensive_refresh_age_seconds": round(max(0.0, now - last_expensive_at), 3)
                if last_expensive_at > 0
                else -1.0,
                "last_tasks_refresh_age_seconds": round(max(0.0, now - last_task_at), 3) if last_task_at > 0 else -1.0,
                "last_installed_refresh_age_seconds": round(max(0.0, now - last_installed_at), 3)
                if last_installed_at > 0
                else -1.0,
            }

        summary = self.catalog.summary()
        cached_cookie = str(self.store.login.cookie or "").strip()
        cached_account = str(self.store.login.user_account or "").strip()
        login_ok = bool(cached_cookie)
        account = cached_account
        message = "未登录"
        if login_ok:
            message = f"已登录（缓存）：{cached_account or '未知账号'}"

        # 面板轮询走纯缓存快路径，避免本地端口探测与网络校验阻塞 RPC。
        login_url = ""
        library_url = ""

        return {
            "login": {
                "logged_in": login_ok,
                "user_account": account,
                "message": message,
                "login_url": login_url,
            },
            "catalog": {
                "total": summary.get("total", 0),
                "preview": [],
                "path": summary.get("path", ""),
            },
            "installed": installed,
            "tasks": tasks,
            "settings": asdict(self.store.settings),
            "library_url": library_url,
            "login_capture": await self.get_login_capture_status(),
            "power_diagnostics": power_diagnostics,
        }

    def get_cloud_login_url(self) -> str:
        """返回天翼云官方登录网址。"""
        return "https://cloud.189.cn/web/login.html"

    async def get_login_url(self) -> str:
        """返回本地登录桥接页面地址。"""
        target = quote(self.get_cloud_login_url(), safe="")
        return await self._ensure_local_web_ready(f"/tianyi/library/login-bridge?target={target}")

    async def get_library_url(self) -> str:
        """返回本地游戏库网页地址。"""
        return await self._ensure_local_web_ready("/tianyi/library")

    async def peek_login_url(self) -> str:
        """只读取登录桥接地址，不主动启动服务。"""
        target = quote(self.get_cloud_login_url(), safe="")
        return await self._peek_local_web_url(f"/tianyi/library/login-bridge?target={target}")

    async def peek_library_url(self) -> str:
        """只读取游戏库地址，不主动启动服务。"""
        return await self._peek_local_web_url("/tianyi/library")

    async def check_login_state(self) -> tuple[bool, str, str]:
        """校验当前登录态。"""
        cookie = (self.store.login.cookie or "").strip()
        if not cookie:
            return False, "", "未登录"
        try:
            account = await get_user_account(cookie)
        except Exception as exc:
            return False, "", f"登录态检查失败: {exc}"

        if not account:
            self.store.clear_login()
            return False, "", "登录态已失效，请重新登录"

        # 登录态有效时刷新账号名。
        self.store.set_login(cookie, account)
        return True, account, "登录态有效"

    async def save_manual_cookie(self, cookie: str, user_account: str = "") -> Dict[str, Any]:
        """手动保存 cookie。"""
        normalized = (cookie or "").strip()
        if not normalized:
            raise TianyiApiError("cookie 不能为空")
        account = (user_account or "").strip()
        if not account:
            fetched = await get_user_account(normalized)
            if not fetched:
                raise TianyiApiError("cookie 无效，请重新获取")
            account = fetched
        self.store.set_login(normalized, account)
        await self._set_capture_state(
            stage="completed",
            message=f"已保存 cookie，登录账号：{account}",
            reason="",
            next_action="",
            user_account=account,
            diagnostics={"source": "manual_cookie"},
        )
        return {"logged_in": True, "user_account": account, "message": "登录态已保存"}

    async def clear_login(self) -> Dict[str, Any]:
        """清理本地登录态。"""
        self.store.clear_login()
        self._invalidate_panel_cache(all_data=True)
        async with self._qr_login_lock:
            await self._close_qr_login_context_locked()
            await self._set_qr_login_state(
                session_id="",
                stage="idle",
                message="未开始",
                reason="",
                next_action="",
                user_account="",
                image_url="",
                expires_at=0,
                diagnostics={},
            )
        await self._set_capture_state(
            stage="idle",
            message="未开始",
            reason="",
            next_action="",
            user_account="",
            diagnostics={},
        )
        await self._cancel_cloud_save_task()
        await self._set_cloud_save_state(
            stage="idle",
            running=False,
            message="未开始",
            reason="login_cleared",
            progress=0.0,
            current_game="",
            total_games=0,
            processed_games=0,
            uploaded=0,
            skipped=0,
            failed=0,
            results=[],
            diagnostics={},
        )
        return {"logged_in": False, "user_account": "", "message": "已清理登录态"}

    def _installed_record_session_key(self, record: TianyiInstalledGame) -> str:
        """为已安装记录构造稳定会话键。"""
        game_id = str(record.game_id or "").strip()
        install_path = self._normalize_dir_path(str(record.install_path or "").strip())
        if game_id and install_path:
            return f"{game_id}|{install_path}"
        if game_id:
            return f"{game_id}|"
        if install_path:
            return f"|{install_path}"
        return ""

    def _derive_tianyi_launch_token(self, game_id: str) -> str:
        """与 steam_shortcuts 使用同一规则构造 Freedeck 启动 token。"""
        token = re.sub(r"[^a-zA-Z0-9._-]+", "_", str(game_id or "")).strip("_")
        return token or "game"

    def _snapshot_record_playtime(self, record: TianyiInstalledGame, *, now_ts: Optional[int] = None) -> Dict[str, Any]:
        """输出记录的游玩时长快照（包含进行中的会话增量）。"""
        now = max(0, _safe_int(now_ts, 0)) or _now_wall_ts()
        total_seconds = max(0, _safe_int(record.playtime_seconds, 0))
        sessions = max(0, _safe_int(record.playtime_sessions, 0))
        last_played_at = max(0, _safe_int(record.playtime_last_played_at, 0))
        active_started_at = max(0, _safe_int(record.playtime_active_started_at, 0))
        active_app_id = max(0, _safe_int(record.playtime_active_app_id, 0))

        active_seconds = 0
        active = bool(active_started_at > 0 and active_app_id > 0)
        if active and now > active_started_at:
            active_seconds = min(now - active_started_at, PLAYTIME_SESSION_MAX_SECONDS)
        snapshot_seconds = total_seconds + active_seconds
        if active_seconds > 0:
            last_played_at = max(last_played_at, now)

        return {
            "seconds": snapshot_seconds,
            "sessions": sessions,
            "last_played_at": last_played_at,
            "active": active,
            "active_app_id": active_app_id,
            "active_started_at": active_started_at,
            "active_seconds_included": active_seconds,
        }

    async def _resolve_installed_record_by_app_id(self, app_id_unsigned: int) -> Optional[TianyiInstalledGame]:
        """按 Steam AppID 定位已安装记录，必要时自动回填 appid 缓存。"""
        target_app_id = max(0, int(app_id_unsigned or 0))
        if target_app_id <= 0:
            return None

        records = list(self.store.installed_games or [])
        for record in records:
            if max(0, _safe_int(record.steam_app_id, 0)) == target_app_id:
                return record

        # 慢路径优化：一次性读取 shortcuts，避免对每条记录重复解析 shortcuts.vdf。
        shortcut_index: Dict[str, Any] = {}
        try:
            shortcut_index = await asyncio.to_thread(list_tianyi_shortcuts_sync)
        except Exception:
            shortcut_index = {}

        by_token = shortcut_index.get("by_token", {}) if isinstance(shortcut_index, dict) else {}
        if isinstance(by_token, dict) and by_token:
            matched: Optional[TianyiInstalledGame] = None
            needs_save = False

            for record in records:
                game_id = str(record.game_id or "").strip()
                if not game_id:
                    continue
                token = self._derive_tianyi_launch_token(game_id)
                row = by_token.get(token)
                if not isinstance(row, dict):
                    continue
                resolved_app_id = max(0, _safe_int(row.get("appid_unsigned"), 0))
                if resolved_app_id <= 0:
                    continue
                if resolved_app_id != max(0, _safe_int(record.steam_app_id, 0)):
                    record.steam_app_id = resolved_app_id
                    needs_save = True
                if resolved_app_id == target_app_id and matched is None:
                    matched = record

            if needs_save:
                await asyncio.to_thread(self.store.save)
            return matched

        matched: Optional[TianyiInstalledGame] = None
        needs_save = False
        for record in records:
            game_id = str(record.game_id or "").strip()
            if not game_id:
                continue
            try:
                shortcut = await asyncio.to_thread(resolve_tianyi_shortcut_sync, game_id=game_id)
            except Exception:
                continue
            if not bool(shortcut.get("ok")):
                continue
            resolved_app_id = max(0, _safe_int(shortcut.get("appid_unsigned"), 0))
            if resolved_app_id <= 0:
                continue
            if resolved_app_id != max(0, _safe_int(record.steam_app_id, 0)):
                record.steam_app_id = resolved_app_id
                needs_save = True
            if resolved_app_id == target_app_id and matched is None:
                matched = record

        if needs_save:
            await asyncio.to_thread(self.store.save)
        return matched

    async def _recover_playtime_sessions_from_store(self) -> None:
        """从 state.json 恢复进行中的游玩会话。"""
        now = _now_wall_ts()
        recovered: Dict[str, Dict[str, Any]] = {}
        stale_updated = False

        for record in list(self.store.installed_games or []):
            active_started_at = max(0, _safe_int(record.playtime_active_started_at, 0))
            active_app_id = max(0, _safe_int(record.playtime_active_app_id, 0))
            if active_started_at <= 0 or active_app_id <= 0:
                continue

            age = max(0, now - active_started_at)
            if age > PLAYTIME_STALE_SESSION_SECONDS:
                record.playtime_active_started_at = 0
                record.playtime_active_app_id = 0
                stale_updated = True
                continue

            key = self._installed_record_session_key(record)
            if not key:
                continue
            recovered[key] = {
                "game_id": str(record.game_id or "").strip(),
                "install_path": str(record.install_path or "").strip(),
                "app_id": active_app_id,
                "started_at": active_started_at,
            }

        async with self._playtime_lock:
            self._playtime_sessions = recovered

        if stale_updated:
            await asyncio.to_thread(self.store.save)
            self._invalidate_panel_cache(installed=True)

    async def _finalize_active_playtime_sessions(self, *, reason: str = "") -> None:
        """在服务关闭等场景结算所有活跃游玩会话。"""
        del reason  # 预留给后续诊断扩展。

        async with self._playtime_lock:
            active_sessions = dict(self._playtime_sessions or {})
            self._playtime_sessions = {}

        if not active_sessions:
            return

        now = _now_wall_ts()
        changed = False
        for session in active_sessions.values():
            game_id = str(session.get("game_id", "") or "").strip()
            install_path = str(session.get("install_path", "") or "").strip()
            started_at = max(0, _safe_int(session.get("started_at"), 0))

            record = self._find_installed_record(game_id=game_id, install_path=install_path)
            if record is None and game_id:
                record = self._find_installed_record(game_id=game_id)
            if record is None:
                continue

            record_changed = False
            if started_at > 0 and now > started_at:
                added = min(now - started_at, PLAYTIME_SESSION_MAX_SECONDS)
                if added > 0:
                    record.playtime_seconds = max(0, _safe_int(record.playtime_seconds, 0)) + added
                    record.playtime_sessions = max(0, _safe_int(record.playtime_sessions, 0)) + 1
                    record.playtime_last_played_at = max(0, now)
                    record_changed = True
            if max(0, _safe_int(record.playtime_active_started_at, 0)) > 0:
                record.playtime_active_started_at = 0
                record_changed = True
            if max(0, _safe_int(record.playtime_active_app_id, 0)) > 0:
                record.playtime_active_app_id = 0
                record_changed = True

            if record_changed:
                record.updated_at = now
                changed = True

        if changed:
            await asyncio.to_thread(self.store.save)
            self._invalidate_panel_cache(installed=True)

    async def record_game_action(self, *, phase: str, app_id: str, action_name: str = "") -> Dict[str, Any]:
        """记录 Steam 启动/退出事件并累计游玩时长。"""
        normalized_phase = str(phase or "").strip().lower()
        if normalized_phase not in {"start", "end"}:
            return {"accepted": False, "reason": "invalid_phase", "message": "无效的 phase"}

        app_id_unsigned = max(0, _safe_int(app_id, 0))
        if app_id_unsigned <= 0:
            return {"accepted": False, "reason": "invalid_app_id", "message": "无效的 app_id"}

        normalized_action = str(action_name or "").strip()
        if normalized_phase == "start" and normalized_action and normalized_action != "LaunchApp":
            return {
                "accepted": False,
                "reason": "ignored_action",
                "message": "非 LaunchApp 事件已忽略",
                "action_name": normalized_action,
            }

        record = await self._resolve_installed_record_by_app_id(app_id_unsigned)
        if record is None:
            return {
                "accepted": False,
                "reason": "app_not_managed",
                "message": "该 AppID 不属于 Freedeck 安装记录",
                "app_id": app_id_unsigned,
            }

        session_key = self._installed_record_session_key(record)
        if not session_key:
            return {"accepted": False, "reason": "record_invalid", "message": "已安装记录无效"}

        now = _now_wall_ts()
        changed = False
        added_seconds = 0
        now_record = self._find_installed_record(game_id=record.game_id, install_path=record.install_path) or record
        duplicate_start_grace_seconds = 5

        async with self._playtime_lock:
            existing = dict(self._playtime_sessions.get(session_key) or {})
            started_at = max(
                0,
                _safe_int(
                    existing.get("started_at"),
                    _safe_int(now_record.playtime_active_started_at, 0),
                ),
            )

            if normalized_phase == "start":
                if started_at > 0 and now > started_at:
                    elapsed = now - started_at
                    if elapsed > duplicate_start_grace_seconds:
                        added_seconds = min(elapsed, PLAYTIME_SESSION_MAX_SECONDS)
                        if added_seconds > 0:
                            now_record.playtime_seconds = max(0, _safe_int(now_record.playtime_seconds, 0)) + added_seconds
                            now_record.playtime_sessions = max(0, _safe_int(now_record.playtime_sessions, 0)) + 1
                            now_record.playtime_last_played_at = max(0, now)
                            changed = True

                self._playtime_sessions[session_key] = {
                    "game_id": str(now_record.game_id or "").strip(),
                    "install_path": str(now_record.install_path or "").strip(),
                    "app_id": app_id_unsigned,
                    "started_at": now,
                }
                if max(0, _safe_int(now_record.playtime_active_started_at, 0)) != now:
                    now_record.playtime_active_started_at = now
                    changed = True
                if max(0, _safe_int(now_record.playtime_active_app_id, 0)) != app_id_unsigned:
                    now_record.playtime_active_app_id = app_id_unsigned
                    changed = True
                if max(0, _safe_int(now_record.steam_app_id, 0)) != app_id_unsigned:
                    now_record.steam_app_id = app_id_unsigned
                    changed = True
            else:
                if started_at > 0 and now > started_at:
                    added_seconds = min(now - started_at, PLAYTIME_SESSION_MAX_SECONDS)
                    if added_seconds > 0:
                        now_record.playtime_seconds = max(0, _safe_int(now_record.playtime_seconds, 0)) + added_seconds
                        now_record.playtime_sessions = max(0, _safe_int(now_record.playtime_sessions, 0)) + 1
                        now_record.playtime_last_played_at = max(0, now)
                        changed = True

                if session_key in self._playtime_sessions:
                    self._playtime_sessions.pop(session_key, None)
                if max(0, _safe_int(now_record.playtime_active_started_at, 0)) > 0:
                    now_record.playtime_active_started_at = 0
                    changed = True
                if max(0, _safe_int(now_record.playtime_active_app_id, 0)) > 0:
                    now_record.playtime_active_app_id = 0
                    changed = True
                if max(0, _safe_int(now_record.steam_app_id, 0)) != app_id_unsigned:
                    now_record.steam_app_id = app_id_unsigned
                    changed = True

            if changed:
                now_record.updated_at = now
                await asyncio.to_thread(self.store.save)

        if changed:
            self._invalidate_panel_cache(installed=True)

        playtime = self._snapshot_record_playtime(now_record, now_ts=now)
        return {
            "accepted": True,
            "reason": "",
            "message": "记录成功",
            "phase": normalized_phase,
            "app_id": app_id_unsigned,
            "game_id": str(now_record.game_id or "").strip(),
            "game_title": str(now_record.game_title or "").strip(),
            "playtime_seconds": max(0, _safe_int(playtime.get("seconds"), 0)),
            "playtime_sessions": max(0, _safe_int(playtime.get("sessions"), 0)),
            "added_seconds": max(0, int(added_seconds or 0)),
        }

    async def get_library_game_time_stats(self, *, app_id: str = "", title: str = "") -> Dict[str, Any]:
        """按 Steam 库页面游戏返回 Freedeck 时长数据。"""
        app_id_unsigned = max(0, _safe_int(app_id, 0))
        title_raw = str(title or "").strip()
        title_norm = self._normalize_cover_text(title_raw)

        record: Optional[TianyiInstalledGame] = None
        if title_norm:
            for candidate in list(self.store.installed_games or []):
                candidate_title = str(candidate.game_title or "").strip()
                if not candidate_title:
                    continue
                candidate_norm = self._normalize_cover_text(candidate_title)
                if not candidate_norm:
                    continue
                if candidate_norm == title_norm or candidate_norm in title_norm or title_norm in candidate_norm:
                    record = candidate
                    break

        if record is None and app_id_unsigned > 0:
            record = await self._resolve_installed_record_by_app_id(app_id_unsigned)

        if record is None:
            fallback_hltb: Dict[str, Any] = {}
            if title_raw:
                try:
                    fallback_hltb = await self.resolve_hltb_stats(
                        game_id="",
                        title=title_raw,
                        categories="",
                        app_id=app_id_unsigned,
                        force_refresh=False,
                    )
                except Exception as exc:
                    config.logger.warning("库页面未匹配记录时查询 HLTB 失败: title=%s error=%s", title_raw, exc)

            fallback_main_hours = 0.0
            fallback_total_hours = 0.0
            try:
                fallback_main_hours = max(0.0, float(fallback_hltb.get("main_story_hours", 0.0) or 0.0))
            except Exception:
                fallback_main_hours = 0.0
            try:
                fallback_total_hours = max(0.0, float(fallback_hltb.get("total_hours", 0.0) or 0.0))
            except Exception:
                fallback_total_hours = 0.0
            return {
                "managed": False,
                "reason": "not_managed",
                "message": "当前库页面游戏不属于 Freedeck 安装记录",
                "app_id": app_id_unsigned,
                "title": title_raw,
                "my_playtime_seconds": 0,
                "my_playtime_text": "-",
                "main_story_hours": fallback_main_hours,
                "main_story_time_text": str(fallback_hltb.get("main_story_text", "") or "").strip() or "-",
                "total_hours": fallback_total_hours,
                "total_time_text": str(fallback_hltb.get("total_time_text", "") or "").strip() or "-",
            }

        game_id = str(record.game_id or "").strip()
        game_title = str(record.game_title or title_raw or "").strip()
        catalog_item = self.catalog.get_by_game_id(game_id) if game_id else None
        categories = str(getattr(catalog_item, "categories", "") or "")
        hltb_app_id = max(0, _safe_int(record.steam_app_id, 0))

        playtime = self._snapshot_record_playtime(record)
        my_playtime_seconds = max(0, _safe_int(playtime.get("seconds"), 0))
        my_playtime_text = _format_playtime_seconds(my_playtime_seconds)

        hltb: Dict[str, Any] = {}
        try:
            hltb = await self.resolve_hltb_stats(
                game_id=game_id,
                title=game_title,
                categories=categories,
                app_id=hltb_app_id,
                force_refresh=False,
            )
        except Exception as exc:
            config.logger.warning("库页面查询 HLTB 时长失败: game=%s error=%s", game_title, exc)

        main_story_hours = 0.0
        total_hours = 0.0
        try:
            main_story_hours = max(0.0, float(hltb.get("main_story_hours", 0.0) or 0.0))
        except Exception:
            main_story_hours = 0.0
        try:
            total_hours = max(0.0, float(hltb.get("total_hours", 0.0) or 0.0))
        except Exception:
            total_hours = 0.0

        return {
            "managed": True,
            "reason": "",
            "message": "",
            "app_id": hltb_app_id or app_id_unsigned,
            "game_id": game_id,
            "title": game_title,
            "my_playtime_seconds": my_playtime_seconds,
            "my_playtime_text": my_playtime_text,
            "my_playtime_active": bool(playtime.get("active")),
            "main_story_hours": main_story_hours,
            "main_story_time_text": str(hltb.get("main_story_text", "") or "").strip() or "-",
            "total_hours": total_hours,
            "total_time_text": str(hltb.get("total_time_text", "") or "").strip() or "-",
        }

    async def start_qr_login(self) -> Dict[str, Any]:
        """启动后端二维码登录会话。"""
        async with self._qr_login_lock:
            await self._close_qr_login_context_locked()

            login_ok, account, message = await self.check_login_state()
            if login_ok and account:
                await self._set_qr_login_state(
                    session_id="",
                    stage="completed",
                    message=f"检测到有效登录态：{account}",
                    reason="",
                    next_action="",
                    user_account=account,
                    image_url="",
                    expires_at=0,
                    diagnostics={"source": "stored_cookie", "check_message": message},
                )
                return dict(self._qr_login_state)

            session_id = uuid.uuid4().hex
            created_at = _now_wall_ts()
            expires_at = created_at + QR_LOGIN_SESSION_TIMEOUT_SECONDS
            timeout = aiohttp.ClientTimeout(total=QR_LOGIN_HTTP_TIMEOUT_SECONDS)
            ssl_context, tls_diag = self._build_qr_ssl_context()
            connector = aiohttp.TCPConnector(ssl=ssl_context)
            client = aiohttp.ClientSession(
                timeout=timeout,
                cookie_jar=aiohttp.CookieJar(unsafe=True),
                connector=connector,
            )
            context: Dict[str, Any] = {
                "session_id": session_id,
                "client": client,
                "created_at": created_at,
                "expires_at": expires_at,
                "poll_count": 0,
                "tls_diag": tls_diag,
            }

            try:
                bootstrap = await self._bootstrap_qr_login_context(context)
                context.update(bootstrap)
                context["image_url"] = f"/api/tianyi/login/qr/image?session_id={session_id}&_ts={_now_wall_ts()}"
                self._qr_login_context = context

                await self._set_qr_login_state(
                    session_id=session_id,
                    stage="running",
                    message="请使用天翼云盘 App 扫码登录",
                    reason="waiting_scan",
                    next_action="scan_qr",
                    user_account="",
                    image_url=str(context.get("image_url", "")),
                    expires_at=expires_at,
                    diagnostics={
                        "source": "qr_api",
                        "created_at": created_at,
                        "expires_at": expires_at,
                        "req_id": str(context.get("req_id", "")),
                        "tls": tls_diag,
                    },
                )
                return dict(self._qr_login_state)
            except Exception as exc:
                await self._safe_close_client_session(client)
                error_text = str(exc)
                reason = "qr_start_failed"
                if "CERTIFICATE_VERIFY_FAILED" in error_text.upper() or "certificate verify failed" in error_text.lower():
                    reason = "ssl_verify_failed"
                await self._set_qr_login_state(
                    session_id="",
                    stage="failed",
                    message=f"二维码会话启动失败：{exc}",
                    reason=reason,
                    next_action="retry",
                    user_account="",
                    image_url="",
                    expires_at=0,
                    diagnostics={
                        "exception": error_text,
                        "tls": tls_diag,
                    },
                )
                return dict(self._qr_login_state)

    async def poll_qr_login(self, session_id: str = "") -> Dict[str, Any]:
        """轮询二维码登录状态。"""
        async with self._qr_login_lock:
            context = self._qr_login_context
            if context is None:
                return dict(self._qr_login_state)

            current_id = str(context.get("session_id", ""))
            if session_id and session_id != current_id:
                return dict(self._qr_login_state)

            expires_at = int(context.get("expires_at") or 0)
            now_ts = _now_wall_ts()
            if expires_at > 0 and now_ts >= expires_at:
                await self._set_qr_login_state(
                    session_id=current_id,
                    stage="failed",
                    message="二维码已过期，请刷新后重试",
                    reason="qr_expired",
                    next_action="retry",
                    user_account="",
                    image_url=str(context.get("image_url", "")),
                    expires_at=expires_at,
                    diagnostics={"poll_count": int(context.get("poll_count") or 0)},
                )
                await self._close_qr_login_context_locked()
                return dict(self._qr_login_state)

            client = context.get("client")
            if not isinstance(client, aiohttp.ClientSession):
                await self._set_qr_login_state(
                    session_id=current_id,
                    stage="failed",
                    message="二维码会话异常，请刷新后重试",
                    reason="qr_context_invalid",
                    next_action="retry",
                    user_account="",
                    image_url=str(context.get("image_url", "")),
                    expires_at=expires_at,
                    diagnostics={},
                )
                await self._close_qr_login_context_locked()
                return dict(self._qr_login_state)

            state_payload = dict(context.get("state_payload") or {})
            now_ms = str(int(time.time() * 1000))
            state_payload["date"] = now_ms
            state_payload["timeStamp"] = now_ms

            req_id = str(context.get("req_id", ""))
            lt = str(context.get("lt", ""))
            login_page_url = str(context.get("login_page_url", ""))
            headers = self._build_qr_headers(req_id=req_id, lt=lt, referer=login_page_url)

            try:
                async with client.post(
                    "https://open.e.189.cn/api/logbox/oauth2/qrcodeLoginState.do",
                    data=state_payload,
                    headers=headers,
                ) as resp:
                    raw_text = await resp.text()
                    if resp.status >= 400:
                        raise TianyiApiError(f"二维码状态接口失败 status={resp.status}")
            except Exception as exc:
                await self._set_qr_login_state(
                    session_id=current_id,
                    stage="running",
                    message="状态轮询失败，正在重试...",
                    reason="poll_exception",
                    next_action="wait",
                    user_account="",
                    image_url=str(context.get("image_url", "")),
                    expires_at=expires_at,
                    diagnostics={"exception": str(exc), "poll_count": int(context.get("poll_count") or 0)},
                )
                return dict(self._qr_login_state)

            try:
                payload = self._parse_json_like_text(raw_text)
            except Exception as exc:
                await self._set_qr_login_state(
                    session_id=current_id,
                    stage="running",
                    message="状态解析失败，正在重试...",
                    reason="poll_parse_failed",
                    next_action="wait",
                    user_account="",
                    image_url=str(context.get("image_url", "")),
                    expires_at=expires_at,
                    diagnostics={"exception": str(exc), "raw": str(raw_text)[:320]},
                )
                return dict(self._qr_login_state)

            status_code = self._extract_qr_status_code(payload)
            context["poll_count"] = int(context.get("poll_count") or 0) + 1

            poll_diag: Dict[str, Any] = {
                "poll_count": int(context.get("poll_count") or 0),
                "status_code": status_code,
            }

            if status_code == QR_STATUS_SUCCESS:
                redirect_url = self._extract_qr_redirect_url(payload)
                account, cookie, verify_reason = await self._finalize_qr_login_success(
                    context=context,
                    redirect_url=redirect_url,
                )
                poll_diag["redirect_url"] = redirect_url
                if verify_reason:
                    poll_diag["verify_reason"] = verify_reason

                if account and cookie:
                    self.store.set_login(cookie, account)
                    await self._set_qr_login_state(
                        session_id=current_id,
                        stage="completed",
                        message=f"登录成功：{account}",
                        reason="",
                        next_action="",
                        user_account=account,
                        image_url=str(context.get("image_url", "")),
                        expires_at=expires_at,
                        diagnostics=poll_diag,
                    )
                    await self._close_qr_login_context_locked()
                    return dict(self._qr_login_state)

                await self._set_qr_login_state(
                    session_id=current_id,
                    stage="failed",
                    message="扫码已确认，但未拿到有效登录态",
                    reason="qr_cookie_verify_failed",
                    next_action="retry",
                    user_account="",
                    image_url=str(context.get("image_url", "")),
                    expires_at=expires_at,
                    diagnostics=poll_diag,
                )
                await self._close_qr_login_context_locked()
                return dict(self._qr_login_state)

            if status_code in QR_STATUS_EXPIRED:
                await self._set_qr_login_state(
                    session_id=current_id,
                    stage="failed",
                    message="二维码已失效，请刷新后重试",
                    reason="qr_expired",
                    next_action="retry",
                    user_account="",
                    image_url=str(context.get("image_url", "")),
                    expires_at=expires_at,
                    diagnostics=poll_diag,
                )
                await self._close_qr_login_context_locked()
                return dict(self._qr_login_state)

            if status_code in QR_STATUS_SCANNED_WAIT_CONFIRM:
                await self._set_qr_login_state(
                    session_id=current_id,
                    stage="running",
                    message="已扫码，请在手机上确认登录",
                    reason="await_confirm",
                    next_action="confirm_on_phone",
                    user_account="",
                    image_url=str(context.get("image_url", "")),
                    expires_at=expires_at,
                    diagnostics=poll_diag,
                )
                return dict(self._qr_login_state)

            if status_code in QR_STATUS_NEED_EXTRA_VERIFY:
                await self._set_qr_login_state(
                    session_id=current_id,
                    stage="failed",
                    message="账号触发二次验证，请在天翼云官方页面完成验证后重试",
                    reason="need_extra_verify",
                    next_action="open_official_login",
                    user_account="",
                    image_url=str(context.get("image_url", "")),
                    expires_at=expires_at,
                    diagnostics=poll_diag,
                )
                return dict(self._qr_login_state)

            if status_code in QR_STATUS_WAITING:
                await self._set_qr_login_state(
                    session_id=current_id,
                    stage="running",
                    message="等待扫码登录",
                    reason="waiting_scan",
                    next_action="scan_qr",
                    user_account="",
                    image_url=str(context.get("image_url", "")),
                    expires_at=expires_at,
                    diagnostics=poll_diag,
                )
                return dict(self._qr_login_state)

            await self._set_qr_login_state(
                session_id=current_id,
                stage="running",
                message="正在等待登录状态更新...",
                reason="polling",
                next_action="wait",
                user_account="",
                image_url=str(context.get("image_url", "")),
                expires_at=expires_at,
                diagnostics=poll_diag,
            )
            return dict(self._qr_login_state)

    async def stop_qr_login(self, session_id: str = "") -> Dict[str, Any]:
        """停止二维码登录会话。"""
        async with self._qr_login_lock:
            context = self._qr_login_context
            if context is not None:
                current_id = str(context.get("session_id", ""))
                if not session_id or session_id == current_id:
                    await self._close_qr_login_context_locked()
                    await self._set_qr_login_state(
                        session_id=current_id,
                        stage="stopped",
                        message="已停止二维码登录",
                        reason="qr_stopped",
                        next_action="retry",
                        user_account="",
                        image_url="",
                        expires_at=0,
                        diagnostics={},
                    )
                    return dict(self._qr_login_state)
            return dict(self._qr_login_state)

    async def get_qr_login_state(self) -> Dict[str, Any]:
        """读取二维码登录状态。"""
        return dict(self._qr_login_state)

    async def get_qr_login_image(self, session_id: str = "") -> Tuple[bytes, str]:
        """读取二维码图片二进制。"""
        async with self._qr_login_lock:
            context = self._qr_login_context
            if context is None:
                raise TianyiApiError("二维码会话不存在，请先刷新二维码")

            current_id = str(context.get("session_id", ""))
            if session_id and session_id != current_id:
                raise TianyiApiError("二维码会话已更新，请刷新页面")

            client = context.get("client")
            if not isinstance(client, aiohttp.ClientSession):
                raise TianyiApiError("二维码会话异常，请刷新二维码")

            image_remote_url = str(context.get("image_remote_url", ""))
            if not image_remote_url:
                raise TianyiApiError("二维码地址缺失，请刷新二维码")

            headers = self._build_qr_headers(
                req_id=str(context.get("req_id", "")),
                lt=str(context.get("lt", "")),
                referer=str(context.get("login_page_url", "")),
            )
            async with client.get(image_remote_url, headers=headers) as resp:
                if resp.status >= 400:
                    raise TianyiApiError(f"二维码图片获取失败 status={resp.status}")
                content_type = str(resp.headers.get("Content-Type", "image/jpeg") or "image/jpeg")
                body = await resp.read()
                if not body:
                    raise TianyiApiError("二维码图片为空，请刷新二维码")
                return body, content_type

    async def start_login_capture(self, timeout_seconds: int = CAPTURE_DEFAULT_TIMEOUT_SECONDS) -> Dict[str, Any]:
        """启动自动 Cookie 采集流程。"""
        try:
            timeout = int(timeout_seconds or CAPTURE_DEFAULT_TIMEOUT_SECONDS)
        except Exception:
            timeout = CAPTURE_DEFAULT_TIMEOUT_SECONDS
        timeout = max(CAPTURE_MIN_TIMEOUT_SECONDS, min(CAPTURE_MAX_TIMEOUT_SECONDS, timeout))

        async with self._capture_lock:
            if self._capture_task and not self._capture_task.done():
                self._capture_task.cancel()
                try:
                    await self._capture_task
                except BaseException:
                    pass

            quick_diag: Dict[str, Any] = {"timeout_seconds": timeout}
            await self._set_capture_state(
                stage="starting",
                message="正在检查当前登录态...",
                reason="",
                next_action="",
                user_account="",
                diagnostics=quick_diag,
            )

            # 优先走本地已存登录态的快速校验，避免用户已登录却仍等待超时。
            login_ok, account, login_message = await self.check_login_state()
            quick_diag["check_message"] = login_message
            if login_ok and account:
                await self._set_capture_state(
                    stage="completed",
                    message=f"检测到有效登录态：{account}",
                    reason="",
                    next_action="",
                    user_account=account,
                    diagnostics={"source": "stored_cookie", "check_message": login_message},
                    source_attempts=["stored_cookie"],
                    success_source="stored_cookie",
                    source_diagnostics={
                        "stored_cookie": {
                            "ok": True,
                            "reason": "",
                            "message": login_message,
                        }
                    },
                )
                return dict(self._capture_state)

            # 入页后立即执行一次双通道采集尝试，命中即马上回传并落盘。
            initial_attempt = await self._attempt_capture_sources_once()
            quick_diag["initial_reason"] = str(initial_attempt.get("reason", ""))
            quick_diag["main_landing_detected"] = bool(initial_attempt.get("main_landing_detected"))
            quick_diag["source_diagnostics"] = dict(initial_attempt.get("source_diagnostics") or {})

            if bool(initial_attempt.get("success")):
                resolved_cookie = str(initial_attempt.get("cookie", "") or "")
                resolved_account = str(initial_attempt.get("account", "") or "")
                success_source = str(initial_attempt.get("success_source", "") or "")
                if resolved_cookie and resolved_account:
                    self.store.set_login(resolved_cookie, resolved_account)
                    await self._set_capture_state(
                        stage="completed",
                        message=f"登录成功：{resolved_account}",
                        reason="",
                        next_action="",
                        user_account=resolved_account,
                        diagnostics={
                            "source": "initial_dual_source_probe",
                            "success_source": success_source,
                            "main_landing_detected": bool(initial_attempt.get("main_landing_detected")),
                        },
                        source_attempts=list(initial_attempt.get("source_attempts") or []),
                        success_source=success_source,
                        source_diagnostics=dict(initial_attempt.get("source_diagnostics") or {}),
                    )
                    return dict(self._capture_state)

            await self._set_capture_state(
                stage="starting",
                message="正在启动持续采集，请在网页完成扫码登录...",
                reason="",
                next_action="",
                user_account="",
                diagnostics=quick_diag,
                source_attempts=list(initial_attempt.get("source_attempts") or []),
                success_source="",
                source_diagnostics=dict(initial_attempt.get("source_diagnostics") or {}),
            )
            self._capture_task = asyncio.create_task(
                self._capture_loop(timeout_seconds=timeout, seed_diagnostics=quick_diag),
                name="freedeck_tianyi_capture",
            )
            return dict(self._capture_state)

    async def stop_login_capture(self) -> Dict[str, Any]:
        """停止自动 Cookie 采集流程。"""
        async with self._capture_lock:
            if self._capture_task and not self._capture_task.done():
                self._capture_task.cancel()
                try:
                    await self._capture_task
                except BaseException:
                    pass
            self._capture_task = None

            await self._set_capture_state(
                stage="stopped",
                message="已停止自动采集，可改用手动 Cookie",
                reason="capture_stopped",
                next_action="manual_cookie",
                user_account="",
                diagnostics={},
            )
            return dict(self._capture_state)

    async def get_login_capture_status(self) -> Dict[str, Any]:
        """读取当前自动采集状态。"""
        return dict(self._capture_state)

    async def list_catalog(self, query: str, page: int, page_size: int) -> Dict[str, Any]:
        """查询游戏目录。"""
        # page_size 默认跟随设置，但允许前端覆盖。
        if page_size <= 0:
            page_size = self.store.settings.page_size
        return self.catalog.list(query=query, page=page, page_size=page_size)

    async def resolve_catalog_cover(
        self,
        *,
        game_id: str = "",
        title: str = "",
        categories: str = "",
    ) -> Dict[str, Any]:
        """按游戏标题解析封面 URL（优先 Steam 商店），并做内存缓存。"""
        cache_key = str(game_id or title or "").strip().lower()
        now_ts = _now_wall_ts()
        if not cache_key:
            return {
                "cover_url": "",
                "square_cover_url": "",
                "source": "",
                "matched_title": "",
                "app_id": 0,
                "protondb_tier": "",
                "cached": False,
            }

        async with self._catalog_cover_lock:
            cached = self._catalog_cover_cache.get(cache_key)
            if isinstance(cached, dict) and int(cached.get("expires_at", 0)) > now_ts:
                return {
                    "cover_url": str(cached.get("cover_url", "") or ""),
                    "square_cover_url": str(cached.get("square_cover_url", "") or ""),
                    "source": str(cached.get("source", "") or ""),
                    "matched_title": str(cached.get("matched_title", "") or ""),
                    "app_id": _safe_int(cached.get("app_id"), 0),
                    "protondb_tier": str(cached.get("protondb_tier", "") or ""),
                    "cached": True,
                }

        cover_url = ""
        square_cover_url = ""
        source = ""
        matched_title = ""
        app_id = 0
        protondb_tier = ""
        terms = self._build_catalog_cover_terms(title=title, categories=categories)

        if terms:
            try:
                ssl_context, _ = self._build_qr_ssl_context()
                timeout = aiohttp.ClientTimeout(total=CATALOG_COVER_HTTP_TIMEOUT_SECONDS)
                connector = aiohttp.TCPConnector(ssl=ssl_context)
                async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
                    for term in terms:
                        search_url = str(
                            URL("https://store.steampowered.com/api/storesearch/").with_query(
                                {"term": term, "l": "schinese", "cc": "cn"}
                            )
                        )
                        headers = {
                            "Accept": "application/json, text/plain, */*",
                            "User-Agent": "Mozilla/5.0 (Freedeck/1.0; +https://cloud.189.cn)",
                            "Referer": "https://store.steampowered.com/",
                        }
                        try:
                            async with session.get(search_url, headers=headers) as resp:
                                if int(resp.status) != 200:
                                    continue
                                payload = await resp.json(content_type=None)
                        except Exception:
                            continue

                        items = payload.get("items") if isinstance(payload, dict) else []
                        resolved = self._pick_catalog_cover_candidate(term=term, items=items)
                        if not resolved:
                            continue
                        cover_url = str(resolved.get("cover_url", "") or "").strip()
                        matched_title = str(resolved.get("matched_title", "") or "").strip()
                        source = str(resolved.get("source", "") or "").strip()
                        app_id = _safe_int(resolved.get("app_id"), 0)
                        if cover_url or app_id > 0:
                            square_cover_url = self._build_store_square_cover_url(app_id)
                            if app_id > 0:
                                proton_summary = await self._fetch_protondb_summary(session=session, app_id=app_id)
                                protondb_tier = str(proton_summary.get("tier", "") or "").strip()
                            break
            except Exception as exc:
                config.logger.warning("解析游戏封面失败: title=%s error=%s", title, exc)

        has_positive_payload = bool(cover_url or square_cover_url or app_id > 0 or protondb_tier)
        expires_at = now_ts + (
            CATALOG_COVER_CACHE_TTL_SECONDS if has_positive_payload else CATALOG_COVER_NEGATIVE_TTL_SECONDS
        )
        cache_value = {
            "cover_url": cover_url,
            "square_cover_url": square_cover_url,
            "source": source,
            "matched_title": matched_title,
            "app_id": int(app_id),
            "protondb_tier": protondb_tier,
            "expires_at": int(expires_at),
        }
        async with self._catalog_cover_lock:
            self._catalog_cover_cache[cache_key] = cache_value

        return {
            "cover_url": cover_url,
            "square_cover_url": square_cover_url,
            "source": source,
            "matched_title": matched_title,
            "app_id": int(app_id),
            "protondb_tier": protondb_tier,
            "cached": False,
        }

    def _build_catalog_cover_terms(self, *, title: str, categories: str = "") -> List[str]:
        """生成封面检索候选词，优先英文名。"""
        raw_title = str(title or "").strip()
        raw_categories = str(categories or "").strip()
        if not raw_title:
            return []

        def sanitize(value: str) -> str:
            text = str(value or "").strip()
            text = re.sub(r"\s+", " ", text)
            text = re.sub(r"(?i)\b(v|ver|version)\s*\d+(?:\.\d+){0,3}\b", "", text).strip()
            text = re.sub(r"\s+", " ", text).strip()
            return text

        parts = [sanitize(raw_title)]
        parts.extend(sanitize(part) for part in re.split(r"[\/／|｜]+", raw_title))
        if raw_categories:
            parts.extend(sanitize(part) for part in re.split(r"[\/／|｜,，]+", raw_categories))

        ascii_parts = [part for part in parts if re.search(r"[A-Za-z]", part)]
        ordered: List[str] = []
        for value in ascii_parts + parts:
            if not value:
                continue
            if value not in ordered:
                ordered.append(value)
        return ordered[:6]

    def _normalize_cover_text(self, value: str) -> str:
        text = str(value or "").lower()
        text = re.sub(r"[^0-9a-z\u4e00-\u9fff]+", " ", text)
        return " ".join(text.split())

    def _pick_catalog_cover_candidate(self, *, term: str, items: Any) -> Optional[Dict[str, Any]]:
        if not isinstance(items, list) or not items:
            return None

        query_norm = self._normalize_cover_text(term)
        query_tokens = set(query_norm.split()) if query_norm else set()
        best_score = -1
        best: Optional[Dict[str, str]] = None

        for item in items[:CATALOG_COVER_SEARCH_LIMIT]:
            if not isinstance(item, dict):
                continue
            cover_url = self._extract_store_cover_url(item)
            app_id = self._extract_store_app_id(item)
            if not cover_url:
                continue
            name = str(item.get("name", "") or "").strip()
            name_norm = self._normalize_cover_text(name)
            name_tokens = set(name_norm.split()) if name_norm else set()

            score = 1
            if query_norm and name_norm:
                if query_norm == name_norm:
                    score += 120
                elif query_norm in name_norm or name_norm in query_norm:
                    score += 80
            if query_tokens and name_tokens:
                score += len(query_tokens & name_tokens) * 12

            if score > best_score:
                best_score = score
                best = {
                    "cover_url": cover_url,
                    "matched_title": name,
                    "source": "steam_store_search",
                    "app_id": int(app_id),
                }

        return best

    def _extract_store_app_id(self, item: Dict[str, Any]) -> int:
        app_id = _safe_int(item.get("id"), 0)
        if app_id <= 0:
            app_id = _safe_int(item.get("appid"), 0)
        return app_id

    def _extract_store_cover_url(self, item: Dict[str, Any]) -> str:
        for key in (
            "large_capsule_image",
            "header_image",
            "capsule_image",
            "small_capsule_image",
            "tiny_image",
        ):
            value = str(item.get(key, "") or "").strip()
            if value.startswith("http://") or value.startswith("https://"):
                return value

        app_id = self._extract_store_app_id(item)
        if app_id > 0:
            return f"https://shared.fastly.steamstatic.com/store_item_assets/steam/apps/{app_id}/capsule_616x353.jpg"
        return ""

    def _build_store_square_cover_url(self, app_id: int) -> str:
        """优先返回更适合方形裁切的 Steam 竖版素材 URL。"""
        if _safe_int(app_id, 0) <= 0:
            return ""
        app = _safe_int(app_id, 0)
        return f"https://shared.fastly.steamstatic.com/store_item_assets/steam/apps/{app}/library_600x900_2x.jpg"

    async def _fetch_protondb_summary(self, *, session: aiohttp.ClientSession, app_id: int) -> Dict[str, Any]:
        """读取 ProtonDB 摘要信息。"""
        if app_id <= 0:
            return {}
        api_url = f"https://www.protondb.com/api/v1/reports/summaries/{int(app_id)}.json"
        headers = {
            "Accept": "application/json, text/plain, */*",
            "User-Agent": "Mozilla/5.0 (Freedeck/1.0; +https://cloud.189.cn)",
            "Referer": "https://www.protondb.com/",
        }
        try:
            timeout = aiohttp.ClientTimeout(total=PROTONDB_HTTP_TIMEOUT_SECONDS)
            async with session.get(api_url, headers=headers, timeout=timeout) as resp:
                if int(resp.status) != 200:
                    return {}
                payload = await resp.json(content_type=None)
                if not isinstance(payload, dict):
                    return {}
                tier = str(payload.get("tier", "") or "").strip()
                return {"tier": tier}
        except Exception:
            return {}

    def _build_hltb_search_payload(self, term: str) -> Dict[str, Any]:
        """构建 HLTB 搜索请求体。"""
        text = str(term or "").strip()
        words = [item for item in re.split(r"\s+", text) if item]
        if not words and text:
            words = [text]
        return {
            "searchType": "games",
            "searchTerms": words[:8],
            "searchPage": 1,
            "size": 20,
            "searchOptions": {
                "games": {
                    "userId": 0,
                    "platform": "",
                    "sortCategory": "popular",
                    "rangeCategory": "main",
                    "rangeTime": {"min": 0, "max": 0},
                    "gameplay": {
                        "perspective": "",
                        "flow": "",
                        "genre": "",
                        "difficulty": "",
                    },
                    "rangeYear": {"min": "", "max": ""},
                    "modifier": "hide_dlc",
                },
                "users": {"sortCategory": "postcount"},
                "lists": {"sortCategory": "follows"},
                "filter": "",
                "sort": 0,
                "randomizer": 0,
            },
            "useCache": True,
        }

    async def _fetch_hltb_token(self, *, session: aiohttp.ClientSession, headers: Dict[str, str]) -> str:
        """获取 HLTB finder API 的短期鉴权 token。"""
        init_url = f"{HLTB_TOKEN_URL}?t={int(time.time() * 1000)}"
        try:
            async with session.get(init_url, headers=headers) as resp:
                if int(resp.status) != 200:
                    return ""
                data = await resp.json(content_type=None)
        except Exception:
            return ""
        if not isinstance(data, dict):
            return ""
        token = str(data.get("token", "") or "").strip()
        return token

    async def _post_hltb_search(
        self,
        *,
        session: aiohttp.ClientSession,
        payload: Dict[str, Any],
        headers: Dict[str, str],
        token: str,
    ) -> Tuple[int, List[Dict[str, Any]]]:
        """请求 HLTB finder 搜索接口。"""
        req_headers = dict(headers)
        if token:
            req_headers["x-auth-token"] = token
        try:
            async with session.post(HLTB_SEARCH_URL, headers=req_headers, json=payload) as resp:
                status = int(resp.status)
                if status != 200:
                    return status, []
                data = await resp.json(content_type=None)
        except Exception:
            return 0, []
        rows = data.get("data") if isinstance(data, dict) else []
        if isinstance(rows, list):
            return 200, [item for item in rows if isinstance(item, dict)]
        return 200, []

    async def _post_hltb_search_legacy(
        self,
        *,
        session: aiohttp.ClientSession,
        payload: Dict[str, Any],
        headers: Dict[str, str],
    ) -> List[Dict[str, Any]]:
        """兼容旧版 HLTB /api/search 接口。"""
        try:
            async with session.post(HLTB_LEGACY_SEARCH_URL, headers=headers, json=payload) as resp:
                if int(resp.status) != 200:
                    return []
                data = await resp.json(content_type=None)
        except Exception:
            return []
        rows = data.get("data") if isinstance(data, dict) else []
        if isinstance(rows, list):
            return [item for item in rows if isinstance(item, dict)]
        return []

    def _pick_hltb_candidate(
        self,
        *,
        title: str,
        term: str,
        app_id: int,
        rows: Any,
    ) -> Optional[Dict[str, Any]]:
        """从 HLTB 搜索结果中选取最佳候选。"""
        if not isinstance(rows, list) or not rows:
            return None

        query_norm = self._normalize_cover_text(title or term)
        query_tokens = set(query_norm.split()) if query_norm else set()
        best_score = -1
        best_item: Optional[Dict[str, Any]] = None

        for item in rows[:HLTB_SEARCH_LIMIT]:
            if not isinstance(item, dict):
                continue
            game_name = str(item.get("game_name", "") or "").strip()
            if not game_name:
                continue

            candidate_app_id = _safe_int(item.get("profile_steam"), 0)
            name_norm = self._normalize_cover_text(game_name)
            name_tokens = set(name_norm.split()) if name_norm else set()

            score = 1
            if app_id > 0 and candidate_app_id == app_id:
                score += 260
            if query_norm and name_norm:
                if query_norm == name_norm:
                    score += 120
                elif query_norm in name_norm or name_norm in query_norm:
                    score += 80
            if query_tokens and name_tokens:
                score += len(query_tokens & name_tokens) * 14
            score += min(20, max(0, _safe_int(item.get("comp_all_count"), 0)) // 500)

            if score > best_score:
                best_score = score
                best_item = dict(item)

        return best_item

    async def resolve_hltb_stats(
        self,
        *,
        game_id: str = "",
        title: str = "",
        categories: str = "",
        app_id: int = 0,
        force_refresh: bool = False,
    ) -> Dict[str, Any]:
        """查询并缓存 HLTB 时长数据。"""
        cache_key = str(game_id or title or "").strip().lower()
        now_ts = _now_wall_ts()
        if not cache_key or not str(title or "").strip():
            return {
                "main_story_hours": 0.0,
                "main_story_text": "-",
                "total_hours": 0.0,
                "total_time_text": "-",
                "hltb_game_id": 0,
                "matched_title": "",
                "source_term": "",
                "cached": False,
            }

        if not force_refresh:
            async with self._hltb_lock:
                cached = self._hltb_cache.get(cache_key)
                if isinstance(cached, dict) and _safe_int(cached.get("expires_at"), 0) > now_ts:
                    main_hours = float(cached.get("main_story_hours", 0.0) or 0.0)
                    total_hours = float(cached.get("total_hours", 0.0) or 0.0)
                    return {
                        "main_story_hours": main_hours,
                        "main_story_text": _format_hours_value(main_hours),
                        "total_hours": total_hours,
                        "total_time_text": _format_hours_value(total_hours),
                        "hltb_game_id": _safe_int(cached.get("hltb_game_id"), 0),
                        "matched_title": str(cached.get("matched_title", "") or ""),
                        "source_term": str(cached.get("source_term", "") or ""),
                        "cached": True,
                    }

        main_hours = 0.0
        total_hours = 0.0
        hltb_game_id = 0
        matched_title = ""
        source_term = ""
        terms = self._build_catalog_cover_terms(title=title, categories=categories)
        if not terms:
            terms = [str(title or "").strip()]

        if terms:
            headers = {
                "Accept": "application/json, text/plain, */*",
                "Content-Type": "application/json",
                "Origin": "https://howlongtobeat.com",
                "Referer": "https://howlongtobeat.com/",
                "User-Agent": "Mozilla/5.0 (Freedeck/1.0; +https://cloud.189.cn)",
            }
            try:
                timeout = aiohttp.ClientTimeout(total=HLTB_HTTP_TIMEOUT_SECONDS)
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    for term in terms:
                        payload = self._build_hltb_search_payload(term)
                        rows: List[Dict[str, Any]] = []

                        # 优先走 legacy 接口（参考 hltb-for-deck），成功率与速度在 Deck 端更稳定。
                        rows = await self._post_hltb_search_legacy(
                            session=session,
                            payload=payload,
                            headers=headers,
                        )

                        if not rows:
                            token = await self._fetch_hltb_token(session=session, headers=headers)
                            status, rows = await self._post_hltb_search(
                                session=session,
                                payload=payload,
                                headers=headers,
                                token=token,
                            )
                            if status in {401, 403}:
                                token = await self._fetch_hltb_token(session=session, headers=headers)
                                if token:
                                    status, rows = await self._post_hltb_search(
                                        session=session,
                                        payload=payload,
                                        headers=headers,
                                        token=token,
                                    )

                        candidate = self._pick_hltb_candidate(
                            title=title,
                            term=term,
                            app_id=app_id,
                            rows=rows,
                        )
                        if not candidate:
                            continue

                        comp_main = max(0, _safe_int(candidate.get("comp_main"), 0))
                        comp_all = max(0, _safe_int(candidate.get("comp_all"), 0))
                        comp_100 = max(0, _safe_int(candidate.get("comp_100"), 0))
                        comp_plus = max(0, _safe_int(candidate.get("comp_plus"), 0))
                        total_seconds = comp_all or comp_100 or comp_plus or comp_main
                        main_hours = round((comp_main / 3600.0), 1) if comp_main > 0 else 0.0
                        total_hours = round((total_seconds / 3600.0), 1) if total_seconds > 0 else 0.0
                        hltb_game_id = max(0, _safe_int(candidate.get("game_id"), 0))
                        matched_title = str(candidate.get("game_name", "") or "").strip()
                        source_term = str(term or "").strip()
                        break
            except Exception as exc:
                config.logger.warning("解析 HLTB 时长失败: title=%s error=%s", title, exc)

        has_positive_payload = bool(main_hours > 0 or total_hours > 0 or hltb_game_id > 0)
        expires_at = now_ts + (
            HLTB_CACHE_TTL_SECONDS if has_positive_payload else HLTB_NEGATIVE_TTL_SECONDS
        )
        cache_value = {
            "main_story_hours": float(main_hours),
            "total_hours": float(total_hours),
            "hltb_game_id": int(hltb_game_id),
            "matched_title": matched_title,
            "source_term": source_term,
            "expires_at": int(expires_at),
        }
        async with self._hltb_lock:
            self._hltb_cache[cache_key] = cache_value

        return {
            "main_story_hours": float(main_hours),
            "main_story_text": _format_hours_value(main_hours),
            "total_hours": float(total_hours),
            "total_time_text": _format_hours_value(total_hours),
            "hltb_game_id": int(hltb_game_id),
            "matched_title": matched_title,
            "source_term": source_term,
            "cached": False,
        }

    async def get_settings(self) -> Dict[str, Any]:
        """获取下载设置。"""
        return asdict(self.store.settings)

    def _new_cloud_save_state(self) -> Dict[str, Any]:
        """构建云存档任务默认状态。"""
        return {
            "stage": "idle",
            "message": "未开始",
            "reason": "",
            "running": False,
            "progress": 0.0,
            "current_game": "",
            "total_games": 0,
            "processed_games": 0,
            "uploaded": 0,
            "skipped": 0,
            "failed": 0,
            "results": [],
            "diagnostics": {},
            "updated_at": _now_wall_ts(),
            "last_result": dict(self.store.cloud_save_last_result or {}),
        }

    def _copy_cloud_save_result(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """复制单游戏上传结果并规范字段。"""
        result = dict(item or {})
        result["game_id"] = str(result.get("game_id", "") or "")
        result["game_title"] = str(result.get("game_title", "") or "")
        result["game_key"] = str(result.get("game_key", "") or "")
        result["status"] = str(result.get("status", "") or "")
        result["reason"] = str(result.get("reason", "") or "")
        result["cloud_path"] = str(result.get("cloud_path", "") or "")
        source_paths = result.get("source_paths", [])
        if isinstance(source_paths, list):
            result["source_paths"] = [str(path or "") for path in source_paths if str(path or "").strip()]
        else:
            result["source_paths"] = []
        diagnostics = result.get("diagnostics", {})
        result["diagnostics"] = dict(diagnostics) if isinstance(diagnostics, dict) else {}
        return result

    def _cloud_save_state_snapshot_locked(self) -> Dict[str, Any]:
        """在已持锁场景下复制云存档状态。"""
        state = dict(self._cloud_save_state or {})
        results = state.get("results", [])
        if isinstance(results, list):
            state["results"] = [self._copy_cloud_save_result(item) for item in results if isinstance(item, dict)]
        else:
            state["results"] = []
        diagnostics = state.get("diagnostics", {})
        state["diagnostics"] = dict(diagnostics) if isinstance(diagnostics, dict) else {}
        last_result = state.get("last_result", {})
        state["last_result"] = dict(last_result) if isinstance(last_result, dict) else {}
        return state

    async def _set_cloud_save_state(self, **patch: Any) -> Dict[str, Any]:
        """更新云存档状态并返回快照。"""
        async with self._cloud_save_lock:
            state = self._cloud_save_state
            for key, value in patch.items():
                if key == "stage":
                    stage = str(value or "idle")
                    if stage not in CLOUD_SAVE_TASK_STAGES:
                        stage = "idle"
                    state["stage"] = stage
                    continue
                if key == "running":
                    state["running"] = bool(value)
                    continue
                if key == "progress":
                    try:
                        progress = float(value)
                    except Exception:
                        progress = 0.0
                    progress = max(0.0, min(100.0, progress))
                    state["progress"] = round(progress, 2)
                    continue
                if key in {"total_games", "processed_games", "uploaded", "skipped", "failed"}:
                    state[key] = max(0, _safe_int(value, 0))
                    continue
                if key in {"message", "reason", "current_game"}:
                    state[key] = str(value or "")
                    continue
                if key == "results":
                    rows = value if isinstance(value, list) else []
                    state["results"] = [self._copy_cloud_save_result(item) for item in rows if isinstance(item, dict)]
                    continue
                if key == "diagnostics":
                    state["diagnostics"] = dict(value) if isinstance(value, dict) else {}
                    continue
                if key == "last_result":
                    state["last_result"] = dict(value) if isinstance(value, dict) else {}
                    continue
                state[key] = value

            state["updated_at"] = _now_wall_ts()
            return self._cloud_save_state_snapshot_locked()

    async def _get_cloud_save_state_snapshot(self) -> Dict[str, Any]:
        """获取云存档状态快照。"""
        async with self._cloud_save_lock:
            return self._cloud_save_state_snapshot_locked()

    async def _cancel_cloud_save_task(self) -> None:
        """取消当前云存档任务（如有）。"""
        target: Optional[asyncio.Task] = None
        async with self._cloud_save_lock:
            if self._cloud_save_task and not self._cloud_save_task.done():
                target = self._cloud_save_task
            self._cloud_save_task = None

        if target is not None:
            target.cancel()
            try:
                await target
            except BaseException:
                pass

    def _new_cloud_save_restore_state(self) -> Dict[str, Any]:
        """构建云存档恢复任务默认状态。"""
        return {
            "stage": "idle",
            "message": "未开始",
            "reason": "",
            "running": False,
            "progress": 0.0,
            "target_game_id": "",
            "target_game_title": "",
            "target_game_key": "",
            "target_version": "",
            "selected_entry_ids": [],
            "selected_target_dir": "",
            "requires_confirmation": False,
            "conflict_count": 0,
            "conflict_samples": [],
            "restored_files": 0,
            "restored_entries": 0,
            "results": [],
            "diagnostics": {},
            "updated_at": _now_wall_ts(),
            "last_result": dict(self.store.cloud_save_restore_last_result or {}),
        }

    def _copy_cloud_save_restore_result(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """复制单条恢复结果并规范字段。"""
        result = dict(item or {})
        result["entry_id"] = str(result.get("entry_id", "") or "")
        result["entry_name"] = str(result.get("entry_name", "") or "")
        result["status"] = str(result.get("status", "") or "")
        result["reason"] = str(result.get("reason", "") or "")
        result["file_count"] = max(0, _safe_int(result.get("file_count"), 0))
        diagnostics = result.get("diagnostics", {})
        result["diagnostics"] = dict(diagnostics) if isinstance(diagnostics, dict) else {}
        return result

    def _cloud_save_restore_state_snapshot_locked(self) -> Dict[str, Any]:
        """在已持锁场景下复制恢复状态。"""
        state = dict(self._cloud_save_restore_state or {})
        selected_entry_ids = state.get("selected_entry_ids", [])
        if isinstance(selected_entry_ids, list):
            state["selected_entry_ids"] = [str(item or "") for item in selected_entry_ids if str(item or "").strip()]
        else:
            state["selected_entry_ids"] = []

        conflict_samples = state.get("conflict_samples", [])
        if isinstance(conflict_samples, list):
            state["conflict_samples"] = [str(item or "") for item in conflict_samples if str(item or "").strip()]
        else:
            state["conflict_samples"] = []

        results = state.get("results", [])
        if isinstance(results, list):
            state["results"] = [
                self._copy_cloud_save_restore_result(item)
                for item in results
                if isinstance(item, dict)
            ]
        else:
            state["results"] = []

        diagnostics = state.get("diagnostics", {})
        state["diagnostics"] = dict(diagnostics) if isinstance(diagnostics, dict) else {}
        last_result = state.get("last_result", {})
        state["last_result"] = dict(last_result) if isinstance(last_result, dict) else {}
        return state

    async def _set_cloud_save_restore_state(self, **patch: Any) -> Dict[str, Any]:
        """更新恢复状态并返回快照。"""
        async with self._cloud_save_restore_lock:
            state = self._cloud_save_restore_state
            for key, value in patch.items():
                if key == "stage":
                    stage = str(value or "idle")
                    if stage not in CLOUD_SAVE_RESTORE_TASK_STAGES:
                        stage = "idle"
                    state["stage"] = stage
                    continue
                if key == "running":
                    state["running"] = bool(value)
                    continue
                if key == "progress":
                    try:
                        progress = float(value)
                    except Exception:
                        progress = 0.0
                    state["progress"] = max(0.0, min(100.0, round(progress, 2)))
                    continue
                if key in {"message", "reason", "target_game_id", "target_game_title", "target_game_key", "target_version", "selected_target_dir"}:
                    state[key] = str(value or "")
                    continue
                if key in {"requires_confirmation"}:
                    state[key] = bool(value)
                    continue
                if key in {"conflict_count", "restored_files", "restored_entries"}:
                    state[key] = max(0, _safe_int(value, 0))
                    continue
                if key == "selected_entry_ids":
                    rows = value if isinstance(value, list) else []
                    state["selected_entry_ids"] = [str(item or "") for item in rows if str(item or "").strip()]
                    continue
                if key == "conflict_samples":
                    rows = value if isinstance(value, list) else []
                    state["conflict_samples"] = [str(item or "") for item in rows if str(item or "").strip()]
                    continue
                if key == "results":
                    rows = value if isinstance(value, list) else []
                    state["results"] = [
                        self._copy_cloud_save_restore_result(item)
                        for item in rows
                        if isinstance(item, dict)
                    ]
                    continue
                if key == "diagnostics":
                    state["diagnostics"] = dict(value) if isinstance(value, dict) else {}
                    continue
                if key == "last_result":
                    state["last_result"] = dict(value) if isinstance(value, dict) else {}
                    continue
                state[key] = value

            state["updated_at"] = _now_wall_ts()
            return self._cloud_save_restore_state_snapshot_locked()

    async def _get_cloud_save_restore_state_snapshot(self) -> Dict[str, Any]:
        """获取云存档恢复状态快照。"""
        async with self._cloud_save_restore_lock:
            return self._cloud_save_restore_state_snapshot_locked()

    async def _clear_cloud_save_restore_plan(self) -> None:
        """清理恢复计划临时资源。"""
        async with self._cloud_save_restore_lock:
            plan = dict(self._cloud_save_restore_plan or {})
            self._cloud_save_restore_plan = {}
        self._cleanup_cloud_save_temp_paths(list(plan.get("temp_paths") or []))

    async def get_cloud_save_upload_status(self) -> Dict[str, Any]:
        """返回云存档上传任务状态。"""
        return {"state": await self._get_cloud_save_state_snapshot()}

    async def start_cloud_save_upload(self) -> Dict[str, Any]:
        """启动云存档上传任务。"""
        login_ok, account, _message = await self.check_login_state()
        if not login_ok:
            raise TianyiApiError("未登录，请先登录天翼云账号")

        cookie = str(self.store.login.cookie or "").strip()
        if not cookie:
            raise TianyiApiError("缺少有效登录态，请重新登录")

        async with self._cloud_save_lock:
            task = self._cloud_save_task
            if task is not None and task.done():
                self._cloud_save_task = None
                task = None

            if task is not None:
                return {
                    "accepted": False,
                    "message": "已有云存档上传任务正在运行",
                    "state": self._cloud_save_state_snapshot_locked(),
                }

            self._cloud_save_state = self._new_cloud_save_state()
            self._cloud_save_state.update(
                {
                    "stage": "scanning",
                    "message": "正在准备云存档上传任务",
                    "reason": "",
                    "running": True,
                    "progress": 0.0,
                    "current_game": "",
                    "diagnostics": {"user_account": account},
                    "updated_at": _now_wall_ts(),
                }
            )
            self._cloud_save_task = asyncio.create_task(
                self._run_cloud_save_upload_task(cookie=cookie, user_account=account),
                name="freedeck-cloud-save-upload",
            )
            snapshot = self._cloud_save_state_snapshot_locked()

        return {
            "accepted": True,
            "message": "云存档上传任务已启动",
            "state": snapshot,
        }

    async def get_cloud_save_restore_status(self) -> Dict[str, Any]:
        """返回云存档恢复任务状态。"""
        return {"state": await self._get_cloud_save_restore_state_snapshot()}

    async def list_cloud_save_restore_options(self) -> Dict[str, Any]:
        """列出可恢复的云存档版本（按游戏分组）。"""
        login_ok, _account, _message = await self.check_login_state()
        if not login_ok:
            raise TianyiApiError("未登录，请先登录天翼云账号")

        cookie = str(self.store.login.cookie or "").strip()
        if not cookie:
            raise TianyiApiError("缺少有效登录态，请重新登录")

        await self._set_cloud_save_restore_state(
            stage="listing",
            running=True,
            message="正在拉取云存档版本列表",
            reason="",
            progress=0.0,
            diagnostics={},
        )

        games = self._collect_cloud_restore_games()
        grouped: List[Dict[str, Any]] = []
        diagnostics: List[Dict[str, Any]] = []

        try:
            total = len(games)
            for index, game in enumerate(games, start=1):
                game_id = str(game.get("game_id", "") or "").strip()
                game_title = str(game.get("game_title", "") or "").strip() or "未命名游戏"
                game_key = str(game.get("game_key", "") or "").strip()

                item: Dict[str, Any] = {
                    "game_id": game_id,
                    "game_title": game_title,
                    "game_key": game_key,
                    "versions": [],
                    "available": False,
                    "reason": "",
                }

                try:
                    listed = await list_cloud_archives(cookie=cookie, remote_folder_parts=[game_key])
                    files = listed.get("files")
                    if not isinstance(files, list):
                        files = []
                    versions: List[Dict[str, Any]] = []
                    for file_row in files:
                        if not isinstance(file_row, dict):
                            continue
                        name = str(file_row.get("name", "") or "").strip()
                        if not name.lower().endswith(".7z"):
                            continue
                        ts = self._parse_cloud_save_version_timestamp(name)
                        versions.append(
                            {
                                "version_name": name,
                                "timestamp": ts,
                                "display_time": self._format_cloud_save_version_time(ts, name),
                                "size_bytes": max(0, _safe_int(file_row.get("size"), 0)),
                                "file_id": str(file_row.get("file_id", "") or ""),
                                "last_op_time": str(file_row.get("last_op_time", "") or ""),
                            }
                        )
                    versions.sort(
                        key=lambda row: (
                            -_safe_int(row.get("timestamp"), 0),
                            str(row.get("version_name", "") or ""),
                        )
                    )

                    item["versions"] = versions
                    item["available"] = bool(versions)
                    if not versions:
                        item["reason"] = "no_valid_versions"
                    diagnostics.append(
                        {
                            "game_key": game_key,
                            "exists": bool(listed.get("exists", False)),
                            "file_count": len(versions),
                            "trace": listed.get("trace", []),
                        }
                    )
                except TianyiApiError as exc:
                    item["available"] = False
                    item["reason"] = "list_failed"
                    diagnostics.append(
                        {
                            "game_key": game_key,
                            "error": str(exc),
                            "api_diagnostics": dict(getattr(exc, "diagnostics", {}) or {}),
                        }
                    )

                grouped.append(item)
                progress = 100.0 if total <= 0 else (float(index) / float(total)) * 100.0
                await self._set_cloud_save_restore_state(
                    stage="listing",
                    running=True,
                    message=f"正在拉取版本列表 {index}/{total}",
                    progress=progress,
                )
        finally:
            await self._set_cloud_save_restore_state(
                stage="completed",
                running=False,
                progress=100.0,
                message="云存档版本列表已更新",
                diagnostics={"games": len(grouped), "details": diagnostics},
            )

        return {
            "games": grouped,
            "updated_at": _now_wall_ts(),
        }

    async def list_cloud_save_restore_entries(
        self,
        *,
        game_id: str,
        game_key: str,
        game_title: str,
        version_name: str,
    ) -> Dict[str, Any]:
        """读取指定版本的可选存档项。"""
        login_ok, _account, _message = await self.check_login_state()
        if not login_ok:
            raise TianyiApiError("未登录，请先登录天翼云账号")

        cookie = str(self.store.login.cookie or "").strip()
        if not cookie:
            raise TianyiApiError("缺少有效登录态，请重新登录")

        normalized_game_id = str(game_id or "").strip()
        normalized_game_title = str(game_title or "").strip()
        normalized_game_key = str(game_key or "").strip()
        normalized_version = str(version_name or "").strip()
        if not normalized_game_key:
            normalized_game_key = self._build_cloud_save_game_key(normalized_game_id, normalized_game_title)
        if not normalized_version:
            raise TianyiApiError("缺少版本名称")

        await self._set_cloud_save_restore_state(
            stage="planning",
            running=True,
            message="正在读取存档项",
            reason="",
            progress=0.0,
            target_game_id=normalized_game_id,
            target_game_title=normalized_game_title,
            target_game_key=normalized_game_key,
            target_version=normalized_version,
            diagnostics={},
        )

        bundle = await self._download_and_extract_cloud_restore_version(
            cookie=cookie,
            game_key=normalized_game_key,
            version_name=normalized_version,
        )
        try:
            extract_dir = str(bundle.get("extract_dir", "") or "")
            entries = [dict(item) for item in list(bundle.get("entries") or []) if isinstance(item, dict)]
            entry_views: List[Dict[str, Any]] = []
            for item in entries:
                entry_id = str(item.get("entry_id", "") or "").strip()
                entry_name = str(item.get("entry_name", "") or "").strip() or entry_id
                rel_path = str(item.get("archive_rel_path", "") or "").strip().replace("\\", "/").strip("/")
                entry_root = os.path.realpath(os.path.join(extract_dir, rel_path)) if rel_path else extract_dir
                file_count = 0
                if os.path.isfile(entry_root):
                    file_count = 1
                elif os.path.isdir(entry_root):
                    for _dirpath, _dirnames, filenames in os.walk(entry_root):
                        file_count += len(filenames)
                entry_views.append(
                    {
                        "entry_id": entry_id,
                        "entry_name": entry_name,
                        "archive_rel_path": rel_path,
                        "file_count": max(0, file_count),
                    }
                )

            await self._set_cloud_save_restore_state(
                stage="ready",
                running=False,
                progress=100.0,
                message=f"已读取 {len(entry_views)} 个存档项",
                selected_entry_ids=[str(row.get("entry_id", "") or "") for row in entry_views],
                diagnostics={},
            )

            return {
                "game_id": normalized_game_id,
                "game_key": normalized_game_key,
                "game_title": normalized_game_title,
                "version_name": normalized_version,
                "entries": entry_views,
            }
        finally:
            self._cleanup_cloud_save_temp_paths([str(bundle.get("temp_dir", "") or "")])

    async def plan_cloud_save_restore(
        self,
        *,
        game_id: str,
        game_key: str,
        game_title: str,
        version_name: str,
        selected_entry_ids: Sequence[str],
        target_dir: str = "",
    ) -> Dict[str, Any]:
        """生成恢复计划（冲突探测，不写入）。"""
        login_ok, _account, _message = await self.check_login_state()
        if not login_ok:
            raise TianyiApiError("未登录，请先登录天翼云账号")

        cookie = str(self.store.login.cookie or "").strip()
        if not cookie:
            raise TianyiApiError("缺少有效登录态，请重新登录")

        normalized_game_id = str(game_id or "").strip()
        normalized_game_title = str(game_title or "").strip() or "未命名游戏"
        normalized_game_key = str(game_key or "").strip()
        normalized_version = str(version_name or "").strip()
        if not normalized_game_key:
            normalized_game_key = self._build_cloud_save_game_key(normalized_game_id, normalized_game_title)
        if not normalized_version:
            raise TianyiApiError("缺少版本名称")

        await self._clear_cloud_save_restore_plan()
        await self._set_cloud_save_restore_state(
            stage="planning",
            running=True,
            progress=0.0,
            message="正在生成恢复计划",
            reason="",
            target_game_id=normalized_game_id,
            target_game_title=normalized_game_title,
            target_game_key=normalized_game_key,
            target_version=normalized_version,
            selected_target_dir="",
            selected_entry_ids=[],
            requires_confirmation=False,
            conflict_count=0,
            conflict_samples=[],
            restored_files=0,
            restored_entries=0,
            results=[],
            diagnostics={},
        )

        bundle = await self._download_and_extract_cloud_restore_version(
            cookie=cookie,
            game_key=normalized_game_key,
            version_name=normalized_version,
        )

        temp_dir = str(bundle.get("temp_dir", "") or "")
        extract_dir = str(bundle.get("extract_dir", "") or "")
        manifest = dict(bundle.get("manifest") or {})
        manifest_playtime = self._extract_manifest_playtime_payload(manifest)
        entries = [dict(item) for item in list(bundle.get("entries") or []) if isinstance(item, dict)]
        available_entry_ids = [str(item.get("entry_id", "") or "").strip() for item in entries if str(item.get("entry_id", "") or "").strip()]
        selected_ids = [str(item or "").strip() for item in list(selected_entry_ids or []) if str(item or "").strip()]
        if not selected_ids:
            selected_ids = list(available_entry_ids)

        entry_target_dirs: Dict[str, str] = {}
        compat_user_dir = self._resolve_current_compat_user_dir(normalized_game_id)
        selected_set = {str(item or "").strip() for item in list(selected_ids or []) if str(item or "").strip()}
        if compat_user_dir and selected_set:
            for entry in entries:
                entry_id = str(entry.get("entry_id", "") or "").strip()
                if not entry_id or entry_id not in selected_set:
                    continue
                source_path = str(entry.get("source_path", "") or "").strip()
                archive_rel_path = str(entry.get("archive_rel_path", "") or "").strip().replace("\\", "/").strip("/")
                relative = self._extract_proton_relative_path(
                    source_path=source_path,
                    archive_rel_path=archive_rel_path,
                )
                if not relative:
                    continue
                candidate = os.path.join(compat_user_dir, *[part for part in relative.split("/") if part])
                normalized_candidate = self._normalize_dir_path(candidate)
                if normalized_candidate:
                    entry_target_dirs[entry_id] = normalized_candidate

        auto_target_ready = bool(selected_set) and len(entry_target_dirs) >= len(selected_set)
        target_candidates: List[str] = []
        diagnostics: Dict[str, Any] = {
            "compat_user_dir": compat_user_dir,
            "auto_target_from_manifest": auto_target_ready,
            "entry_target_dirs": dict(entry_target_dirs),
        }

        normalized_target = self._normalize_dir_path(str(target_dir or "").strip())
        if auto_target_ready:
            if normalized_target and normalized_target not in set(entry_target_dirs.values()):
                diagnostics["requested_target_dir"] = normalized_target
                diagnostics["target_dir_stale"] = True
            normalized_target = compat_user_dir or self._normalize_dir_path(next(iter(entry_target_dirs.values()), ""))
        else:
            target_result = self._resolve_cloud_restore_target_candidates(
                game_id=normalized_game_id,
                game_key=normalized_game_key,
                game_title=normalized_game_title,
                entries=entries,
            )
            target_candidates = [str(item or "") for item in list(target_result.get("candidates") or []) if str(item or "").strip()]
            diagnostics = {
                **diagnostics,
                **dict(target_result.get("diagnostics") or {}),
            }

            if not target_candidates:
                self._cleanup_cloud_save_temp_paths([temp_dir])
                await self._set_cloud_save_restore_state(
                    stage="failed",
                    running=False,
                    progress=100.0,
                    message="未找到可恢复的目标目录",
                    reason="target_not_found",
                    selected_entry_ids=selected_ids,
                    diagnostics=diagnostics,
                )
                return {
                    "accepted": False,
                    "reason": "target_not_found",
                    "message": "未找到可恢复的目标目录，请确保游戏已安装并至少启动过一次",
                    "target_candidates": [],
                    "available_entries": entries,
                }

            if normalized_target and normalized_target not in target_candidates:
                diagnostics["requested_target_dir"] = normalized_target
                diagnostics["target_dir_stale"] = True
                normalized_target = ""

            if not normalized_target:
                if len(target_candidates) == 1:
                    normalized_target = target_candidates[0]
                else:
                    self._cleanup_cloud_save_temp_paths([temp_dir])
                    await self._set_cloud_save_restore_state(
                        stage="ready",
                        running=False,
                        progress=100.0,
                        message="检测到多个目标目录，请先选择",
                        reason="target_selection_required",
                        selected_entry_ids=selected_ids,
                        diagnostics=diagnostics,
                    )
                    return {
                        "accepted": False,
                        "reason": "target_selection_required",
                        "message": "检测到多个目标目录，请先选择恢复目标",
                        "target_candidates": target_candidates,
                        "available_entries": entries,
                    }

        copy_plan = self._build_restore_copy_plan(
            extract_dir=extract_dir,
            entries=entries,
            selected_entry_ids=selected_ids,
            target_dir=normalized_target,
            entry_target_dirs=entry_target_dirs,
        )
        copy_pairs = list(copy_plan.get("copy_pairs") or [])
        plan_items = [dict(item) for item in list(copy_plan.get("plan_items") or []) if isinstance(item, dict)]
        conflict_count = max(0, _safe_int(copy_plan.get("conflict_count"), 0))
        conflict_samples = [str(item or "") for item in list(copy_plan.get("conflict_samples") or []) if str(item or "").strip()]
        requires_confirmation = conflict_count > 0

        plan_id = uuid.uuid4().hex
        async with self._cloud_save_restore_lock:
            self._cloud_save_restore_plan = {
                "plan_id": plan_id,
                "temp_paths": [temp_dir],
                "copy_pairs": copy_pairs,
                "plan_items": plan_items,
                "requires_confirmation": requires_confirmation,
                "conflict_count": conflict_count,
                "conflict_samples": conflict_samples,
                "target_dir": normalized_target,
                "entry_target_dirs": dict(entry_target_dirs),
                "game_id": normalized_game_id,
                "game_key": normalized_game_key,
                "game_title": normalized_game_title,
                "version_name": normalized_version,
                "selected_entry_ids": selected_ids,
                "manifest_playtime": manifest_playtime,
            }

        await self._set_cloud_save_restore_state(
            stage="ready",
            running=False,
            progress=100.0,
            message="恢复计划已生成",
            reason="",
            selected_entry_ids=selected_ids,
            selected_target_dir=normalized_target,
            requires_confirmation=requires_confirmation,
            conflict_count=conflict_count,
            conflict_samples=conflict_samples,
            restored_files=0,
            restored_entries=0,
            diagnostics={
                **diagnostics,
                "target_candidates": target_candidates,
                "plan_items": plan_items,
                "manifest_playtime": manifest_playtime,
            },
        )

        return {
            "accepted": True,
            "plan_id": plan_id,
            "message": "恢复计划已生成",
            "reason": "",
            "requires_confirmation": requires_confirmation,
            "conflict_count": conflict_count,
            "conflict_samples": conflict_samples,
            "target_candidates": target_candidates,
            "selected_target_dir": normalized_target,
            "selected_entry_ids": selected_ids,
            "available_entries": entries,
            "restorable_files": len(copy_pairs),
            "restorable_entries": len(plan_items),
        }

    async def apply_cloud_save_restore(
        self,
        *,
        plan_id: str,
        confirm_overwrite: bool = False,
    ) -> Dict[str, Any]:
        """执行恢复计划（确认后写入）。"""
        normalized_plan_id = str(plan_id or "").strip()
        if not normalized_plan_id:
            raise TianyiApiError("缺少 plan_id")

        async with self._cloud_save_restore_lock:
            plan = dict(self._cloud_save_restore_plan or {})

        if not plan or str(plan.get("plan_id", "") or "").strip() != normalized_plan_id:
            raise TianyiApiError("恢复计划不存在或已过期，请重新规划")

        requires_confirmation = bool(plan.get("requires_confirmation", False))
        if requires_confirmation and not bool(confirm_overwrite):
            result_payload = {
                "status": "cancelled",
                "reason": "user_cancelled",
                "message": "用户已取消覆盖恢复",
                "game_id": str(plan.get("game_id", "") or ""),
                "game_key": str(plan.get("game_key", "") or ""),
                "game_title": str(plan.get("game_title", "") or ""),
                "version_name": str(plan.get("version_name", "") or ""),
                "target_dir": str(plan.get("target_dir", "") or ""),
                "selected_entry_ids": list(plan.get("selected_entry_ids") or []),
                "restored_files": 0,
                "restored_entries": 0,
                "conflicts_overwritten": 0,
                "results": [],
                "diagnostics": {"requires_confirmation": True},
                "finished_at": _now_wall_ts(),
            }
            await asyncio.to_thread(self.store.set_cloud_save_restore_last_result, result_payload)
            await self._set_cloud_save_restore_state(
                stage="failed",
                running=False,
                progress=100.0,
                message=result_payload["message"],
                reason=result_payload["reason"],
                restored_files=0,
                restored_entries=0,
                results=[],
                diagnostics=dict(result_payload.get("diagnostics", {}) or {}),
                last_result=result_payload,
            )
            await self._clear_cloud_save_restore_plan()
            return result_payload

        copy_pairs = list(plan.get("copy_pairs") or [])
        plan_items = [dict(item) for item in list(plan.get("plan_items") or []) if isinstance(item, dict)]
        target_dir = str(plan.get("target_dir", "") or "")
        restored_files = 0
        results: List[Dict[str, Any]] = []
        exception_text = ""

        await self._set_cloud_save_restore_state(
            stage="applying",
            running=True,
            progress=0.0,
            message="正在恢复存档",
            reason="",
            restored_files=0,
            restored_entries=0,
            results=[],
        )

        try:
            total_files = len(copy_pairs)
            for index, (src, dst) in enumerate(copy_pairs, start=1):
                src_file = os.path.realpath(os.path.expanduser(str(src or "").strip()))
                dst_file = os.path.realpath(os.path.expanduser(str(dst or "").strip()))
                if not src_file or not os.path.isfile(src_file):
                    continue
                parent = os.path.dirname(dst_file)
                os.makedirs(parent, exist_ok=True)
                if os.path.isdir(dst_file):
                    shutil.rmtree(dst_file, ignore_errors=False)
                elif os.path.exists(dst_file):
                    os.remove(dst_file)
                shutil.copy2(src_file, dst_file)
                restored_files += 1
                progress = 100.0 if total_files <= 0 else (float(index) / float(total_files)) * 100.0
                await self._set_cloud_save_restore_state(
                    stage="applying",
                    running=True,
                    progress=progress,
                    message=f"正在恢复文件 {index}/{total_files}",
                    restored_files=restored_files,
                )

            for item in plan_items:
                results.append(
                    {
                        "entry_id": str(item.get("entry_id", "") or ""),
                        "entry_name": str(item.get("entry_name", "") or ""),
                        "status": "restored",
                        "reason": "",
                        "file_count": max(0, _safe_int(item.get("file_count"), 0)),
                        "diagnostics": {},
                    }
                )

            manifest_playtime = dict(plan.get("manifest_playtime") or {})
            playtime_merge: Dict[str, Any] = {}
            try:
                playtime_merge = await self._merge_cloud_restore_playtime(
                    game_id=str(plan.get("game_id", "") or ""),
                    game_key=str(plan.get("game_key", "") or ""),
                    target_dir=target_dir,
                    manifest_playtime=manifest_playtime,
                )
            except Exception as exc:
                playtime_merge = {
                    "merged": False,
                    "reason": "playtime_merge_failed",
                    "message": f"游玩时长合并失败：{exc}",
                }

            result_payload = {
                "status": "success",
                "reason": "",
                "message": f"云存档恢复完成（恢复文件 {restored_files}）",
                "game_id": str(plan.get("game_id", "") or ""),
                "game_key": str(plan.get("game_key", "") or ""),
                "game_title": str(plan.get("game_title", "") or ""),
                "version_name": str(plan.get("version_name", "") or ""),
                "target_dir": target_dir,
                "selected_entry_ids": list(plan.get("selected_entry_ids") or []),
                "restored_files": restored_files,
                "restored_entries": len(plan_items),
                "conflicts_overwritten": max(0, _safe_int(plan.get("conflict_count"), 0)),
                "results": results,
                "diagnostics": {
                    "requires_confirmation": requires_confirmation,
                    "conflict_samples": list(plan.get("conflict_samples") or []),
                    "entry_target_dirs": dict(plan.get("entry_target_dirs") or {}),
                    "playtime_merge": playtime_merge,
                },
                "finished_at": _now_wall_ts(),
            }
            await asyncio.to_thread(self.store.set_cloud_save_restore_last_result, result_payload)
            await self._set_cloud_save_restore_state(
                stage="completed",
                running=False,
                progress=100.0,
                message=str(result_payload.get("message", "") or "恢复完成"),
                reason="",
                restored_files=restored_files,
                restored_entries=len(plan_items),
                results=results,
                diagnostics=dict(result_payload.get("diagnostics", {}) or {}),
                last_result=result_payload,
            )
            return result_payload
        except Exception as exc:
            exception_text = str(exc)
            result_payload = {
                "status": "failed",
                "reason": "apply_failed",
                "message": f"云存档恢复失败：{exc}",
                "game_id": str(plan.get("game_id", "") or ""),
                "game_key": str(plan.get("game_key", "") or ""),
                "game_title": str(plan.get("game_title", "") or ""),
                "version_name": str(plan.get("version_name", "") or ""),
                "target_dir": target_dir,
                "selected_entry_ids": list(plan.get("selected_entry_ids") or []),
                "restored_files": restored_files,
                "restored_entries": 0,
                "conflicts_overwritten": 0,
                "results": results,
                "diagnostics": {"exception": exception_text},
                "finished_at": _now_wall_ts(),
            }
            await asyncio.to_thread(self.store.set_cloud_save_restore_last_result, result_payload)
            await self._set_cloud_save_restore_state(
                stage="failed",
                running=False,
                progress=100.0,
                message=str(result_payload.get("message", "") or "恢复失败"),
                reason=str(result_payload.get("reason", "") or "apply_failed"),
                restored_files=restored_files,
                restored_entries=0,
                results=results,
                diagnostics=dict(result_payload.get("diagnostics", {}) or {}),
                last_result=result_payload,
            )
            return result_payload
        finally:
            await self._clear_cloud_save_restore_plan()

    def _normalize_existing_dir(self, path: str) -> str:
        """规范化并校验目录存在。"""
        normalized = self._normalize_dir_path(path)
        if not normalized or not os.path.isdir(normalized):
            return ""
        return normalized

    def _normalize_dir_path(self, path: str) -> str:
        """规范化目录路径（允许目录暂不存在）。"""
        raw = str(path or "").strip()
        if not raw:
            return ""
        try:
            normalized = os.path.realpath(os.path.expanduser(raw))
        except Exception:
            return ""
        return str(normalized or "").strip()

    def _dedupe_paths(self, paths: Sequence[str], *, require_existing: bool = False) -> List[str]:
        """目录去重并去除被父目录覆盖的子目录。"""
        normalized: List[str] = []
        seen = set()
        for raw in list(paths or []):
            path = self._normalize_dir_path(str(raw or ""))
            if not path:
                continue
            if require_existing and not os.path.isdir(path):
                continue
            if path in seen:
                continue
            seen.add(path)
            normalized.append(path)

        normalized.sort(key=lambda item: (len(item), item))
        compacted: List[str] = []
        for path in normalized:
            covered = False
            for parent in compacted:
                if path == parent or path.startswith(parent + os.sep):
                    covered = True
                    break
            if not covered:
                compacted.append(path)
            if len(compacted) >= CLOUD_SAVE_MAX_SOURCE_PATHS:
                break
        return compacted

    def _extract_proton_relative_path(self, *, source_path: str, archive_rel_path: str = "") -> str:
        """从 source_path / archive_rel_path 提取 Proton 用户目录下的相对路径。"""

        patterns: Tuple[Tuple[str, ...], ...] = (
            ("Documents", "My Games"),
            ("Saved Games",),
            ("AppData", "Roaming"),
            ("AppData", "LocalLow"),
            ("AppData", "Local"),
        )

        def _match(parts: Sequence[str]) -> str:
            seq = [str(item or "").strip() for item in list(parts or []) if str(item or "").strip()]
            if not seq:
                return ""
            lowered = [item.lower() for item in seq]
            for pattern in patterns:
                token = [str(item or "").strip().lower() for item in pattern if str(item or "").strip()]
                if not token:
                    continue
                max_start = len(lowered) - len(token)
                for idx in range(max(0, max_start + 1)):
                    if lowered[idx: idx + len(token)] == token:
                        return "/".join(seq[idx:])
            return ""

        source_parts = [part for part in Path(self._normalize_dir_path(source_path)).parts if str(part or "").strip()]
        matched = _match(source_parts)
        if matched:
            return matched

        rel_parts = [
            part
            for part in str(archive_rel_path or "").replace("\\", "/").split("/")
            if part and part not in {".", ".."}
        ]
        return _match(rel_parts)

    def _resolve_current_compat_user_dir(self, game_id: str) -> str:
        """解析当前游戏对应的 Proton 用户目录。"""
        target_game_id = str(game_id or "").strip()
        if not target_game_id:
            return ""
        try:
            result = resolve_tianyi_shortcut_sync(game_id=target_game_id)
        except Exception:
            return ""
        return self._normalize_existing_dir(str(result.get("compat_user_dir", "") or ""))

    def _build_cloud_save_game_key(self, game_id: str, game_title: str) -> str:
        """生成稳定 game-key。"""
        token = re.sub(r"[^a-zA-Z0-9._-]+", "_", str(game_id or "").strip()).strip("_")
        if token:
            return token.lower()

        title_token = re.sub(r"[^a-zA-Z0-9._-]+", "_", str(game_title or "").strip()).strip("_")
        if title_token:
            return title_token.lower()

        return f"game_{_now_wall_ts()}"

    def _parse_cloud_save_version_timestamp(self, version_name: str) -> int:
        """从云端版本名中提取时间戳（秒）。"""
        raw_name = str(version_name or "").strip()
        if not raw_name:
            return 0
        stem = raw_name
        if stem.lower().endswith(".7z"):
            stem = stem[:-3]
        stem = stem.strip()
        if not re.fullmatch(r"\d{8}_\d{6}", stem):
            return 0
        try:
            return int(time.mktime(time.strptime(stem, CLOUD_SAVE_DATE_FORMAT)))
        except Exception:
            return 0

    def _format_cloud_save_version_time(self, ts: int, fallback: str) -> str:
        """格式化版本时间显示。"""
        value = _safe_int(ts, 0)
        if value <= 0:
            return str(fallback or "")
        try:
            return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(value))
        except Exception:
            return str(fallback or "")

    def _collect_cloud_restore_games(self) -> List[Dict[str, Any]]:
        """按 game_key 聚合当前已安装游戏。"""
        records = list(self.store.installed_games or [])
        records.sort(key=lambda item: int(item.updated_at or 0), reverse=True)

        grouped: Dict[str, Dict[str, Any]] = {}
        for record in records:
            game_id = str(record.game_id or "").strip()
            game_title = str(record.game_title or "").strip() or "未命名游戏"
            game_key = self._build_cloud_save_game_key(game_id, game_title)
            install_path = self._normalize_existing_dir(str(record.install_path or "").strip())
            if not install_path:
                continue

            item = grouped.get(game_key)
            if item is None:
                item = {
                    "game_id": game_id,
                    "game_title": game_title,
                    "game_key": game_key,
                    "install_paths": [],
                    "updated_at": _safe_int(record.updated_at, 0),
                }
                grouped[game_key] = item

            paths = item.get("install_paths")
            if not isinstance(paths, list):
                paths = []
                item["install_paths"] = paths
            if install_path not in paths:
                paths.append(install_path)

            if game_id and not str(item.get("game_id", "") or "").strip():
                item["game_id"] = game_id
            if _safe_int(record.updated_at, 0) > _safe_int(item.get("updated_at"), 0):
                item["updated_at"] = _safe_int(record.updated_at, 0)
                if game_title:
                    item["game_title"] = game_title
                if game_id:
                    item["game_id"] = game_id

        games = list(grouped.values())
        games.sort(
            key=lambda row: (
                -_safe_int(row.get("updated_at"), 0),
                str(row.get("game_title", "") or "").lower(),
                str(row.get("game_key", "") or ""),
            )
        )
        return games

    def _normalize_cloud_restore_entries(self, *, manifest: Dict[str, Any], extract_dir: str) -> List[Dict[str, Any]]:
        """从 manifest 或归档目录生成可选存档项。"""
        result: List[Dict[str, Any]] = []
        extract_root = self._normalize_existing_dir(extract_dir)
        if not extract_root:
            return result

        entries = manifest.get("entries")
        if isinstance(entries, list):
            for idx, item in enumerate(entries, start=1):
                if not isinstance(item, dict):
                    continue
                entry_id = str(item.get("entry_id", "") or "").strip() or f"entry_{idx}"
                entry_name = str(item.get("name", "") or item.get("entry_name", "") or "").strip() or entry_id
                rel_path = str(item.get("archive_rel_path", "") or "").strip().replace("\\", "/").strip("/")
                if not rel_path:
                    rel_path = str(item.get("relative_path", "") or "").strip().replace("\\", "/").strip("/")
                abs_path = os.path.realpath(os.path.join(extract_root, rel_path)) if rel_path else extract_root
                if not abs_path.startswith(extract_root):
                    continue
                if not os.path.exists(abs_path):
                    continue
                result.append(
                    {
                        "entry_id": entry_id,
                        "entry_name": entry_name,
                        "archive_rel_path": rel_path,
                        "source_path": str(item.get("source_path", "") or ""),
                    }
                )

        if result:
            return result

        source_paths = manifest.get("source_paths")
        if isinstance(source_paths, list):
            for idx, raw in enumerate(source_paths, start=1):
                source_path = str(raw or "").strip()
                if not source_path:
                    continue
                name = os.path.basename(source_path.rstrip("/\\")).strip() or f"entry_{idx}"
                result.append(
                    {
                        "entry_id": f"entry_{idx}",
                        "entry_name": name,
                        "archive_rel_path": "",
                        "source_path": source_path,
                    }
                )

        if result:
            fallback_entries: List[Dict[str, Any]] = []
            try:
                roots = sorted(os.listdir(extract_root))
            except Exception:
                roots = []
            for idx, name in enumerate(roots, start=1):
                root_name = str(name or "").strip()
                if not root_name or root_name == "manifest.json":
                    continue
                abs_path = os.path.join(extract_root, root_name)
                if not os.path.exists(abs_path):
                    continue
                fallback_entries.append(
                    {
                        "entry_id": f"entry_{idx}",
                        "entry_name": root_name,
                        "archive_rel_path": root_name.replace("\\", "/"),
                        "source_path": "",
                    }
                )
            if fallback_entries:
                return fallback_entries

        return [
            {
                "entry_id": "entry_all",
                "entry_name": "全部存档",
                "archive_rel_path": "",
                "source_path": "",
            }
        ]

    def _extract_manifest_playtime_payload(self, manifest: Dict[str, Any]) -> Dict[str, Any]:
        """从云存档 manifest 中提取游玩时长信息。"""
        if not isinstance(manifest, dict):
            return {}

        playtime_raw = manifest.get("playtime")
        playtime = playtime_raw if isinstance(playtime_raw, dict) else {}

        seconds = max(0, _safe_int(playtime.get("seconds"), _safe_int(manifest.get("playtime_seconds"), 0)))
        sessions = max(0, _safe_int(playtime.get("sessions"), _safe_int(manifest.get("playtime_sessions"), 0)))
        last_played_at = max(
            0,
            _safe_int(playtime.get("last_played_at"), _safe_int(manifest.get("playtime_last_played_at"), 0)),
        )
        captured_at = max(
            0,
            _safe_int(playtime.get("captured_at"), _safe_int(manifest.get("playtime_captured_at"), 0)),
        )

        if seconds <= 0 and sessions <= 0 and last_played_at <= 0:
            return {}

        return {
            "seconds": seconds,
            "sessions": sessions,
            "last_played_at": last_played_at,
            "captured_at": captured_at,
        }

    async def _merge_cloud_restore_playtime(
        self,
        *,
        game_id: str,
        game_key: str,
        target_dir: str,
        manifest_playtime: Dict[str, Any],
    ) -> Dict[str, Any]:
        """将云存档中的游玩时长合并到本地记录（不降级覆盖）。"""
        payload = dict(manifest_playtime or {})
        if not payload:
            return {"merged": False, "reason": "playtime_missing", "message": "manifest 未包含游玩时长"}

        cloud_seconds = max(0, _safe_int(payload.get("seconds"), 0))
        cloud_sessions = max(0, _safe_int(payload.get("sessions"), 0))
        cloud_last_played_at = max(0, _safe_int(payload.get("last_played_at"), 0))
        if cloud_seconds <= 0 and cloud_sessions <= 0 and cloud_last_played_at <= 0:
            return {"merged": False, "reason": "playtime_empty", "message": "云端游玩时长为空"}

        target_game_id = str(game_id or "").strip()
        target_game_key = str(game_key or "").strip()
        normalized_target_dir = self._normalize_dir_path(str(target_dir or "").strip())

        record = self._find_installed_record(game_id=target_game_id, install_path=normalized_target_dir)
        if record is None and target_game_id:
            record = self._find_installed_record(game_id=target_game_id)
        if record is None and target_game_key:
            for item in list(self.store.installed_games or []):
                item_key = self._build_cloud_save_game_key(
                    str(item.game_id or "").strip(),
                    str(item.game_title or "").strip(),
                )
                if item_key == target_game_key:
                    record = item
                    break

        if record is None:
            return {"merged": False, "reason": "record_not_found", "message": "未找到本地安装记录"}

        local_seconds_before = max(0, _safe_int(record.playtime_seconds, 0))
        local_sessions_before = max(0, _safe_int(record.playtime_sessions, 0))
        local_last_played_before = max(0, _safe_int(record.playtime_last_played_at, 0))

        local_seconds_after = max(local_seconds_before, cloud_seconds)
        local_sessions_after = max(local_sessions_before, cloud_sessions)
        local_last_played_after = max(local_last_played_before, cloud_last_played_at)
        changed = (
            local_seconds_after != local_seconds_before
            or local_sessions_after != local_sessions_before
            or local_last_played_after != local_last_played_before
        )

        if not changed:
            return {
                "merged": False,
                "reason": "already_up_to_date",
                "message": "本地游玩时长不低于云端",
                "local_seconds": local_seconds_before,
                "cloud_seconds": cloud_seconds,
            }

        now_ts = _now_wall_ts()
        record.playtime_seconds = local_seconds_after
        record.playtime_sessions = local_sessions_after
        record.playtime_last_played_at = local_last_played_after
        record.updated_at = now_ts
        await asyncio.to_thread(self.store.save)
        self._invalidate_panel_cache(installed=True)
        return {
            "merged": True,
            "reason": "",
            "message": "已合并云端游玩时长",
            "local_seconds_before": local_seconds_before,
            "local_seconds_after": local_seconds_after,
            "cloud_seconds": cloud_seconds,
        }

    def _build_cloud_save_match_tokens(self, *, game_id: str, game_title: str, install_path: str) -> List[str]:
        """构建存档目录匹配词元。"""
        raw_values = [
            str(game_id or "").strip(),
            str(game_title or "").strip(),
            os.path.basename(str(install_path or "").strip()),
        ]

        token_set = set()
        for raw in raw_values:
            if not raw:
                continue
            lower = raw.lower()
            compact = re.sub(r"[^0-9a-z\u4e00-\u9fff]+", "", lower)
            if len(compact) >= 3:
                token_set.add(compact)
            for part in re.split(r"[^0-9a-z\u4e00-\u9fff]+", lower):
                text = str(part or "").strip()
                if len(text) >= 2:
                    token_set.add(text)

        return sorted(token_set, key=lambda item: (-len(item), item))

    def _dedupe_existing_paths(self, paths: Sequence[str]) -> List[str]:
        """目录去重并去除被父目录覆盖的子目录。"""
        return self._dedupe_paths(paths, require_existing=True)

    def _should_keep_cloud_save_dir(
        self,
        *,
        root: str,
        current: str,
        tokens: Sequence[str],
        keywords: Sequence[str],
    ) -> bool:
        """判断当前目录是否应作为存档候选。"""
        root_dir = str(root or "").strip()
        current_dir = str(current or "").strip()
        if not root_dir or not current_dir:
            return False
        if current_dir == root_dir:
            return False

        name = os.path.basename(current_dir).lower()
        try:
            rel = os.path.relpath(current_dir, root_dir).replace("\\", "/").lower()
        except Exception:
            rel = name

        token_hit = False
        for token in list(tokens or []):
            text = str(token or "").strip().lower()
            if len(text) < 2:
                continue
            if text in name or text in rel:
                token_hit = True
                break

        keyword_hit = False
        for keyword in list(keywords or []):
            text = str(keyword or "").strip().lower()
            if len(text) < 2:
                continue
            if text in name or text in rel:
                keyword_hit = True
                break

        if not token_hit and not keyword_hit:
            return False

        try:
            with os.scandir(current_dir) as it:
                for _ in it:
                    return True
        except Exception:
            return False
        return False

    def _scan_cloud_save_paths(
        self,
        *,
        root: str,
        tokens: Sequence[str],
        keywords: Sequence[str],
    ) -> List[str]:
        """在限定深度内扫描存档候选目录。"""
        root_dir = self._normalize_existing_dir(root)
        if not root_dir:
            return []

        base_depth = root_dir.count(os.sep)
        matches: List[str] = []
        for dirpath, dirnames, _filenames in os.walk(root_dir):
            depth = max(0, dirpath.count(os.sep) - base_depth)
            if depth >= CLOUD_SAVE_SCAN_MAX_DEPTH:
                dirnames[:] = []

            if self._should_keep_cloud_save_dir(
                root=root_dir,
                current=dirpath,
                tokens=tokens,
                keywords=keywords,
            ):
                matches.append(dirpath)
                if len(matches) >= CLOUD_SAVE_SCAN_MAX_MATCHES:
                    break

        return self._dedupe_existing_paths(matches)

    def _collect_cloud_save_paths_from_proton(
        self,
        *,
        compat_user_dir: str,
        tokens: Sequence[str],
    ) -> Tuple[List[str], Dict[str, Any]]:
        """从 Proton 前缀白名单目录采集存档路径。"""
        root = self._normalize_existing_dir(compat_user_dir)
        diagnostics: Dict[str, Any] = {"compat_user_dir": root, "scanned_bases": [], "matched": []}
        if not root:
            diagnostics["reason"] = "compat_user_dir_missing"
            return [], diagnostics

        collected: List[str] = []
        for parts in CLOUD_SAVE_PROTON_BASE_DIRS:
            base = os.path.join(root, *parts)
            if not os.path.isdir(base):
                continue
            diagnostics["scanned_bases"].append(base)
            collected.extend(self._scan_cloud_save_paths(root=base, tokens=tokens, keywords=()))

        merged = self._dedupe_existing_paths(collected)
        diagnostics["matched"] = list(merged)
        if not merged:
            diagnostics["reason"] = "save_path_not_found"
        return merged, diagnostics

    def _collect_cloud_save_paths_from_install(
        self,
        *,
        install_path: str,
        tokens: Sequence[str],
    ) -> Tuple[List[str], Dict[str, Any]]:
        """在安装目录白名单兜底采集存档路径。"""
        root = self._normalize_existing_dir(install_path)
        diagnostics: Dict[str, Any] = {
            "install_path": root,
            "keywords": list(CLOUD_SAVE_INSTALL_FALLBACK_DIRS),
            "matched": [],
        }
        if not root:
            diagnostics["reason"] = "install_path_missing"
            return [], diagnostics

        collected = self._scan_cloud_save_paths(
            root=root,
            tokens=tokens,
            keywords=CLOUD_SAVE_INSTALL_FALLBACK_DIRS,
        )
        diagnostics["matched"] = list(collected)
        if not collected:
            diagnostics["reason"] = "save_path_not_found"
        return collected, diagnostics

    async def _collect_cloud_save_candidates(self) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        """基于已安装记录构建云存档候选。"""
        records = list(self.store.installed_games or [])
        records.sort(key=lambda item: int(item.updated_at or 0), reverse=True)

        candidates: List[Dict[str, Any]] = []
        ignored: List[Dict[str, Any]] = []
        dedupe_keys = set()

        for record in records:
            game_id = str(record.game_id or "").strip()
            game_title = str(record.game_title or "").strip() or "未命名游戏"
            install_path = self._normalize_existing_dir(str(record.install_path or "").strip())
            if not install_path:
                ignored.append(
                    {
                        "game_id": game_id,
                        "game_title": game_title,
                        "reason": "install_path_missing",
                    }
                )
                continue

            dedupe_key = f"{game_id}|{install_path}"
            if dedupe_key in dedupe_keys:
                continue
            dedupe_keys.add(dedupe_key)

            game_key = self._build_cloud_save_game_key(game_id, game_title)
            tokens = self._build_cloud_save_match_tokens(
                game_id=game_id,
                game_title=game_title,
                install_path=install_path,
            )

            shortcut_result: Dict[str, Any] = {}
            compat_user_dir = ""
            if game_id:
                try:
                    shortcut_result = await asyncio.to_thread(resolve_tianyi_shortcut_sync, game_id=game_id)
                except Exception as exc:
                    shortcut_result = {"ok": False, "message": str(exc)}
                compat_user_dir = self._normalize_existing_dir(str(shortcut_result.get("compat_user_dir", "") or ""))
            else:
                shortcut_result = {"ok": False, "message": "game_id_missing"}

            source_paths: List[str] = []
            source_strategy = ""
            skip_reason = ""
            diagnostics: Dict[str, Any] = {
                "tokens": list(tokens),
                "install_path": install_path,
                "shortcut": {
                    "ok": bool(shortcut_result.get("ok")),
                    "message": str(shortcut_result.get("message", "") or ""),
                    "appid_unsigned": _safe_int(shortcut_result.get("appid_unsigned"), 0),
                    "compat_user_dir": str(shortcut_result.get("compat_user_dir", "") or ""),
                },
            }

            if compat_user_dir:
                source_paths, proton_diag = self._collect_cloud_save_paths_from_proton(
                    compat_user_dir=compat_user_dir,
                    tokens=tokens,
                )
                diagnostics["proton_scan"] = proton_diag
                source_strategy = "proton_prefix"
                if not source_paths:
                    skip_reason = "save_path_not_found"
            else:
                source_paths, fallback_diag = self._collect_cloud_save_paths_from_install(
                    install_path=install_path,
                    tokens=tokens,
                )
                diagnostics["fallback_scan"] = fallback_diag
                source_strategy = "install_fallback"
                if not source_paths:
                    skip_reason = "prefix_unresolved"

            playtime = self._snapshot_record_playtime(record)

            candidates.append(
                {
                    "game_id": game_id,
                    "game_title": game_title,
                    "game_key": game_key,
                    "install_path": install_path,
                    "source_paths": list(source_paths),
                    "source_strategy": source_strategy,
                    "skip_reason": skip_reason,
                    "playtime": {
                        "seconds": max(0, _safe_int(playtime.get("seconds"), 0)),
                        "sessions": max(0, _safe_int(playtime.get("sessions"), 0)),
                        "last_played_at": max(0, _safe_int(playtime.get("last_played_at"), 0)),
                        "captured_at": _now_wall_ts(),
                    },
                    "diagnostics": diagnostics,
                }
            )

        diagnostics = {
            "installed_total": len(records),
            "candidate_total": len(candidates),
            "ignored_total": len(ignored),
            "ignored": ignored,
        }
        return candidates, diagnostics

    async def _download_and_extract_cloud_restore_version(
        self,
        *,
        cookie: str,
        game_key: str,
        version_name: str,
    ) -> Dict[str, Any]:
        """下载并解压指定云存档版本，返回清单与临时路径。"""
        normalized_key = str(game_key or "").strip()
        normalized_version = str(version_name or "").strip()
        if not normalized_key:
            raise TianyiApiError("缺少 game_key")
        if not normalized_version or not normalized_version.lower().endswith(".7z"):
            raise TianyiApiError("版本文件无效，仅支持 .7z")

        listed = await list_cloud_archives(cookie=cookie, remote_folder_parts=[normalized_key])
        files = listed.get("files")
        if not isinstance(files, list):
            files = []

        target_file: Dict[str, Any] = {}
        for item in files:
            if not isinstance(item, dict):
                continue
            name = str(item.get("name", "") or "").strip()
            if name == normalized_version:
                target_file = dict(item)
                break
        if not target_file:
            raise TianyiApiError("未找到指定版本，请刷新后重试")

        file_id = str(target_file.get("file_id", "") or "").strip()
        if not file_id:
            raise TianyiApiError("云端版本缺少 file_id")

        temp_dir = tempfile.mkdtemp(prefix=f"freedeck_cloudrestore_{normalized_key}_")
        archive_path = os.path.join(temp_dir, normalized_version)
        extract_dir = os.path.join(temp_dir, "extracted")
        manifest_path = os.path.join(extract_dir, "manifest.json")
        os.makedirs(extract_dir, exist_ok=True)
        try:
            await download_cloud_archive(
                cookie=cookie,
                file_id=file_id,
                local_file_path=archive_path,
            )
            await asyncio.to_thread(self.seven_zip.extract_archive, archive_path, extract_dir)

            manifest: Dict[str, Any] = {}
            if os.path.isfile(manifest_path):
                try:
                    with open(manifest_path, "r", encoding="utf-8") as fp:
                        loaded = json.load(fp)
                    if isinstance(loaded, dict):
                        manifest = loaded
                except Exception:
                    manifest = {}

            entries = self._normalize_cloud_restore_entries(manifest=manifest, extract_dir=extract_dir)
            return {
                "temp_dir": temp_dir,
                "archive_path": archive_path,
                "extract_dir": extract_dir,
                "manifest_path": manifest_path,
                "manifest": manifest,
                "entries": entries,
                "version_name": normalized_version,
                "game_key": normalized_key,
                "file_id": file_id,
                "cloud_list": listed,
            }
        except Exception:
            self._cleanup_cloud_save_temp_paths([temp_dir])
            raise

    def _resolve_cloud_restore_target_candidates(
        self,
        *,
        game_id: str,
        game_key: str,
        game_title: str,
        entries: Sequence[Dict[str, Any]] = (),
    ) -> Dict[str, Any]:
        """为恢复流程解析当前可用目标目录候选。"""
        records = list(self.store.installed_games or [])
        matched_records: List[TianyiInstalledGame] = []
        for record in records:
            record_game_id = str(record.game_id or "").strip()
            record_title = str(record.game_title or "").strip() or "未命名游戏"
            record_key = self._build_cloud_save_game_key(record_game_id, record_title)
            if game_id and record_game_id and game_id == record_game_id:
                matched_records.append(record)
                continue
            if game_key and record_key and game_key == record_key:
                matched_records.append(record)
                continue

        matched_records.sort(key=lambda item: int(item.updated_at or 0), reverse=True)

        candidates: List[str] = []
        diagnostics: Dict[str, Any] = {
            "matched_records": [],
            "proton_scans": [],
            "fallback_scans": [],
        }

        for record in matched_records:
            install_path = self._normalize_existing_dir(str(record.install_path or "").strip())
            if not install_path:
                continue

            record_game_id = str(record.game_id or "").strip() or str(game_id or "").strip()
            record_title = str(record.game_title or "").strip() or str(game_title or "").strip() or "未命名游戏"
            tokens = self._build_cloud_save_match_tokens(
                game_id=record_game_id,
                game_title=record_title,
                install_path=install_path,
            )

            shortcut_result: Dict[str, Any] = {}
            compat_user_dir = ""
            if record_game_id:
                try:
                    shortcut_result = resolve_tianyi_shortcut_sync(game_id=record_game_id)
                except Exception as exc:
                    shortcut_result = {"ok": False, "message": str(exc)}
                compat_user_dir = self._normalize_existing_dir(str(shortcut_result.get("compat_user_dir", "") or ""))

            diagnostics["matched_records"].append(
                {
                    "game_id": record_game_id,
                    "game_title": record_title,
                    "install_path": install_path,
                    "shortcut": {
                        "ok": bool(shortcut_result.get("ok")),
                        "message": str(shortcut_result.get("message", "") or ""),
                        "appid_unsigned": _safe_int(shortcut_result.get("appid_unsigned"), 0),
                        "compat_user_dir": str(shortcut_result.get("compat_user_dir", "") or ""),
                    },
                }
            )

            if compat_user_dir:
                paths, proton_diag = self._collect_cloud_save_paths_from_proton(
                    compat_user_dir=compat_user_dir,
                    tokens=tokens,
                )
                diagnostics["proton_scans"].append(proton_diag)
                candidates.extend(paths)

                inferred_candidates: List[str] = []
                for entry in list(entries or []):
                    if not isinstance(entry, dict):
                        continue
                    source_path = str(entry.get("source_path", "") or "").strip()
                    archive_rel_path = str(entry.get("archive_rel_path", "") or "").strip().replace("\\", "/").strip("/")
                    relative = self._extract_proton_relative_path(
                        source_path=source_path,
                        archive_rel_path=archive_rel_path,
                    )
                    if not relative:
                        continue
                    inferred_candidates.append(
                        os.path.join(compat_user_dir, *[part for part in relative.split("/") if part]),
                    )

                if inferred_candidates:
                    candidates.extend(inferred_candidates)
                    diagnostics.setdefault("inferred_candidates", [])
                    diagnostics["inferred_candidates"].extend(
                        self._dedupe_paths(inferred_candidates, require_existing=False),
                    )
            else:
                paths, fallback_diag = self._collect_cloud_save_paths_from_install(
                    install_path=install_path,
                    tokens=tokens,
                )
                diagnostics["fallback_scans"].append(fallback_diag)
                candidates.extend(paths)

        merged = self._dedupe_paths(candidates, require_existing=False)
        diagnostics["candidate_count"] = len(merged)
        diagnostics["candidates"] = list(merged)
        return {
            "candidates": merged,
            "diagnostics": diagnostics,
        }

    def _build_restore_copy_plan(
        self,
        *,
        extract_dir: str,
        entries: Sequence[Dict[str, Any]],
        selected_entry_ids: Sequence[str],
        target_dir: str,
        entry_target_dirs: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """将所选存档项映射为复制计划。"""
        extract_root = self._normalize_existing_dir(extract_dir)
        target_root = self._normalize_dir_path(target_dir)
        if not extract_root:
            raise ValueError("解压目录不存在")
        has_entry_targets = isinstance(entry_target_dirs, dict) and bool(entry_target_dirs)
        if not target_root and not has_entry_targets:
            raise ValueError("目标目录无效")

        selected = {str(item or "").strip() for item in list(selected_entry_ids or []) if str(item or "").strip()}
        if not selected:
            raise ValueError("未选择任何存档项")

        normalized_entries = [dict(item) for item in list(entries or []) if isinstance(item, dict)]
        selected_entries = [item for item in normalized_entries if str(item.get("entry_id", "") or "") in selected]
        if not selected_entries:
            raise ValueError("未匹配到所选存档项")

        copy_pairs: List[Tuple[str, str]] = []
        plan_items: List[Dict[str, Any]] = []
        for entry in selected_entries:
            entry_id = str(entry.get("entry_id", "") or "").strip()
            entry_name = str(entry.get("entry_name", "") or entry_id).strip() or entry_id
            rel_path = str(entry.get("archive_rel_path", "") or "").strip().replace("\\", "/").strip("/")
            entry_root = os.path.realpath(os.path.join(extract_root, rel_path)) if rel_path else extract_root
            if not entry_root.startswith(extract_root):
                continue
            if not os.path.exists(entry_root):
                continue

            preferred_target = ""
            if isinstance(entry_target_dirs, dict):
                preferred_target = str(entry_target_dirs.get(entry_id, "") or "").strip()
            entry_target_root = self._normalize_dir_path(preferred_target) or target_root
            if not entry_target_root:
                continue

            pairs_for_entry: List[Tuple[str, str]] = []
            if os.path.isfile(entry_root):
                if os.path.basename(entry_root) == "manifest.json":
                    continue
                dest = os.path.join(entry_target_root, os.path.basename(entry_root))
                pairs_for_entry.append((entry_root, dest))
            else:
                for dirpath, _dirnames, filenames in os.walk(entry_root):
                    for filename in filenames:
                        if filename == "manifest.json":
                            continue
                        src_file = os.path.join(dirpath, filename)
                        rel_file = os.path.relpath(src_file, entry_root)
                        dst_file = os.path.realpath(os.path.join(entry_target_root, rel_file))
                        if not dst_file.startswith(entry_target_root):
                            continue
                        pairs_for_entry.append((src_file, dst_file))

            if not pairs_for_entry:
                continue
            copy_pairs.extend(pairs_for_entry)
            plan_items.append(
                {
                    "entry_id": entry_id,
                    "entry_name": entry_name,
                    "file_count": len(pairs_for_entry),
                    "target_dir": entry_target_root,
                }
            )

        if not copy_pairs:
            raise ValueError("选中的存档项没有可恢复文件")

        conflict_paths: List[str] = []
        for _src, dst in copy_pairs:
            if os.path.exists(dst):
                conflict_paths.append(dst)
        dedup_conflicts = sorted(set(conflict_paths))

        return {
            "copy_pairs": copy_pairs,
            "plan_items": plan_items,
            "conflict_count": len(dedup_conflicts),
            "conflict_samples": dedup_conflicts[:CLOUD_SAVE_RESTORE_CONFLICT_SAMPLES],
        }

    def _compute_common_working_dir(self, paths: Sequence[str]) -> str:
        """计算 7z 工作目录。"""
        normalized = [
            os.path.realpath(os.path.expanduser(str(item or "").strip()))
            for item in list(paths or [])
            if str(item or "").strip()
        ]
        if not normalized:
            raise ValueError("缺少待打包路径")

        try:
            common = os.path.commonpath(normalized)
        except Exception:
            common = ""
        if common and os.path.isdir(common):
            return common

        first = normalized[0]
        if os.path.isdir(first):
            return first
        parent = os.path.dirname(first)
        if parent and os.path.isdir(parent):
            return parent
        raise ValueError("无法确定打包工作目录")

    async def _archive_single_game_saves(
        self,
        *,
        candidate: Dict[str, Any],
        timestamp: str,
    ) -> Tuple[str, Dict[str, Any]]:
        """打包单游戏存档并写入 manifest。"""
        game_key = str(candidate.get("game_key", "") or "game")
        game_id = str(candidate.get("game_id", "") or "")
        game_title = str(candidate.get("game_title", "") or "")
        source_paths = [str(path or "") for path in list(candidate.get("source_paths") or []) if str(path or "").strip()]
        playtime_raw = candidate.get("playtime")
        playtime: Dict[str, Any] = playtime_raw if isinstance(playtime_raw, dict) else {}
        if not source_paths:
            raise SevenZipError("缺少可打包的存档目录")

        temp_dir = tempfile.mkdtemp(prefix=f"freedeck_cloudsave_{game_key}_")
        archive_name = f"{game_key}_{timestamp}.7z"
        archive_path = os.path.join(temp_dir, archive_name)
        manifest_path = os.path.join(temp_dir, "manifest.json")
        try:
            working_dir = self._compute_common_working_dir(source_paths)

            await asyncio.to_thread(
                self.seven_zip.create_archive,
                archive_path,
                source_paths,
                working_dir,
            )

            entry_items: List[Dict[str, Any]] = []
            for idx, source_path in enumerate(source_paths, start=1):
                normalized_source = os.path.realpath(os.path.expanduser(str(source_path or "").strip()))
                rel_path = ""
                try:
                    rel_path = os.path.relpath(normalized_source, working_dir).replace("\\", "/")
                except Exception:
                    rel_path = os.path.basename(normalized_source.rstrip("/\\"))
                rel_path = str(rel_path or "").replace("\\", "/").strip("./")
                if not rel_path:
                    rel_path = os.path.basename(normalized_source.rstrip("/\\"))
                entry_name = os.path.basename(normalized_source.rstrip("/\\")).strip() or f"entry_{idx}"
                entry_items.append(
                    {
                        "entry_id": f"entry_{idx}",
                        "name": entry_name,
                        "source_path": normalized_source,
                        "archive_rel_path": rel_path,
                    }
                )

            manifest = {
                "game_id": game_id,
                "game_title": game_title,
                "game_key": game_key,
                "manifest_version": 2,
                "generated_at": _now_wall_ts(),
                "source_paths": list(source_paths),
                "source_strategy": str(candidate.get("source_strategy", "") or ""),
                "install_path": str(candidate.get("install_path", "") or ""),
                "working_dir": working_dir,
                "entries": entry_items,
                "playtime": {
                    "seconds": max(0, _safe_int(playtime.get("seconds"), 0)),
                    "sessions": max(0, _safe_int(playtime.get("sessions"), 0)),
                    "last_played_at": max(0, _safe_int(playtime.get("last_played_at"), 0)),
                    "captured_at": max(0, _safe_int(playtime.get("captured_at"), 0)) or _now_wall_ts(),
                },
            }
            with open(manifest_path, "w", encoding="utf-8") as fp:
                json.dump(manifest, fp, ensure_ascii=False, indent=2)

            await asyncio.to_thread(
                self.seven_zip.create_archive,
                archive_path,
                [manifest_path],
                temp_dir,
            )

            archive_size = 0
            try:
                archive_size = max(0, int(os.path.getsize(archive_path)))
            except Exception:
                archive_size = 0

            return archive_path, {
                "temp_dir": temp_dir,
                "manifest_path": manifest_path,
                "archive_name": archive_name,
                "archive_size_bytes": archive_size,
            }
        except Exception:
            self._cleanup_cloud_save_temp_paths([temp_dir])
            raise

    def _cleanup_cloud_save_temp_paths(self, temp_paths: Sequence[str]) -> None:
        """清理临时文件与目录。"""
        for raw in list(temp_paths or []):
            path = os.path.realpath(os.path.expanduser(str(raw or "").strip()))
            if not path:
                continue
            try:
                if os.path.isdir(path):
                    shutil.rmtree(path, ignore_errors=True)
                elif os.path.exists(path):
                    os.remove(path)
            except Exception:
                pass

    async def _run_cloud_save_upload_task(self, *, cookie: str, user_account: str) -> None:
        """执行云存档上传任务。"""
        started_at = _now_wall_ts()
        timestamp = time.strftime(CLOUD_SAVE_DATE_FORMAT, time.localtime(started_at))
        candidates: List[Dict[str, Any]] = []
        collect_diagnostics: Dict[str, Any] = {}
        results: List[Dict[str, Any]] = []
        uploaded = 0
        skipped = 0
        failed = 0
        final_stage = "completed"
        final_reason = ""
        final_message = "云存档上传完成"
        exception_text = ""
        cancelled = False

        try:
            candidates, collect_diagnostics = await self._collect_cloud_save_candidates()
            total_games = len(candidates)

            await self._set_cloud_save_state(
                stage="scanning",
                running=True,
                message=f"发现 {total_games} 个候选游戏",
                reason="",
                current_game="",
                total_games=total_games,
                processed_games=0,
                uploaded=0,
                skipped=0,
                failed=0,
                progress=0.0 if total_games > 0 else 100.0,
                results=[],
                diagnostics={
                    "started_at": started_at,
                    "timestamp": timestamp,
                    "user_account": user_account,
                    "collect": collect_diagnostics,
                },
            )

            if total_games <= 0:
                final_message = "未找到可上传的已安装游戏"

            for index, candidate in enumerate(candidates, start=1):
                game_title = str(candidate.get("game_title", "") or "未命名游戏")
                game_key = str(candidate.get("game_key", "") or "")
                source_paths = [str(path or "") for path in list(candidate.get("source_paths") or []) if str(path or "").strip()]
                skip_reason = str(candidate.get("skip_reason", "") or "").strip()
                entry: Dict[str, Any] = {
                    "game_id": str(candidate.get("game_id", "") or ""),
                    "game_title": game_title,
                    "game_key": game_key,
                    "status": "",
                    "reason": "",
                    "cloud_path": "",
                    "source_paths": source_paths,
                    "diagnostics": dict(candidate.get("diagnostics") or {}),
                }

                await self._set_cloud_save_state(
                    stage="packaging",
                    running=True,
                    message=f"正在处理 {index}/{total_games}：{game_title}",
                    current_game=game_title,
                )

                if skip_reason:
                    entry["status"] = "skipped"
                    entry["reason"] = skip_reason
                    skipped += 1
                else:
                    cleanup_paths: List[str] = []
                    try:
                        archive_path, archive_meta = await self._archive_single_game_saves(
                            candidate=candidate,
                            timestamp=timestamp,
                        )
                        cleanup_paths.append(str(archive_meta.get("temp_dir", "") or ""))
                        archive_name = str(archive_meta.get("archive_name", "") or "")
                        archive_size = _safe_int(archive_meta.get("archive_size_bytes"), 0)

                        await self._set_cloud_save_state(
                            stage="uploading",
                            running=True,
                            message=f"正在上传 {index}/{total_games}：{game_title}",
                            current_game=game_title,
                        )

                        remote_name = f"{timestamp}.7z"
                        upload_result = await upload_archive_to_cloud(
                            cookie=cookie,
                            local_file_path=archive_path,
                            remote_folder_parts=[game_key],
                            remote_name=remote_name,
                        )

                        cloud_path = str(upload_result.get("cloud_path", "") or "").strip()
                        if not cloud_path:
                            cloud_path = f"/{CLOUD_SAVE_UPLOAD_ROOT}/{game_key}/{remote_name}"

                        entry["status"] = "uploaded"
                        entry["reason"] = ""
                        entry["cloud_path"] = cloud_path
                        entry["diagnostics"] = {
                            **dict(entry.get("diagnostics") or {}),
                            "archive_name": archive_name,
                            "archive_size_bytes": archive_size,
                            "upload_result": dict(upload_result),
                        }
                        uploaded += 1
                    except asyncio.CancelledError:
                        cancelled = True
                        raise
                    except SevenZipError as exc:
                        entry["status"] = "failed"
                        entry["reason"] = "package_failed"
                        entry["diagnostics"] = {
                            **dict(entry.get("diagnostics") or {}),
                            "exception": str(exc),
                        }
                        failed += 1
                    except TianyiApiError as exc:
                        entry["status"] = "failed"
                        entry["reason"] = "upload_failed"
                        entry["diagnostics"] = {
                            **dict(entry.get("diagnostics") or {}),
                            "exception": str(exc),
                            "api_diagnostics": dict(getattr(exc, "diagnostics", {}) or {}),
                        }
                        failed += 1
                    except Exception as exc:
                        entry["status"] = "failed"
                        entry["reason"] = "unexpected_error"
                        entry["diagnostics"] = {
                            **dict(entry.get("diagnostics") or {}),
                            "exception": str(exc),
                        }
                        failed += 1
                    finally:
                        self._cleanup_cloud_save_temp_paths(cleanup_paths)

                results.append(self._copy_cloud_save_result(entry))
                processed = len(results)
                progress = 100.0 if total_games <= 0 else (float(processed) / float(total_games)) * 100.0
                await self._set_cloud_save_state(
                    stage="uploading" if processed < total_games else "scanning",
                    running=True,
                    message=f"已处理 {processed}/{total_games}",
                    current_game=game_title,
                    processed_games=processed,
                    uploaded=uploaded,
                    skipped=skipped,
                    failed=failed,
                    progress=progress,
                    results=results,
                )

                if cancelled:
                    break
        except asyncio.CancelledError:
            cancelled = True
            final_stage = "failed"
            final_reason = "task_cancelled"
            final_message = "云存档上传任务已取消"
        except Exception as exc:
            final_stage = "failed"
            final_reason = "task_exception"
            final_message = f"云存档上传任务异常：{exc}"
            exception_text = str(exc)
            config.logger.exception("Cloud save upload task failed: %s", exc)
        else:
            if failed > 0:
                final_stage = "failed"
                final_reason = "partial_failed"
                final_message = f"云存档上传完成（成功 {uploaded}，失败 {failed}，跳过 {skipped}）"
            else:
                final_stage = "completed"
                final_reason = ""
                final_message = f"云存档上传完成（成功 {uploaded}，跳过 {skipped}）"
        finally:
            finished_at = _now_wall_ts()
            total_games = len(candidates)
            final_payload = {
                "stage": final_stage,
                "reason": final_reason,
                "message": final_message,
                "started_at": started_at,
                "finished_at": finished_at,
                "timestamp": timestamp,
                "total_games": total_games,
                "processed_games": len(results),
                "uploaded": uploaded,
                "skipped": skipped,
                "failed": failed,
                "results": results,
                "diagnostics": {
                    "collect": collect_diagnostics,
                    "exception": exception_text,
                    "cancelled": cancelled,
                },
            }

            try:
                await asyncio.to_thread(self.store.set_cloud_save_last_result, final_payload)
            except Exception as exc:
                config.logger.warning("Persist cloud save upload result failed: %s", exc)

            await self._set_cloud_save_state(
                stage=final_stage,
                running=False,
                message=final_message,
                reason=final_reason,
                current_game="",
                total_games=total_games,
                processed_games=len(results),
                uploaded=uploaded,
                skipped=skipped,
                failed=failed,
                progress=100.0 if total_games <= 0 else (float(len(results)) / float(total_games)) * 100.0,
                results=results,
                diagnostics={
                    "collect": collect_diagnostics,
                    "exception": exception_text,
                    "cancelled": cancelled,
                },
                last_result=final_payload,
            )

            async with self._cloud_save_lock:
                current = asyncio.current_task()
                if self._cloud_save_task is current:
                    self._cloud_save_task = None

            if cancelled:
                raise asyncio.CancelledError()

    async def uninstall_installed_game(
        self,
        *,
        game_id: str = "",
        install_path: str = "",
        delete_files: bool = True,
    ) -> Dict[str, Any]:
        """卸载已安装游戏（删除文件并移除记录）。"""
        target_game_id = str(game_id or "").strip()
        target_install_path = str(install_path or "").strip()
        if not target_game_id and not target_install_path:
            raise ValueError("缺少卸载目标")

        record = self._find_installed_record(game_id=target_game_id, install_path=target_install_path)
        if record is None:
            raise ValueError("未找到已安装游戏记录")

        resolved_path = os.path.realpath(os.path.expanduser(str(record.install_path or "").strip()))
        removed_files = False
        if bool(delete_files):
            allow, reason = self._can_remove_install_path(resolved_path)
            if not allow:
                raise ValueError(reason or "卸载路径不安全，已拒绝删除")

            if os.path.lexists(resolved_path):
                try:
                    if os.path.islink(resolved_path) or os.path.isfile(resolved_path):
                        os.remove(resolved_path)
                    elif os.path.isdir(resolved_path):
                        shutil.rmtree(resolved_path, ignore_errors=False)
                    else:
                        os.remove(resolved_path)
                    removed_files = True
                except Exception as exc:
                    raise RuntimeError(f"删除安装文件失败: {exc}") from exc

        removed = await asyncio.to_thread(
            self.store.remove_installed_game,
            game_id=record.game_id,
            install_path=record.install_path,
        )
        persist_warning = ""
        if removed is None:
            # 某些异常场景下 remove 可能返回空，回退为内存移除避免前端直接报错。
            removed = self._remove_installed_record_in_memory(
                game_id=record.game_id,
                install_path=record.install_path,
            )
            if removed is not None:
                try:
                    await asyncio.to_thread(self.store.save)
                except Exception as exc:
                    persist_warning = str(exc)
                    config.logger.warning("Installed record fallback save failed: %s", exc)
            else:
                raise RuntimeError("卸载记录写入失败，请重试")

        steam_cleanup: Dict[str, Any] = {}
        steam_warning = ""
        record_game_id = str(record.game_id or "").strip()
        if record_game_id:
            try:
                steam_cleanup = await remove_tianyi_shortcut(game_id=record_game_id)
            except Exception as exc:
                steam_cleanup = {"ok": False, "removed": False, "message": str(exc)}

            if not bool(steam_cleanup.get("ok")):
                steam_warning = f"Steam 快捷方式清理失败：{str(steam_cleanup.get('message', '') or '未知错误')}"
            elif not bool(steam_cleanup.get("cleanup_ok", True)):
                steam_warning = "Steam 快捷方式已删除，但 Proton 映射或封面清理失败"
        else:
            steam_cleanup = {"ok": False, "removed": False, "message": "缺少 game_id，跳过 Steam 快捷方式清理"}
            steam_warning = "Steam 快捷方式清理已跳过：缺少 game_id"

        removed_key = self._installed_record_session_key(record)
        if removed_key:
            async with self._playtime_lock:
                self._playtime_sessions.pop(removed_key, None)

        self._invalidate_panel_cache(installed=True)
        summary = self._build_installed_summary(limit=60, persist=False)
        self._panel_installed_cache = {
            "total": int(summary.get("total", 0) or 0),
            "preview": list(summary.get("preview") or []),
        }
        self._panel_installed_cache_at = time.monotonic()
        response: Dict[str, Any] = {
            "removed": True,
            "game_id": str(record.game_id or ""),
            "title": str(record.game_title or ""),
            "install_path": str(record.install_path or ""),
            "files_deleted": bool(removed_files),
            "installed": summary,
            "steam": steam_cleanup,
        }
        warnings: List[str] = []
        if persist_warning:
            warnings.append(f"卸载记录回退保存失败：{persist_warning}")
        if steam_warning:
            warnings.append(steam_warning)
        if warnings:
            response["warning"] = "；".join(warnings)
        return response

    async def update_settings(
        self,
        *,
        download_dir: Optional[str] = None,
        install_dir: Optional[str] = None,
        split_count: Optional[int] = None,
        page_size: Optional[int] = None,
        auto_delete_package: Optional[bool] = None,
        auto_install: Optional[bool] = None,
    ) -> Dict[str, Any]:
        """更新下载设置。"""
        if download_dir is not None:
            path = os.path.realpath(os.path.expanduser(str(download_dir).strip()))
            if not path:
                raise ValueError("下载目录无效")
            os.makedirs(path, exist_ok=True)
            download_dir = path
            # 同步插件原有下载目录，避免路径分裂。
            self.plugin.downloads_dir = path
            await self.plugin.set_download_dir(path)
        if install_dir is not None:
            path = os.path.realpath(os.path.expanduser(str(install_dir).strip()))
            if not path:
                raise ValueError("安装目录无效")
            os.makedirs(path, exist_ok=True)
            install_dir = path

        self.store.set_settings(
            download_dir=download_dir,
            install_dir=install_dir,
            split_count=split_count,
            page_size=page_size,
            auto_delete_package=auto_delete_package,
            auto_install=True,
        )
        self._invalidate_panel_cache(all_data=True)
        return asdict(self.store.settings)

    async def prepare_install(
        self,
        *,
        game_id: str = "",
        share_url: str = "",
        file_ids: Optional[Sequence[str]] = None,
        download_dir: Optional[str] = None,
        install_dir: Optional[str] = None,
    ) -> Dict[str, Any]:
        """生成安装前确认数据（不创建任务）。"""
        return await self._build_install_plan(
            game_id=game_id,
            share_url=share_url,
            file_ids=file_ids,
            download_dir=download_dir,
            install_dir=install_dir,
        )

    async def start_install(
        self,
        *,
        game_id: str = "",
        share_url: str = "",
        file_ids: Optional[Sequence[str]] = None,
        split_count: Optional[int] = None,
        download_dir: Optional[str] = None,
        install_dir: Optional[str] = None,
    ) -> Dict[str, Any]:
        """确认后创建下载任务并进入安装链路。"""
        async with self._lock:
            plan = await self._build_install_plan(
                game_id=game_id,
                share_url=share_url,
                file_ids=file_ids,
                download_dir=download_dir,
                install_dir=install_dir,
            )
            if not bool(plan.get("can_install")):
                raise TianyiApiError("空间不足，无法开始安装")

            settings = self.store.settings
            split = int(split_count or settings.split_count or 16)
            split = max(1, min(64, split))

            created = await self._create_tasks_from_plan(plan=plan, split=split)
            self.store.upsert_tasks(created)
            self._invalidate_panel_cache(tasks=True)
            await self.refresh_tasks(sync_aria2=True)

            created_ids = {task.task_id for task in created}
            created_view = [_task_to_view(task) for task in self.store.tasks if task.task_id in created_ids]
            return {
                "plan": plan,
                "tasks": created_view,
            }

    async def create_tasks_for_game(
        self,
        *,
        game_id: str = "",
        share_url: str = "",
        file_ids: Optional[Sequence[str]] = None,
        split_count: Optional[int] = None,
        download_dir: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """兼容旧接口：直接创建下载任务。"""
        result = await self.start_install(
            game_id=game_id,
            share_url=share_url,
            file_ids=file_ids,
            split_count=split_count,
            download_dir=download_dir,
            install_dir=None,
        )
        tasks = result.get("tasks")
        if isinstance(tasks, list):
            return [item for item in tasks if isinstance(item, dict)]
        return []

    async def _build_install_plan(
        self,
        *,
        game_id: str = "",
        share_url: str = "",
        file_ids: Optional[Sequence[str]] = None,
        download_dir: Optional[str] = None,
        install_dir: Optional[str] = None,
    ) -> Dict[str, Any]:
        """构建安装计划与空间探针信息。"""
        login_ok, _, message = await self.check_login_state()
        if not login_ok:
            raise TianyiApiError(message or "请先登录天翼账号")

        open_path = ""
        if not share_url:
            item = self.catalog.get_by_game_id(game_id)
            if not item:
                raise ValueError("未找到对应游戏条目")
            share_url = item.down_url
            game_title = item.title
            game_id = item.game_id
            open_path = str(item.openpath or "")
        else:
            normalized_share_url = str(share_url or "").strip()
            matched_item = None
            for entry in self.catalog.entries:
                if game_id and str(entry.game_id) != str(game_id):
                    continue
                if str(entry.down_url or "").strip() == normalized_share_url:
                    matched_item = entry
                    break
            if matched_item is None:
                for entry in self.catalog.entries:
                    if str(entry.down_url or "").strip() == normalized_share_url:
                        matched_item = entry
                        break

            if matched_item is not None:
                game_title = str(matched_item.title or game_id or "自定义分享")
                game_id = str(matched_item.game_id or game_id or normalized_share_url)
                open_path = str(matched_item.openpath or "")
                if "pwd=" not in normalized_share_url and str(matched_item.pwd or "").strip():
                    joiner = "&" if "?" in normalized_share_url else "?"
                    normalized_share_url = f"{normalized_share_url}{joiner}pwd={matched_item.pwd.strip()}"
                share_url = normalized_share_url
            else:
                game_title = game_id or "自定义分享"
                game_id = game_id or normalized_share_url

        settings = self.store.settings
        target_download = (download_dir or settings.download_dir or self.plugin.downloads_dir).strip()
        if not target_download:
            raise ValueError("下载目录为空")
        target_download = os.path.realpath(os.path.expanduser(target_download))
        os.makedirs(target_download, exist_ok=True)

        target_install = (install_dir or settings.install_dir or target_download).strip()
        if not target_install:
            raise ValueError("安装目录为空")
        target_install = os.path.realpath(os.path.expanduser(target_install))
        os.makedirs(target_install, exist_ok=True)

        try:
            resolved = await resolve_share(share_url, self.store.login.cookie)
        except TianyiApiError as exc:
            diagnostics = dict(getattr(exc, "diagnostics", {}) or {})
            diagnostics.setdefault("stage", "resolve_share")
            raise TianyiApiError(str(exc), diagnostics=diagnostics) from exc
        selected = {str(v).strip() for v in (file_ids or []) if str(v).strip()}
        files = [
            file_item
            for file_item in resolved.files
            if not file_item.is_folder and (not selected or file_item.file_id in selected)
        ]
        if not files:
            raise TianyiApiError("未找到可下载文件，可能所选条目是目录")

        required_download_bytes = sum(max(0, int(file_item.size or 0)) for file_item in files)
        required_install_bytes = required_download_bytes
        free_download_bytes = _disk_free_bytes(target_download)
        free_install_bytes = _disk_free_bytes(target_install)
        download_dir_ok = free_download_bytes >= required_download_bytes
        install_dir_ok = free_install_bytes >= required_install_bytes
        can_install = bool(download_dir_ok and install_dir_ok)

        plan_files: List[Dict[str, Any]] = []
        for file_item in files:
            plan_files.append(
                {
                    "file_id": str(file_item.file_id or ""),
                    "name": str(file_item.name or ""),
                    "size": max(0, int(file_item.size or 0)),
                    "is_folder": bool(file_item.is_folder),
                }
            )

        return {
            "game_id": game_id,
            "game_title": game_title,
            "openpath": open_path,
            "share_url": share_url,
            "share_code": resolved.share_code,
            "share_id": resolved.share_id,
            "pwd": resolved.pwd,
            "download_dir": target_download,
            "install_dir": target_install,
            "required_download_bytes": required_download_bytes,
            "required_install_bytes": required_install_bytes,
            "required_download_human": _format_size_bytes(required_download_bytes),
            "required_install_human": _format_size_bytes(required_install_bytes),
            "free_download_bytes": free_download_bytes,
            "free_install_bytes": free_install_bytes,
            "free_download_human": _format_size_bytes(free_download_bytes),
            "free_install_human": _format_size_bytes(free_install_bytes),
            "download_dir_ok": download_dir_ok,
            "install_dir_ok": install_dir_ok,
            "can_install": can_install,
            "file_count": len(plan_files),
            "files": plan_files,
        }

    async def _create_tasks_from_plan(self, *, plan: Dict[str, Any], split: int) -> List[TianyiTaskRecord]:
        """根据安装计划创建 aria2 下载任务。"""
        await self.aria2.ensure_running()
        access_token = await fetch_access_token(self.store.login.cookie)

        share_id = str(plan.get("share_id", "")).strip()
        share_code = str(plan.get("share_code", "")).strip()
        game_id = str(plan.get("game_id", "")).strip()
        game_title = str(plan.get("game_title", "")).strip() or game_id or "未命名游戏"
        open_path = str(plan.get("openpath", "") or "")
        target_dir = str(plan.get("download_dir", "")).strip()

        created: List[TianyiTaskRecord] = []
        for file_item in plan.get("files", []):
            if not isinstance(file_item, dict):
                continue
            file_id = str(file_item.get("file_id", "")).strip()
            name = str(file_item.get("name", "")).strip() or f"file-{file_id}"
            if not file_id:
                continue

            direct_url = await fetch_download_url(
                self.store.login.cookie,
                access_token,
                share_id,
                file_id,
            )
            gid = await self.aria2.add_uri(
                direct_url=direct_url,
                cookie=self.store.login.cookie,
                download_dir=target_dir,
                out_name=name,
                split=split,
            )
            now = _now_wall_ts()
            created.append(
                TianyiTaskRecord(
                    task_id=str(uuid.uuid4()),
                    gid=gid,
                    game_id=game_id,
                    game_title=game_title,
                    share_code=share_code,
                    share_id=share_id,
                    file_id=file_id,
                    file_name=name,
                    download_dir=target_dir,
                    local_path=os.path.join(target_dir, name),
                    status="waiting",
                    progress=0.0,
                    speed=0,
                    openpath=open_path,
                    created_at=now,
                    updated_at=now,
                )
            )
        return created

    async def refresh_tasks(self, sync_aria2: bool = True, persist: bool = True) -> List[Dict[str, Any]]:
        """刷新任务列表并同步状态。"""
        tasks = list(self.store.tasks)
        if sync_aria2 and tasks:
            for task in tasks:
                if _is_terminal(task.status):
                    continue
                try:
                    info = await self.aria2.tell_status(task.gid)
                    status = str(info.get("status", task.status) or task.status)
                    total = _safe_int(info.get("totalLength"), 0)
                    completed = _safe_int(info.get("completedLength"), 0)
                    speed = _safe_int(info.get("downloadSpeed"), 0)
                    progress = 0.0
                    if total > 0:
                        progress = (completed * 100.0) / total
                    if status == "complete":
                        progress = 100.0
                    task.status = status
                    task.progress = round(progress, 2)
                    task.speed = speed
                    task.error_reason = str(info.get("errorMessage", "") or "")
                    task.updated_at = _now_wall_ts()
                    if status == "complete" and not task.post_processed:
                        self._schedule_post_process_task(task.task_id)
                except Aria2Error as exc:
                    if task.status in {"active", "waiting"}:
                        task.status = "error"
                        task.error_reason = str(exc)
                        task.updated_at = _now_wall_ts()

            self._cleanup_tasks(tasks)
            if persist:
                self.store.replace_tasks(tasks)
            else:
                self.store.tasks = list(tasks)
        else:
            self._cleanup_tasks(tasks)
            if tasks != self.store.tasks:
                if persist:
                    self.store.replace_tasks(tasks)
                else:
                    self.store.tasks = list(tasks)

        views = [_task_to_view(t) for t in tasks]
        self._panel_tasks_cache = list(views)
        self._panel_tasks_cache_at = time.monotonic()
        self._panel_last_active_tasks = self._count_active_tasks(views)
        return views

    def _schedule_post_process_task(self, task_id: str) -> None:
        """将下载后安装流程调度为后台任务，避免阻塞面板刷新。"""
        target = str(task_id or "").strip()
        if not target:
            return
        current = self._post_process_jobs.get(target)
        if current is not None and not current.done():
            return
        job = asyncio.create_task(
            self._run_post_process_task(target),
            name=f"freedeck_post_process_{target[:8]}",
        )
        self._post_process_jobs[target] = job

        def _cleanup(done_job: asyncio.Task, key: str = target) -> None:
            if self._post_process_jobs.get(key) is done_job:
                self._post_process_jobs.pop(key, None)

        job.add_done_callback(_cleanup)

    async def _run_post_process_task(self, task_id: str) -> None:
        """执行后台安装流程并在结束后持久化状态。"""
        task = self._find_task(task_id)
        if task is None or task.post_processed:
            return
        try:
            await self._post_process_completed_task(task)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            config.logger.exception("Post-process failed for task %s: %s", task_id, exc)
            task.post_processed = True
            task.install_status = "failed"
            task.install_message = f"安装流程异常: {exc}"
            task.updated_at = _now_wall_ts()
        finally:
            await asyncio.to_thread(self.store.save)

    async def pause_task(self, task_id: str) -> Dict[str, Any]:
        """暂停任务。"""
        task = self._find_task(task_id)
        if task is None:
            raise ValueError("任务不存在")
        await self.aria2.pause(task.gid)
        task.status = "paused"
        task.updated_at = _now_wall_ts()
        self.store.save()
        self._invalidate_panel_cache(tasks=True)
        return _task_to_view(task)

    async def resume_task(self, task_id: str) -> Dict[str, Any]:
        """恢复任务。"""
        task = self._find_task(task_id)
        if task is None:
            raise ValueError("任务不存在")
        await self.aria2.resume(task.gid)
        task.status = "active"
        task.updated_at = _now_wall_ts()
        self.store.save()
        self._invalidate_panel_cache(tasks=True)
        return _task_to_view(task)

    async def remove_task(self, task_id: str) -> Dict[str, Any]:
        """移除任务。"""
        task = self._find_task(task_id)
        if task is None:
            raise ValueError("任务不存在")
        await self.aria2.remove(task.gid)
        task.status = "removed"
        task.updated_at = _now_wall_ts()
        self._cleanup_tasks(self.store.tasks)
        self.store.save()
        self._invalidate_panel_cache(tasks=True)
        return _task_to_view(task)

    def _find_task(self, task_id: str) -> Optional[TianyiTaskRecord]:
        """查找任务对象。"""
        target = (task_id or "").strip()
        if not target:
            return None
        for task in self.store.tasks:
            if task.task_id == target:
                return task
        return None

    def _cleanup_tasks(self, tasks: List[TianyiTaskRecord]) -> None:
        """清理过旧终态任务。"""
        now = _now_wall_ts()
        filtered: List[TianyiTaskRecord] = []
        for task in tasks:
            if not _is_terminal(task.status):
                filtered.append(task)
                continue
            if now - int(task.updated_at) <= TASK_RETENTION_SECONDS:
                filtered.append(task)
        tasks[:] = filtered

    def _build_installed_summary(self, limit: int = 8, persist: bool = True) -> Dict[str, Any]:
        """构建已安装游戏预览。"""
        normalized_limit = max(0, int(limit or 0))
        visible_items: List[Dict[str, Any]] = []
        kept_records: List[TianyiInstalledGame] = []

        # 过滤已不存在的安装目录，避免主界面展示脏数据。
        records = sorted(self.store.installed_games, key=lambda item: int(item.updated_at or 0), reverse=True)
        for record in records:
            install_path = str(record.install_path or "").strip()
            if not install_path or not os.path.exists(install_path):
                continue
            kept_records.append(record)
            visible_items.append(self._installed_record_to_view(record))

        if len(kept_records) != len(self.store.installed_games):
            self.store.installed_games = kept_records
            if persist:
                self.store.save()

        return {
            "total": len(visible_items),
            "preview": visible_items[:normalized_limit],
        }

    def _find_installed_record(self, *, game_id: str = "", install_path: str = "") -> Optional[TianyiInstalledGame]:
        """查找已安装游戏记录。"""
        target_game_id = str(game_id or "").strip()
        target_install_path = os.path.realpath(os.path.expanduser(str(install_path or "").strip())) if install_path else ""
        for record in self.store.installed_games:
            same_game = bool(target_game_id and str(record.game_id or "") == target_game_id)
            current_install_path = os.path.realpath(os.path.expanduser(str(record.install_path or "").strip()))
            same_path = bool(target_install_path and current_install_path == target_install_path)
            if same_game or same_path:
                return record
        return None

    def _remove_installed_record_in_memory(
        self,
        *,
        game_id: str = "",
        install_path: str = "",
    ) -> Optional[TianyiInstalledGame]:
        """仅在内存中移除已安装记录，供持久化异常时回退。"""
        target_game_id = str(game_id or "").strip()
        target_install_path = os.path.realpath(os.path.expanduser(str(install_path or "").strip())) if install_path else ""
        for idx, record in enumerate(list(self.store.installed_games)):
            same_game = bool(target_game_id and str(record.game_id or "") == target_game_id)
            current_path = os.path.realpath(os.path.expanduser(str(record.install_path or "").strip()))
            same_path = bool(target_install_path and current_path == target_install_path)
            if same_game or same_path:
                return self.store.installed_games.pop(idx)
        return None

    def _can_remove_install_path(self, path: str) -> Tuple[bool, str]:
        """卸载前校验路径安全性，避免误删根目录。"""
        raw = str(path or "").strip()
        if not raw:
            return False, "安装路径为空"
        target = os.path.realpath(os.path.expanduser(raw))
        if not target:
            return False, "安装路径无效"

        normalized = target.rstrip(os.sep)
        if not normalized:
            return False, "安装路径无效"

        home_dir = os.path.realpath(os.path.expanduser("~")).rstrip(os.sep)
        install_root = os.path.realpath(os.path.expanduser(str(self.store.settings.install_dir or "").strip())).rstrip(os.sep)
        download_root = os.path.realpath(os.path.expanduser(str(self.store.settings.download_dir or "").strip())).rstrip(os.sep)

        blocked = {"/", "/home", "/home/deck", home_dir}
        if install_root:
            blocked.add(install_root)
        if download_root:
            blocked.add(download_root)

        if normalized in blocked:
            return False, "目标路径为系统或根目录级路径，已拒绝删除"

        parts = [part for part in normalized.split(os.sep) if part]
        if len(parts) < 2:
            return False, "目标路径层级过浅，已拒绝删除"

        return True, ""

    def _installed_record_to_view(self, record: TianyiInstalledGame) -> Dict[str, Any]:
        """转换已安装游戏展示结构。"""
        size_bytes = max(0, int(record.size_bytes or 0))
        playtime = self._snapshot_record_playtime(record)
        playtime_seconds = max(0, _safe_int(playtime.get("seconds"), 0))
        return {
            "game_id": record.game_id,
            "title": record.game_title,
            "install_path": record.install_path,
            "source_path": record.source_path,
            "status": record.status,
            "size_bytes": size_bytes,
            "size_text": _format_size_bytes(size_bytes) if size_bytes > 0 else "",
            "steam_app_id": max(0, _safe_int(record.steam_app_id, 0)),
            "playtime_seconds": playtime_seconds,
            "playtime_text": _format_playtime_seconds(playtime_seconds),
            "playtime_sessions": max(0, _safe_int(playtime.get("sessions"), 0)),
            "playtime_last_played_at": max(0, _safe_int(playtime.get("last_played_at"), 0)),
            "playtime_active": bool(playtime.get("active")),
            "updated_at": int(record.updated_at or 0),
        }

    async def _post_process_completed_task(self, task: TianyiTaskRecord) -> None:
        """下载完成后执行安装与清理。"""
        task.post_processed = True
        settings = self.store.settings

        local_path = str(task.local_path or "").strip()
        if not local_path:
            local_path = os.path.join(str(task.download_dir or "").strip(), str(task.file_name or "").strip())
        local_path = os.path.realpath(os.path.expanduser(local_path))

        if not os.path.isfile(local_path):
            task.install_status = "failed"
            task.install_message = "下载文件不存在，无法安装"
            task.updated_at = _now_wall_ts()
            return

        install_root = str(settings.install_dir or "").strip() or str(settings.download_dir or "").strip()
        if not install_root:
            install_root = str(getattr(self.plugin, "downloads_dir", config.DOWNLOADS_DIR) or config.DOWNLOADS_DIR)
        install_root = os.path.realpath(os.path.expanduser(install_root))
        os.makedirs(install_root, exist_ok=True)

        target_dir = self._resolve_install_target_dir(task, install_root)
        os.makedirs(target_dir, exist_ok=True)

        task.install_status = "installing"
        task.install_message = "正在安装..."
        task.updated_at = _now_wall_ts()

        is_archive = self._is_archive_file(local_path)
        if is_archive:
            ok, reason = await asyncio.to_thread(self._extract_archive_to_dir, local_path, target_dir)
            if not ok:
                task.install_status = "failed"
                task.install_message = reason
                task.updated_at = _now_wall_ts()
                return
        else:
            # 非压缩包按普通文件归档到安装目录。
            file_name = os.path.basename(local_path)
            dest_file = os.path.join(target_dir, file_name)
            try:
                if os.path.realpath(local_path) != os.path.realpath(dest_file):
                    shutil.copy2(local_path, dest_file)
            except Exception as exc:
                task.install_status = "failed"
                task.install_message = f"复制安装文件失败: {exc}"
                task.updated_at = _now_wall_ts()
                return

        source_size = 0
        try:
            source_size = max(0, int(os.path.getsize(local_path)))
        except Exception:
            source_size = 0

        existing_record = self._find_installed_record(
            game_id=str(task.game_id or "").strip(),
            install_path=target_dir,
        )
        existing_playtime_seconds = 0
        existing_playtime_sessions = 0
        existing_playtime_last_played_at = 0
        existing_playtime_active_started_at = 0
        existing_playtime_active_app_id = 0
        existing_steam_app_id = 0
        if existing_record is not None:
            existing_playtime_seconds = max(0, _safe_int(existing_record.playtime_seconds, 0))
            existing_playtime_sessions = max(0, _safe_int(existing_record.playtime_sessions, 0))
            existing_playtime_last_played_at = max(0, _safe_int(existing_record.playtime_last_played_at, 0))
            existing_playtime_active_started_at = max(0, _safe_int(existing_record.playtime_active_started_at, 0))
            existing_playtime_active_app_id = max(0, _safe_int(existing_record.playtime_active_app_id, 0))
            existing_steam_app_id = max(0, _safe_int(existing_record.steam_app_id, 0))

        task.install_status = "installed"
        task.installed_path = target_dir
        task.updated_at = _now_wall_ts()

        self.store.upsert_installed_game(
            TianyiInstalledGame(
                game_id=str(task.game_id or ""),
                game_title=str(task.game_title or task.file_name or "未命名游戏"),
                install_path=target_dir,
                source_path=local_path,
                status="installed",
                size_bytes=source_size,
                steam_app_id=existing_steam_app_id,
                playtime_seconds=existing_playtime_seconds,
                playtime_sessions=existing_playtime_sessions,
                playtime_last_played_at=existing_playtime_last_played_at,
                playtime_active_started_at=existing_playtime_active_started_at,
                playtime_active_app_id=existing_playtime_active_app_id,
            )
        )
        self._invalidate_panel_cache(tasks=True, installed=True)

        message_parts: List[str] = ["安装完成"]
        steam_result = await self._auto_register_task_to_steam(task=task, target_dir=target_dir)
        if steam_result.get("ok"):
            app_id = _safe_int(steam_result.get("appid_unsigned"), 0)
            if app_id > 0:
                installed_record = self._find_installed_record(
                    game_id=str(task.game_id or "").strip(),
                    install_path=target_dir,
                )
                if installed_record is not None and max(0, _safe_int(installed_record.steam_app_id, 0)) != app_id:
                    installed_record.steam_app_id = app_id
                    installed_record.updated_at = _now_wall_ts()
                    await asyncio.to_thread(self.store.save)
                    self._invalidate_panel_cache(installed=True)
                message_parts.append(f"已加入 Steam（AppID {app_id}）")
            else:
                message_parts.append("已加入 Steam")
        else:
            reason = str(steam_result.get("message", "") or "").strip() or "未知错误"
            message_parts.append(f"Steam 导入失败: {reason}")

        if bool(settings.auto_delete_package):
            try:
                os.remove(local_path)
                message_parts.append("已删除下载压缩包")
            except Exception as exc:
                message_parts.append(f"删除压缩包失败: {exc}")

        task.install_message = "，".join(message_parts)

    def _resolve_install_target_dir(self, task: TianyiTaskRecord, install_root: str) -> str:
        """解析任务的目标安装目录。"""
        raw_openpath = str(task.openpath or "").strip().replace("\\", "/")
        if raw_openpath:
            parts = [
                self._sanitize_path_segment(part)
                for part in raw_openpath.split("/")
                if part and part not in {".", ".."}
            ]
            parts = [part for part in parts if part]
            if parts:
                # openpath 仅用于确定游戏根目录，避免把 exe 名当成目录继续拼接。
                root_part = parts[0]
                if len(parts) == 1:
                    stem, ext = os.path.splitext(root_part)
                    if ext:
                        normalized_stem = self._sanitize_path_segment(stem)
                        if normalized_stem:
                            root_part = normalized_stem
                return os.path.join(install_root, root_part)

        title = self._sanitize_path_segment(str(task.game_title or task.file_name or task.game_id or "game"))
        if not title:
            title = "game"
        game_id = self._sanitize_path_segment(str(task.game_id or ""))
        if game_id:
            return os.path.join(install_root, f"{title}_{game_id[:12]}")
        return os.path.join(install_root, title)

    async def _auto_register_task_to_steam(self, *, task: TianyiTaskRecord, target_dir: str) -> Dict[str, Any]:
        """安装完成后自动写入 Steam 快捷方式、Proton 和封面。"""
        exe_path = self._resolve_installed_executable_path(task=task, target_dir=target_dir)
        if not exe_path:
            return {"ok": False, "message": "安装目录未找到可执行文件"}

        game_id = str(task.game_id or "").strip()
        launch_token = re.sub(r"[^a-zA-Z0-9._-]+", "_", game_id or task.task_id or "game").strip("_")
        if not launch_token:
            launch_token = "game"
        launch_options = f"freedeck:tianyi:{launch_token}"

        display_name = self._derive_display_title_for_steam(str(task.game_title or task.file_name or "Freedeck Game"))

        categories = ""
        try:
            entry = self.catalog.get_by_game_id(game_id)
            if entry is not None:
                categories = str(entry.categories or "")
        except Exception:
            categories = ""

        cover_info: Dict[str, Any] = {}
        try:
            cover_info = await self.resolve_catalog_cover(
                game_id=game_id,
                title=str(task.game_title or display_name),
                categories=categories,
            )
        except Exception as exc:
            config.logger.warning("Resolve catalog cover failed for %s: %s", game_id or display_name, exc)
            cover_info = {}

        cover_landscape = str(cover_info.get("cover_url", "") or "").strip()
        cover_portrait = str(cover_info.get("square_cover_url", "") or "").strip()
        steam_app_id = _safe_int(cover_info.get("app_id"), 0)

        try:
            result = await add_or_update_tianyi_shortcut(
                game_id=game_id or task.task_id,
                display_name=display_name,
                exe_path=exe_path,
                launch_options=launch_options,
                proton_tool="proton_experimental",
                cover_landscape_url=cover_landscape,
                cover_portrait_url=cover_portrait,
                steam_app_id=steam_app_id,
            )
            if not result.get("ok"):
                config.logger.warning(
                    "Auto add to Steam failed game=%s exe=%s reason=%s",
                    game_id or display_name,
                    exe_path,
                    result.get("message", ""),
                )
            return result
        except Exception as exc:
            config.logger.exception("Auto add to Steam exception game=%s exe=%s", game_id or display_name, exe_path)
            return {"ok": False, "message": str(exc)}

    def _derive_display_title_for_steam(self, title: str) -> str:
        """优先取中文标题片段。"""
        raw = " ".join(str(title or "").replace("\u3000", " ").split())
        if not raw:
            return "Freedeck Game"

        parts = [part.strip() for part in re.split(r"[\\/|｜丨]+", raw) if part and part.strip()]
        if not parts:
            return raw

        for part in parts:
            if re.search(r"[\u4e00-\u9fff]", part):
                return part
        return parts[0]

    def _resolve_installed_executable_path(self, *, task: TianyiTaskRecord, target_dir: str) -> str:
        """基于 openpath 优先定位安装后的可执行文件。"""
        root = os.path.realpath(os.path.expanduser(str(target_dir or "").strip()))
        if not root or not os.path.isdir(root):
            return ""

        raw_openpath = str(task.openpath or "").strip().replace("\\", "/")
        if raw_openpath:
            parts = [
                self._sanitize_path_segment(part)
                for part in raw_openpath.split("/")
                if part and part not in {".", ".."}
            ]
            parts = [part for part in parts if part]

            if parts:
                rel_parts = parts[1:] if len(parts) > 1 else parts
                if rel_parts:
                    candidate = os.path.realpath(os.path.join(root, *rel_parts))
                    if os.path.isfile(candidate):
                        return candidate
                    leaf = rel_parts[-1]
                    matched_leaf = self._find_path_by_leaf_name(root, leaf, max_depth=8)
                    if matched_leaf:
                        return matched_leaf

        fallback = self._find_first_executable_candidate(root, max_depth=8)
        if fallback:
            return fallback
        return ""

    def _find_path_by_leaf_name(self, root_dir: str, leaf_name: str, max_depth: int = 6) -> str:
        """在目录树中按文件名查找（大小写不敏感）。"""
        root = os.path.realpath(os.path.expanduser(str(root_dir or "").strip()))
        leaf = str(leaf_name or "").strip().lower()
        if not root or not leaf or not os.path.isdir(root):
            return ""

        base_depth = root.count(os.sep)
        for dirpath, dirnames, filenames in os.walk(root):
            depth = dirpath.count(os.sep) - base_depth
            if depth >= max_depth:
                dirnames[:] = []
            for name in filenames:
                if str(name).lower() != leaf:
                    continue
                return os.path.realpath(os.path.join(dirpath, name))
        return ""

    def _find_first_executable_candidate(self, root_dir: str, max_depth: int = 6) -> str:
        """回退查找首个可执行文件候选（优先 .exe）。"""
        root = os.path.realpath(os.path.expanduser(str(root_dir or "").strip()))
        if not root or not os.path.isdir(root):
            return ""

        best_path = ""
        best_rank: Tuple[int, int, int] = (9, 999, 99999)
        base_depth = root.count(os.sep)

        for dirpath, dirnames, filenames in os.walk(root):
            depth = dirpath.count(os.sep) - base_depth
            if depth >= max_depth:
                dirnames[:] = []

            for name in filenames:
                lower = str(name).lower()
                ext_rank = 9
                if lower.endswith(".exe"):
                    ext_rank = 0
                elif lower.endswith(".bat") or lower.endswith(".cmd"):
                    ext_rank = 1
                elif lower.endswith(".sh") or lower.endswith(".x86_64"):
                    ext_rank = 2
                elif lower.endswith(".appimage"):
                    ext_rank = 3
                else:
                    continue

                candidate = os.path.realpath(os.path.join(dirpath, name))
                rank = (ext_rank, max(0, depth), len(candidate))
                if rank < best_rank:
                    best_rank = rank
                    best_path = candidate

        return best_path

    def _sanitize_path_segment(self, text: str) -> str:
        """清理路径片段，避免非法字符。"""
        raw = str(text or "").strip()
        if not raw:
            return ""
        cleaned_chars: List[str] = []
        for ch in raw:
            if ord(ch) < 32:
                continue
            if ch in '<>:"/\\|?*':
                continue
            cleaned_chars.append(ch)
        value = "".join(cleaned_chars).strip().strip(".")
        return value

    def _is_archive_file(self, file_path: str) -> bool:
        """判断文件是否为支持的压缩包。"""
        normalized = str(file_path or "").strip().lower()
        return any(normalized.endswith(ext) for ext in ARCHIVE_SUFFIXES)

    def _extract_archive_to_dir(self, archive_path: str, target_dir: str) -> Tuple[bool, str]:
        """解压压缩包到目标目录。"""
        normalized = str(archive_path or "").strip().lower()
        try:
            os.makedirs(target_dir, exist_ok=True)
        except Exception as exc:
            return False, f"创建安装目录失败: {exc}"

        parent_dir = os.path.dirname(target_dir)
        staging_parent = parent_dir if parent_dir and os.path.isdir(parent_dir) else None
        try:
            staging_dir = tempfile.mkdtemp(prefix="freedeck_extract_", dir=staging_parent)
        except Exception:
            staging_dir = tempfile.mkdtemp(prefix="freedeck_extract_")

        try:
            if normalized.endswith(".7z") or normalized.endswith(".rar"):
                try:
                    self.seven_zip.extract_archive(archive_path, staging_dir)
                except SevenZipError as exc:
                    return False, str(exc)
                except Exception as exc:
                    return False, f"7z 解压异常: {exc}"
            else:
                try:
                    shutil.unpack_archive(archive_path, staging_dir)
                except Exception as exc:
                    return False, f"解压失败: {exc}"

            try:
                self._merge_extracted_content(staging_dir, target_dir)
            except Exception as exc:
                return False, f"整理解压目录失败: {exc}"
            return True, ""
        finally:
            shutil.rmtree(staging_dir, ignore_errors=True)

    def _merge_extracted_content(self, staging_dir: str, target_dir: str) -> None:
        """将临时解压目录内容合并到目标目录，并尽量去掉一层包内同名根目录。"""
        if not staging_dir or not os.path.isdir(staging_dir):
            return
        os.makedirs(target_dir, exist_ok=True)

        entries = [name for name in os.listdir(staging_dir) if name not in {".", ".."}]
        if not entries:
            return

        source_root = staging_dir
        target_base = os.path.basename(os.path.normpath(target_dir))
        preferred = os.path.join(staging_dir, target_base)
        if target_base and os.path.isdir(preferred):
            source_root = preferred
        else:
            dir_entries = [name for name in entries if os.path.isdir(os.path.join(staging_dir, name))]
            file_entries = [name for name in entries if not os.path.isdir(os.path.join(staging_dir, name))]
            if len(dir_entries) == 1 and not file_entries:
                source_root = os.path.join(staging_dir, dir_entries[0])

        for name in os.listdir(source_root):
            source_path = os.path.join(source_root, name)
            target_path = os.path.join(target_dir, name)
            self._merge_path(source_path, target_path)

    def _merge_path(self, source_path: str, target_path: str) -> None:
        """将 source_path 合并到 target_path，目录递归合并，文件冲突时覆盖。"""
        if not os.path.exists(source_path):
            return

        if not os.path.exists(target_path):
            shutil.move(source_path, target_path)
            return

        source_is_dir = os.path.isdir(source_path) and not os.path.islink(source_path)
        target_is_dir = os.path.isdir(target_path) and not os.path.islink(target_path)
        if source_is_dir and target_is_dir:
            for child in os.listdir(source_path):
                self._merge_path(
                    os.path.join(source_path, child),
                    os.path.join(target_path, child),
                )
            try:
                os.rmdir(source_path)
            except Exception:
                pass
            return

        if target_is_dir:
            shutil.rmtree(target_path, ignore_errors=True)
        else:
            try:
                os.remove(target_path)
            except Exception:
                pass
        shutil.move(source_path, target_path)

    async def _set_qr_login_state(
        self,
        *,
        session_id: str,
        stage: str,
        message: str,
        reason: str,
        next_action: str,
        user_account: str,
        image_url: str,
        expires_at: int,
        diagnostics: Optional[Dict[str, Any]],
    ) -> None:
        """更新二维码登录状态。"""
        self._qr_login_state = {
            "session_id": str(session_id or ""),
            "stage": str(stage or "idle"),
            "message": str(message or ""),
            "reason": str(reason or ""),
            "next_action": str(next_action or ""),
            "user_account": str(user_account or ""),
            "image_url": str(image_url or ""),
            "expires_at": int(expires_at or 0),
            "updated_at": _now_wall_ts(),
            "diagnostics": diagnostics or {},
        }

    async def _safe_close_client_session(self, client: Optional[aiohttp.ClientSession]) -> None:
        """安全关闭 aiohttp 会话。"""
        if not isinstance(client, aiohttp.ClientSession):
            return
        if client.closed:
            return
        try:
            await client.close()
        except Exception:
            pass

    async def _close_qr_login_context_locked(self) -> None:
        """关闭并清理二维码登录上下文（需持有锁）。"""
        context = self._qr_login_context
        self._qr_login_context = None
        if not isinstance(context, dict):
            return
        await self._safe_close_client_session(context.get("client"))

    def _build_qr_ssl_context(self) -> Tuple[ssl.SSLContext, Dict[str, Any]]:
        """构建二维码登录用 TLS 上下文并输出证书链诊断。"""
        diagnostics: Dict[str, Any] = {
            "mode": "verify",
            "selected_ca_file": "",
            "candidate_ca_files": [],
            "candidate_errors": [],
        }

        env_cert_file = str(os.environ.get("SSL_CERT_FILE", "") or "").strip()
        candidates: List[str] = []
        if env_cert_file:
            candidates.append(env_cert_file)
        candidates.extend(list(QR_CA_CANDIDATE_FILES))

        try:
            import certifi  # type: ignore

            certifi_path = str(certifi.where() or "").strip()
            if certifi_path:
                candidates.append(certifi_path)
        except Exception:
            pass

        dedup_candidates: List[str] = []
        seen = set()
        for raw in candidates:
            path = os.path.realpath(os.path.expanduser(str(raw).strip()))
            if not path or path in seen:
                continue
            seen.add(path)
            dedup_candidates.append(path)

        diagnostics["candidate_ca_files"] = dedup_candidates
        for path in dedup_candidates:
            if not os.path.isfile(path):
                continue
            try:
                context = ssl.create_default_context(cafile=path)
                diagnostics["selected_ca_file"] = path
                return context, diagnostics
            except Exception as exc:
                diagnostics["candidate_errors"].append({"path": path, "error": str(exc)})

        context = ssl.create_default_context()
        diagnostics["selected_ca_file"] = "system_default"

        # 仅用于紧急排障，默认不关闭校验。
        insecure_flag = str(os.environ.get("FREEDECK_QR_INSECURE_TLS", "") or "").strip().lower()
        if insecure_flag in {"1", "true", "yes"}:
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE
            diagnostics["mode"] = "insecure"
            diagnostics["selected_ca_file"] = "insecure_env_override"

        return context, diagnostics

    def _build_qr_headers(self, *, req_id: str, lt: str, referer: str) -> Dict[str, str]:
        """构建二维码相关请求头。"""
        headers: Dict[str, str] = {
            "User-Agent": "Mozilla/5.0 (Freedeck/1.0)",
            "Accept": "application/json, text/plain, */*",
            "Referer": referer or "https://open.e.189.cn/",
        }
        if req_id:
            headers["reqId"] = req_id
            headers["REQID"] = req_id
        if lt:
            headers["lt"] = lt
        return headers

    def _parse_json_like_text(self, raw_text: str) -> Dict[str, Any]:
        """解析 text/html 包裹的 JSON 返回。"""
        text = str(raw_text or "").strip()
        if not text:
            raise TianyiApiError("接口返回为空")
        try:
            payload = json.loads(text)
        except Exception as exc:
            raise TianyiApiError(f"JSON 解析失败: {exc}") from exc
        if not isinstance(payload, dict):
            raise TianyiApiError("接口返回结构异常")
        return payload

    def _extract_qr_status_code(self, payload: Dict[str, Any]) -> int:
        """提取二维码轮询状态码。"""
        for key in ("status", "result", "code", "res_code"):
            if key not in payload:
                continue
            try:
                return int(str(payload.get(key)))
            except Exception:
                continue
        return -99999

    def _extract_qr_redirect_url(self, payload: Dict[str, Any]) -> str:
        """提取扫码成功后的跳转地址。"""
        direct_keys = ("redirectUrl", "redirectURL", "url", "targetUrl", "jumpUrl")
        for key in direct_keys:
            value = str(payload.get(key, "") or "").strip()
            if value:
                return value
        data_obj = payload.get("data")
        if isinstance(data_obj, dict):
            for key in direct_keys:
                value = str(data_obj.get(key, "") or "").strip()
                if value:
                    return value
        return ""

    def _build_tianyi_cookie_from_cookie_jar(self, jar: aiohttp.CookieJar) -> str:
        """从 aiohttp CookieJar 组装 189 域 Cookie 头。"""
        kv_map: Dict[str, str] = {}

        # 优先按目标域筛选，兼容 host-only cookie（无 Domain 属性）。
        for target in (
            URL("https://cloud.189.cn/"),
            URL("https://h5.cloud.189.cn/"),
            URL("https://open.e.189.cn/"),
        ):
            try:
                scoped = jar.filter_cookies(target)
            except Exception:
                scoped = {}
            for key, morsel in scoped.items():
                name = str(key or "").strip()
                value = str(getattr(morsel, "value", "") or "").strip()
                if not name or not value:
                    continue
                # 保留 cloud/h5 首次命中的同名值，避免被 open.e 同名 cookie 覆盖。
                if name not in kv_map:
                    kv_map[name] = value

        # 兜底：补充所有 189 域 cookie（防止某些环境 filter 丢失）。
        for morsel in jar:
            try:
                domain = str(morsel["domain"] or "").lower()
            except Exception:
                domain = ""
            if domain and "189.cn" not in domain:
                continue
            name = str(getattr(morsel, "key", "") or "").strip()
            value = str(getattr(morsel, "value", "") or "").strip()
            if not name or not value:
                continue
            if name not in kv_map:
                kv_map[name] = value

        if not kv_map:
            return ""
        ordered = [f"{k}={v}" for k, v in sorted(kv_map.items(), key=lambda item: item[0].lower())]
        return "; ".join(ordered)

    async def _bootstrap_qr_login_context(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """初始化二维码登录上下文。"""
        client = context.get("client")
        if not isinstance(client, aiohttp.ClientSession):
            raise TianyiApiError("二维码会话未初始化")

        redirect_url = quote("https://cloud.189.cn/web/main/", safe="")
        bootstrap_url = f"https://cloud.189.cn/api/portal/loginUrl.action?redirectURL={redirect_url}&pageId=1"

        base_headers = {
            "User-Agent": "Mozilla/5.0 (Freedeck/1.0)",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }
        async with client.get(bootstrap_url, headers=base_headers, allow_redirects=False) as resp:
            if resp.status in {301, 302, 303, 307, 308}:
                login_entry_url = str(resp.headers.get("Location", "") or "").strip()
            else:
                login_entry_url = str(resp.url)
            if not login_entry_url:
                raise TianyiApiError("未获取到天翼登录入口地址")

        async with client.get(login_entry_url, headers=base_headers, allow_redirects=True) as resp:
            login_page_url = str(resp.url)
            await resp.text()

        query = parse_qs(urlparse(login_page_url).query or "", keep_blank_values=True)
        app_id = str((query.get("appId") or [""])[0] or "").strip()
        lt = str((query.get("lt") or [""])[0] or "").strip()
        req_id = str((query.get("reqId") or [""])[0] or "").strip()
        if not app_id or not lt or not req_id:
            raise TianyiApiError("登录页面参数缺失（appId/lt/reqId）")

        api_headers = self._build_qr_headers(req_id=req_id, lt=lt, referer=login_page_url)

        async with client.post(
            "https://open.e.189.cn/api/logbox/oauth2/appConf.do",
            data={"version": "2.0", "appKey": app_id},
            headers=api_headers,
        ) as resp:
            app_conf_text = await resp.text()
            if resp.status >= 400:
                raise TianyiApiError(f"appConf 请求失败 status={resp.status}")
        app_conf_payload = self._parse_json_like_text(app_conf_text)
        app_conf_data = app_conf_payload.get("data")
        if not isinstance(app_conf_data, dict):
            raise TianyiApiError("appConf 返回异常")

        async with client.post(
            "https://open.e.189.cn/api/logbox/oauth2/getUUID.do",
            data={"appId": app_id},
            headers=api_headers,
        ) as resp:
            uuid_text = await resp.text()
            if resp.status >= 400:
                raise TianyiApiError(f"getUUID 请求失败 status={resp.status}")
        uuid_payload = self._parse_json_like_text(uuid_text)
        if self._extract_qr_status_code(uuid_payload) != QR_STATUS_SUCCESS:
            msg = str(uuid_payload.get("msg", "") or "二维码生成失败")
            raise TianyiApiError(msg)

        uuid_value = str(uuid_payload.get("uuid", "") or "").strip()
        encryuuid = str(uuid_payload.get("encryuuid", "") or "").strip()
        encodeuuid = str(uuid_payload.get("encodeuuid", "") or "").strip()
        if not uuid_value or not encryuuid or not encodeuuid:
            raise TianyiApiError("二维码参数缺失（uuid/encryuuid/encodeuuid）")

        image_remote_url = f"https://open.e.189.cn/api/logbox/oauth2/image.do?uuid={encodeuuid}&REQID={req_id}"
        state_payload = {
            "appId": app_id,
            "encryuuid": encryuuid,
            "date": str(int(time.time() * 1000)),
            "uuid": uuid_value,
            "returnUrl": str(app_conf_data.get("returnUrl") or ""),
            "clientType": str(app_conf_data.get("clientType") or "1"),
            "timeStamp": str(int(time.time() * 1000)),
            "cb_SaveName": str(app_conf_data.get("defaultSaveName") or ""),
            "isOauth2": "false" if str(app_conf_data.get("isOauth2")).lower() == "false" else "true",
            "state": str(app_conf_data.get("state") or ""),
            "paramId": str(app_conf_data.get("paramId") or ""),
        }

        return {
            "app_id": app_id,
            "lt": lt,
            "req_id": req_id,
            "login_page_url": login_page_url,
            "image_remote_url": image_remote_url,
            "state_payload": state_payload,
        }

    async def _finalize_qr_login_success(
        self,
        *,
        context: Dict[str, Any],
        redirect_url: str,
    ) -> Tuple[str, str, str]:
        """扫码成功后拉起回调并验证账号。"""
        client = context.get("client")
        if not isinstance(client, aiohttp.ClientSession):
            return "", "", "qr_client_invalid"

        req_id = str(context.get("req_id", ""))
        lt = str(context.get("lt", ""))
        login_page_url = str(context.get("login_page_url", ""))
        headers = self._build_qr_headers(req_id=req_id, lt=lt, referer=login_page_url)
        headers["Accept"] = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"

        candidate_urls: List[str] = []
        redirect = str(redirect_url or "").strip()
        if redirect:
            candidate_urls.append(redirect)
        candidate_urls.append("https://cloud.189.cn/web/main/")

        for url in candidate_urls:
            try:
                async with client.get(url, headers=headers, allow_redirects=True) as resp:
                    await resp.read()
            except Exception:
                continue

        cloud_scoped_count = 0
        open_scoped_count = 0
        try:
            cloud_scoped_count = len(client.cookie_jar.filter_cookies(URL("https://cloud.189.cn/")).items())
        except Exception:
            cloud_scoped_count = 0
        try:
            open_scoped_count = len(client.cookie_jar.filter_cookies(URL("https://open.e.189.cn/")).items())
        except Exception:
            open_scoped_count = 0

        cookie = self._build_tianyi_cookie_from_cookie_jar(client.cookie_jar)
        if not cookie:
            return "", "", f"cookie_missing:cloud={cloud_scoped_count},open={open_scoped_count}"

        account, verify_reason = await self._verify_cookie_candidate(cookie)
        if not account:
            scoped = f"cloud={cloud_scoped_count},open={open_scoped_count}"
            return "", cookie, f"{verify_reason or 'account_verify_failed'}:{scoped}"
        return account, cookie, ""

    async def _set_capture_state(
        self,
        *,
        stage: str,
        message: str,
        reason: str,
        next_action: str,
        user_account: str,
        diagnostics: Optional[Dict[str, Any]],
        source_attempts: Optional[List[str]] = None,
        success_source: str = "",
        source_diagnostics: Optional[Dict[str, Any]] = None,
    ) -> None:
        """更新自动采集状态。"""
        attempts = [str(item) for item in (source_attempts or []) if str(item).strip()]
        source_diag = dict(source_diagnostics or {})
        self._capture_state = {
            "stage": str(stage or "idle"),
            "message": str(message or ""),
            "reason": str(reason or ""),
            "next_action": str(next_action or ""),
            "user_account": str(user_account or ""),
            "updated_at": _now_wall_ts(),
            "diagnostics": diagnostics or {},
            "source_attempts": attempts,
            "success_source": str(success_source or ""),
            "source_diagnostics": source_diag,
        }

    def _normalize_capture_host(self, host: str) -> str:
        """归一化采集探测中的 host，便于识别互跳。"""
        raw = str(host or "").strip().lower()
        if not raw:
            return ""
        if raw.endswith(".h5.cloud.189.cn") or raw == "h5.cloud.189.cn":
            return "h5.cloud.189.cn"
        if raw.endswith(".cloud.189.cn") or raw == "cloud.189.cn":
            return "cloud.189.cn"
        return raw

    def _extract_probe_hosts(self, probe: Dict[str, Any]) -> List[str]:
        """从探针结果提取 host 列表。"""
        hosts: List[str] = []
        pages = probe.get("page_candidates")
        if isinstance(pages, list):
            for item in pages:
                if not isinstance(item, dict):
                    continue
                host = self._normalize_capture_host(item.get("host", ""))
                if host and host not in hosts:
                    hosts.append(host)
        matched = probe.get("matched_page")
        if isinstance(matched, dict):
            host = self._normalize_capture_host(matched.get("host", ""))
            if host and host not in hosts:
                hosts.insert(0, host)
        return hosts

    def _is_capture_redirect_loop(self, host_history: List[str]) -> bool:
        """判断是否命中 cloud/h5 互跳。"""
        if len(host_history) < CAPTURE_LOOP_WINDOW:
            return False

        tail = host_history[-CAPTURE_LOOP_WINDOW:]
        if any(host not in CAPTURE_LOOP_CORE_HOSTS for host in tail):
            return False
        if len(set(tail)) != 2:
            return False
        for idx in range(1, len(tail)):
            if tail[idx] == tail[idx - 1]:
                return False
        return True

    def _build_capture_diag_payload(
        self,
        *,
        reason: str,
        source_diagnostics: Dict[str, Any],
        main_landing_detected: bool,
        host_history: Optional[List[str]] = None,
        elapsed_seconds: Optional[int] = None,
        remaining_seconds: Optional[int] = None,
    ) -> Dict[str, Any]:
        """构建统一诊断结构，便于前端展示。"""
        payload: Dict[str, Any] = {
            "reason": str(reason or ""),
            "main_landing_detected": bool(main_landing_detected),
            "source_diagnostics": dict(source_diagnostics or {}),
        }
        if host_history:
            payload["host_history"] = list(host_history)
        if elapsed_seconds is not None:
            payload["elapsed_seconds"] = int(elapsed_seconds)
        if remaining_seconds is not None:
            payload["remaining_seconds"] = int(remaining_seconds)
        return payload

    async def _verify_cookie_candidate(self, cookie: str) -> Tuple[str, str]:
        """校验候选 Cookie，返回账号与失败原因。"""
        normalized = str(cookie or "").strip()
        if not normalized:
            return "", "empty_cookie"
        try:
            account = await get_user_account(normalized)
        except Exception as exc:
            return "", f"account_verify_exception:{exc}"
        if not account:
            return "", "account_verify_failed"
        return account, ""

    def _derive_capture_failure_reason(
        self,
        source_diagnostics: Dict[str, Any],
        *,
        main_landing_detected: bool,
    ) -> str:
        """根据来源级探测信息生成失败原因。"""
        if main_landing_detected:
            return "main_landing_verify_failed"

        cdp_reason = str((source_diagnostics.get("cdp") or {}).get("reason", "")).strip()
        cookie_db_reason = str((source_diagnostics.get("cookie_db") or {}).get("reason", "")).strip()

        cdp_unavailable_reasons = {"cdp_endpoints_unreachable", "cdp_no_pages", "cdp_probe_exception"}
        cookie_db_unavailable_reasons = {"cookie_db_not_found", "cookie_db_read_failed", "cookie_db_probe_exception"}

        if cdp_reason in cdp_unavailable_reasons and cookie_db_reason in cookie_db_unavailable_reasons:
            return "all_sources_unavailable"
        if cdp_reason in cdp_unavailable_reasons and not cookie_db_reason:
            return "cdp_unavailable"
        if cookie_db_reason in cookie_db_unavailable_reasons and not cdp_reason:
            return "cookie_db_unavailable"
        return "no_valid_cookie"

    async def _attempt_capture_sources_once(self) -> Dict[str, Any]:
        """执行一次双来源采集并做统一账号校验。"""
        source_attempts: List[str] = []
        source_diagnostics: Dict[str, Any] = {}
        main_landing_detected = False

        for source in COOKIE_CAPTURE_SOURCES:
            source_attempts.append(source)
            try:
                if source == "cdp":
                    cookie, diag = await self._collect_tianyi_cookie_from_cdp()
                    if bool((diag or {}).get("main_landing_detected")):
                        main_landing_detected = True
                else:
                    cookie, diag = await self._collect_tianyi_cookie_from_cookie_db()
            except Exception as exc:
                cookie = ""
                diag = {
                    "ok": False,
                    "reason": f"{source}_probe_exception",
                    "error": str(exc),
                }

            source_diag = dict(diag or {})
            source_diagnostics[source] = source_diag

            if not cookie:
                continue

            source_diag["cookie_found"] = True
            account, verify_reason = await self._verify_cookie_candidate(cookie)
            source_diag["verify_reason"] = verify_reason
            if account:
                source_diag["ok"] = True
                source_diag["reason"] = ""
                return {
                    "success": True,
                    "cookie": cookie,
                    "account": account,
                    "reason": "",
                    "success_source": source,
                    "source_attempts": source_attempts,
                    "source_diagnostics": source_diagnostics,
                    "main_landing_detected": main_landing_detected,
                }
            if not str(source_diag.get("reason", "")).strip():
                source_diag["reason"] = "account_verify_failed"

        return {
            "success": False,
            "cookie": "",
            "account": "",
            "reason": self._derive_capture_failure_reason(
                source_diagnostics,
                main_landing_detected=main_landing_detected,
            ),
            "success_source": "",
            "source_attempts": source_attempts,
            "source_diagnostics": source_diagnostics,
            "main_landing_detected": main_landing_detected,
        }

    async def _capture_loop(
        self,
        timeout_seconds: int,
        seed_diagnostics: Optional[Dict[str, Any]] = None,
    ) -> None:
        """后台采集循环。"""
        diagnostics: Dict[str, Any] = {"timeout_seconds": int(timeout_seconds)}
        if isinstance(seed_diagnostics, dict):
            diagnostics["entry_seed"] = seed_diagnostics
        await self._set_capture_state(
            stage="running",
            message="请在网页中完成天翼登录，系统将自动采集登录态...",
            reason="",
            next_action="",
            user_account="",
            diagnostics=diagnostics,
        )

        deadline = time.monotonic() + float(timeout_seconds)
        last_source_diagnostics: Dict[str, Any] = {}
        last_source_attempts: List[str] = []
        last_reason = ""
        host_history: List[str] = []
        main_landing_seen = False

        try:
            while time.monotonic() < deadline:
                attempt = await self._attempt_capture_sources_once()
                last_reason = str(attempt.get("reason", "") or "")
                last_source_attempts = list(attempt.get("source_attempts") or [])
                last_source_diagnostics = dict(attempt.get("source_diagnostics") or {})
                main_landing_seen = bool(main_landing_seen or attempt.get("main_landing_detected"))

                cdp_probe = dict((last_source_diagnostics.get("cdp") or {}))
                probe_hosts = self._extract_probe_hosts(cdp_probe)
                if probe_hosts:
                    host_history.append(probe_hosts[0])
                    if len(host_history) > CAPTURE_LOOP_WINDOW + 4:
                        host_history = host_history[-(CAPTURE_LOOP_WINDOW + 4):]

                if self._is_capture_redirect_loop(host_history):
                    reason = "redirect_loop_detected"
                    loop_diag = self._build_capture_diag_payload(
                        reason=reason,
                        source_diagnostics=last_source_diagnostics,
                        main_landing_detected=main_landing_seen,
                        host_history=host_history[-CAPTURE_LOOP_WINDOW:],
                    )
                    await self._set_capture_state(
                        stage="failed",
                        message="检测到 cloud.189.cn 与 h5.cloud.189.cn 持续互跳",
                        reason=reason,
                        next_action="manual_cookie",
                        user_account="",
                        diagnostics=loop_diag,
                        source_attempts=last_source_attempts,
                        success_source="",
                        source_diagnostics=last_source_diagnostics,
                    )
                    return

                if bool(attempt.get("success")):
                    resolved_cookie = str(attempt.get("cookie", "") or "")
                    resolved_account = str(attempt.get("account", "") or "")
                    success_source = str(attempt.get("success_source", "") or "")
                    if resolved_cookie and resolved_account:
                        self.store.set_login(resolved_cookie, resolved_account)
                        done_diag = self._build_capture_diag_payload(
                            reason="",
                            source_diagnostics=last_source_diagnostics,
                            main_landing_detected=main_landing_seen,
                            host_history=host_history[-CAPTURE_LOOP_WINDOW:] if host_history else None,
                        )
                        await self._set_capture_state(
                            stage="completed",
                            message=f"登录成功：{resolved_account}",
                            reason="",
                            next_action="",
                            user_account=resolved_account,
                            diagnostics=done_diag,
                            source_attempts=last_source_attempts,
                            success_source=success_source,
                            source_diagnostics=last_source_diagnostics,
                        )
                        return

                elapsed = int(max(0.0, float(timeout_seconds) - max(0.0, deadline - time.monotonic())))
                remaining = int(max(0.0, deadline - time.monotonic()))
                running_diag = self._build_capture_diag_payload(
                    reason=last_reason,
                    source_diagnostics=last_source_diagnostics,
                    main_landing_detected=main_landing_seen,
                    host_history=host_history[-CAPTURE_LOOP_WINDOW:] if host_history else None,
                    elapsed_seconds=elapsed,
                    remaining_seconds=remaining,
                )
                running_message = "等待登录完成并同步登录态..."
                if last_reason == "main_landing_verify_failed":
                    running_message = "检测到已到达主站，正在持续校验账号登录态..."
                await self._set_capture_state(
                    stage="running",
                    message=running_message,
                    reason=last_reason,
                    next_action="",
                    user_account="",
                    diagnostics=running_diag,
                    source_attempts=last_source_attempts,
                    success_source="",
                    source_diagnostics=last_source_diagnostics,
                )
                await asyncio.sleep(1.8)

            timeout_reason = "capture_timeout"
            timeout_message = "自动采集超时，请改用手动 Cookie"
            if main_landing_seen or last_reason == "main_landing_verify_failed":
                timeout_reason = "main_landing_verify_failed"
                timeout_message = "检测到已跳转主站，但账号校验未通过，请重试登录或改用手动 Cookie"
            timeout_diag = self._build_capture_diag_payload(
                reason=timeout_reason,
                source_diagnostics=last_source_diagnostics,
                main_landing_detected=main_landing_seen,
                host_history=host_history[-CAPTURE_LOOP_WINDOW:] if host_history else None,
            )
            await self._set_capture_state(
                stage="failed",
                message=timeout_message,
                reason=timeout_reason,
                next_action="manual_cookie",
                user_account="",
                diagnostics=timeout_diag,
                source_attempts=last_source_attempts,
                success_source="",
                source_diagnostics=last_source_diagnostics,
            )
        except asyncio.CancelledError:
            stopped_diag = self._build_capture_diag_payload(
                reason="capture_stopped",
                source_diagnostics=last_source_diagnostics,
                main_landing_detected=main_landing_seen,
                host_history=host_history[-CAPTURE_LOOP_WINDOW:] if host_history else None,
            )
            await self._set_capture_state(
                stage="stopped",
                message="采集已停止，可改用手动 Cookie",
                reason="capture_stopped",
                next_action="manual_cookie",
                user_account="",
                diagnostics=stopped_diag,
                source_attempts=last_source_attempts,
                success_source="",
                source_diagnostics=last_source_diagnostics,
            )
            raise
        except Exception as exc:
            error_diag = self._build_capture_diag_payload(
                reason="capture_exception",
                source_diagnostics=last_source_diagnostics,
                main_landing_detected=main_landing_seen,
                host_history=host_history[-CAPTURE_LOOP_WINDOW:] if host_history else None,
            )
            error_diag["exception"] = str(exc)
            await self._set_capture_state(
                stage="failed",
                message=f"自动采集异常：{exc}",
                reason="capture_exception",
                next_action="manual_cookie",
                user_account="",
                diagnostics=error_diag,
                source_attempts=last_source_attempts,
                success_source="",
                source_diagnostics=last_source_diagnostics,
            )

    async def _collect_tianyi_cookie_from_cdp(self) -> tuple[str, Dict[str, Any]]:
        """从 CEF DevTools 端点尝试提取天翼 cookie。"""
        diagnostics: Dict[str, Any] = {
            "source": "cdp",
            "candidate_ports": list(CDP_ENDPOINT_PORTS),
            "probe_results": [],
            "page_candidates": [],
            "main_landing_detected": False,
            "ok": False,
            "reason": "",
        }
        pages: List[Dict[str, Any]] = []

        timeout = aiohttp.ClientTimeout(total=LOCAL_WEB_PROBE_TIMEOUT_SECONDS)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            for port in CDP_ENDPOINT_PORTS:
                url = f"http://127.0.0.1:{port}/json"
                probe_item: Dict[str, Any] = {"port": port, "url": url, "ok": False}
                try:
                    async with session.get(url) as resp:
                        probe_item["status"] = int(resp.status)
                        text = await resp.text()
                        if 200 <= resp.status < 300:
                            payload = json.loads(text)
                            if isinstance(payload, list):
                                for item in payload:
                                    if isinstance(item, dict):
                                        item["_cdp_port"] = port
                                        pages.append(item)
                                probe_item["ok"] = True
                                probe_item["page_count"] = len(payload)
                            else:
                                probe_item["error"] = "cdp_json_not_list"
                        else:
                            probe_item["error"] = "http_status_not_ok"
                except Exception as exc:
                    probe_item["error"] = str(exc)
                diagnostics["probe_results"].append(probe_item)

        if not pages:
            probe_results = diagnostics.get("probe_results", [])
            if isinstance(probe_results, list) and probe_results and all(not bool(item.get("ok")) for item in probe_results if isinstance(item, dict)):
                diagnostics["reason"] = "cdp_endpoints_unreachable"
            else:
                diagnostics["reason"] = "cdp_no_pages"
            return "", diagnostics

        preferred_pages: List[Dict[str, Any]] = []
        fallback_pages: List[Dict[str, Any]] = []
        all_hosts: List[str] = []
        for page in pages:
            page_url = str(page.get("url", "") or "")
            ws_url = str(page.get("webSocketDebuggerUrl", "") or "")
            if not ws_url:
                continue

            host = ""
            try:
                host = str(urlparse(page_url).hostname or "").lower()
            except Exception:
                host = ""
            if host and host not in all_hosts:
                all_hosts.append(host)
            if page_url.startswith("https://cloud.189.cn/web/main/") or page_url.startswith("http://cloud.189.cn/web/main/"):
                diagnostics["main_landing_detected"] = True
            entry = {
                "title": str(page.get("title", "") or ""),
                "url": page_url,
                "host": host,
                "ws_url": ws_url,
                "port": int(page.get("_cdp_port") or 0),
            }
            if host and any(key in host for key in TIANYI_HOST_KEYWORDS):
                preferred_pages.append(entry)
            else:
                fallback_pages.append(entry)

        diagnostics["page_candidates"] = preferred_pages[:6]
        diagnostics["all_hosts"] = all_hosts[:12]
        selected_pages = preferred_pages + fallback_pages[:3]

        for page in selected_pages:
            ws_url = str(page.get("ws_url", "") or "")
            try:
                cookies = await self._get_all_cookies_from_ws(ws_url)
                cookie_str = self._build_tianyi_cookie_string(cookies)
                if cookie_str:
                    diagnostics["matched_page"] = {
                        "host": page.get("host", ""),
                        "url": page.get("url", ""),
                        "port": page.get("port", 0),
                    }
                    diagnostics["ok"] = True
                    diagnostics["reason"] = ""
                    return cookie_str, diagnostics
            except Exception as exc:
                diagnostics.setdefault("ws_errors", []).append(
                    {
                        "port": page.get("port", 0),
                        "url": page.get("url", ""),
                        "error": str(exc),
                    }
                )

        diagnostics["reason"] = "cdp_no_tianyi_cookie"
        return "", diagnostics

    def _cookie_db_candidate_paths(self) -> List[str]:
        """生成 CookieDB 候选路径列表。"""
        home_dir = str(Path.home())
        raw_paths = [
            os.path.join(home_dir, ".local", "share", "Steam", "config", "htmlcache", "Cookies"),
            os.path.join(home_dir, ".steam", "steam", "config", "htmlcache", "Cookies"),
            os.path.join(home_dir, ".steam", "root", "config", "htmlcache", "Cookies"),
            os.path.join(home_dir, ".steam", "steam", "config", "htmlcache", "Default", "Cookies"),
            os.path.join(home_dir, ".config", "chromium", "Default", "Cookies"),
            os.path.join(
                home_dir,
                ".var",
                "app",
                "com.valvesoftware.Steam",
                ".local",
                "share",
                "Steam",
                "config",
                "htmlcache",
                "Cookies",
            ),
        ]
        dedup: List[str] = []
        seen = set()
        for item in raw_paths:
            path = os.path.realpath(os.path.expanduser(item))
            if path in seen:
                continue
            seen.add(path)
            dedup.append(path)
        return dedup

    def _read_cookie_db_rows(self, db_path: str) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        """从 CookieDB 快照读取天翼域行。"""
        diagnostics: Dict[str, Any] = {"db_path": db_path, "snapshot_path": "", "row_count": 0}
        temp_path = ""
        conn: Optional[sqlite3.Connection] = None
        try:
            with tempfile.NamedTemporaryFile(prefix="freedeck_cookie_", suffix=".db", delete=False) as temp_file:
                temp_path = temp_file.name
            diagnostics["snapshot_path"] = temp_path

            shutil.copy2(db_path, temp_path)

            conn = sqlite3.connect(f"file:{temp_path}?mode=ro", uri=True)
            cursor = conn.execute("PRAGMA table_info(cookies)")
            columns = [str(row[1]) for row in cursor.fetchall()]
            diagnostics["columns"] = columns
            if "host_key" not in columns or "name" not in columns:
                diagnostics["reason"] = "cookie_db_schema_invalid"
                return [], diagnostics

            select_cols = ["host_key", "name"]
            if "value" in columns:
                select_cols.append("value")
            if "encrypted_value" in columns:
                select_cols.append("encrypted_value")
            order_col = "last_access_utc" if "last_access_utc" in columns else "rowid"

            sql = (
                f"SELECT {', '.join(select_cols)} "
                f"FROM cookies "
                f"WHERE host_key LIKE ? "
                f"ORDER BY {order_col} DESC "
                f"LIMIT {COOKIE_DB_MAX_ROWS}"
            )
            rows_raw = conn.execute(sql, ("%189.cn%",)).fetchall()
            rows: List[Dict[str, Any]] = []
            for raw in rows_raw:
                item: Dict[str, Any] = {}
                for idx, col_name in enumerate(select_cols):
                    item[col_name] = raw[idx]
                rows.append(item)
            diagnostics["row_count"] = len(rows)
            diagnostics["reason"] = ""
            return rows, diagnostics
        except Exception as exc:
            diagnostics["reason"] = "cookie_db_read_failed"
            diagnostics["error"] = str(exc)
            return [], diagnostics
        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass
            if temp_path:
                try:
                    os.remove(temp_path)
                except Exception:
                    pass

    async def _collect_tianyi_cookie_from_cookie_db(self) -> tuple[str, Dict[str, Any]]:
        """从本地 CookieDB 尝试提取天翼 Cookie。"""
        candidate_paths = self._cookie_db_candidate_paths()
        diagnostics: Dict[str, Any] = {
            "source": "cookie_db",
            "candidate_paths": candidate_paths,
            "probe_results": [],
            "ok": False,
            "reason": "",
        }

        any_existing = False
        any_read_failed = False
        for path in candidate_paths:
            probe_item: Dict[str, Any] = {"path": path, "exists": False}
            if not os.path.isfile(path):
                diagnostics["probe_results"].append(probe_item)
                continue

            any_existing = True
            probe_item["exists"] = True
            rows, row_diag = await asyncio.to_thread(self._read_cookie_db_rows, path)
            probe_item.update(row_diag)
            if str(row_diag.get("reason", "")).strip():
                any_read_failed = True
                diagnostics["probe_results"].append(probe_item)
                continue

            kv_map: Dict[str, str] = {}
            encrypted_only_count = 0
            for row in rows:
                name = str(row.get("name", "") or "").strip()
                if not name:
                    continue

                raw_value = row.get("value", "")
                value = str(raw_value or "").strip()
                if not value:
                    encrypted_value = row.get("encrypted_value", b"")
                    if isinstance(encrypted_value, memoryview):
                        encrypted_value = encrypted_value.tobytes()
                    if isinstance(encrypted_value, (bytes, bytearray)) and encrypted_value:
                        blob = bytes(encrypted_value)
                        if blob.startswith(b"v10") or blob.startswith(b"v11"):
                            encrypted_only_count += 1
                            continue
                        try:
                            decoded = blob.decode("utf-8", errors="ignore").strip()
                        except Exception:
                            decoded = ""
                        if not decoded:
                            encrypted_only_count += 1
                            continue
                        value = decoded
                if not value:
                    continue
                if name not in kv_map:
                    kv_map[name] = value

            probe_item["cookie_name_count"] = len(kv_map)
            probe_item["encrypted_only_count"] = encrypted_only_count
            diagnostics["probe_results"].append(probe_item)

            if kv_map:
                diagnostics["ok"] = True
                diagnostics["reason"] = ""
                diagnostics["selected_path"] = path
                diagnostics["cookie_name_count"] = len(kv_map)
                ordered = [f"{k}={v}" for k, v in sorted(kv_map.items(), key=lambda item: item[0].lower())]
                return "; ".join(ordered), diagnostics

        if not any_existing:
            diagnostics["reason"] = "cookie_db_not_found"
        elif any_read_failed:
            diagnostics["reason"] = "cookie_db_read_failed"
        else:
            diagnostics["reason"] = "cookie_db_no_tianyi_cookie"
        return "", diagnostics

    async def _get_all_cookies_from_ws(self, ws_url: str) -> List[Dict[str, Any]]:
        """通过 CDP WebSocket 获取所有 cookie。"""
        timeout = aiohttp.ClientTimeout(total=6.0)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.ws_connect(ws_url, autoping=True, heartbeat=10.0) as ws:
                request_id = 1

                async def cdp_call(method: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
                    nonlocal request_id
                    current_id = request_id
                    request_id += 1
                    await ws.send_json({"id": current_id, "method": method, "params": params or {}})

                    while True:
                        msg = await ws.receive(timeout=5.0)
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            payload = json.loads(str(msg.data))
                            if int(payload.get("id", 0) or 0) != current_id:
                                continue
                            if "error" in payload:
                                raise RuntimeError(str(payload.get("error")))
                            return payload.get("result") or {}
                        if msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED):
                            raise RuntimeError("cdp websocket closed")
                        if msg.type == aiohttp.WSMsgType.ERROR:
                            raise RuntimeError("cdp websocket error")

                await cdp_call("Network.enable")
                result = await cdp_call("Network.getAllCookies")
                cookies = result.get("cookies")
                if isinstance(cookies, list):
                    return [item for item in cookies if isinstance(item, dict)]
                return []

    def _build_tianyi_cookie_string(self, cookies: List[Dict[str, Any]]) -> str:
        """从 cookies 中筛出天翼域并组装 Cookie 头。"""
        kv_map: Dict[str, str] = {}
        for item in cookies:
            domain = str(item.get("domain", "") or "").lower()
            if "189.cn" not in domain:
                continue
            name = str(item.get("name", "") or "").strip()
            value = str(item.get("value", "") or "").strip()
            if not name or not value:
                continue
            kv_map[name] = value

        if not kv_map:
            return ""

        ordered = [f"{k}={v}" for k, v in sorted(kv_map.items(), key=lambda x: x[0].lower())]
        return "; ".join(ordered)

    async def _ensure_local_web_ready(self, route_path: str) -> str:
        """确保本地网页服务与目标页面可访问。"""
        diagnostics: Dict[str, Any] = {
            "host_candidates": ["127.0.0.1"],
            "route": route_path,
            "probe_results": [],
        }

        status = await self.plugin.get_server_status()
        diagnostics["status_before"] = dict(status)
        if not bool(status.get("running")):
            start_result = await self.plugin.start_server(self.plugin.server_port)
            diagnostics["start_result"] = dict(start_result)
            if start_result.get("status") != "success":
                raise LocalWebNotReadyError(
                    str(start_result.get("message", "本地网页服务启动失败")),
                    reason="local_server_start_failed",
                    diagnostics=diagnostics,
                )
            status = await self.plugin.get_server_status()
            diagnostics["status_after"] = dict(status)

        if not bool(status.get("running")):
            raise LocalWebNotReadyError(
                "本地网页服务未就绪，请稍后再试",
                reason="local_server_not_running",
                diagnostics=diagnostics,
            )

        port = int(status.get("port") or self.plugin.server_port)
        diagnostics["port"] = port

        health_probe = await self._probe_local_route(port, "/_healthz")
        page_probe = await self._probe_local_route(port, route_path.split("?", 1)[0])
        diagnostics["probe_results"].append(health_probe)
        diagnostics["probe_results"].append(page_probe)

        if not health_probe.get("ok"):
            raise LocalWebNotReadyError(
                "本地网页基础探针未通过，请稍后再试",
                reason="health_probe_failed",
                diagnostics=diagnostics,
            )

        if not page_probe.get("ok"):
            raise LocalWebNotReadyError(
                "本地网页页面探针未通过，请稍后再试",
                reason="page_probe_failed",
                diagnostics=diagnostics,
            )

        return f"http://127.0.0.1:{port}{route_path}"

    async def _peek_local_web_url(self, route_path: str) -> str:
        """仅检查当前服务状态，不主动拉起服务。"""
        status = await self.plugin.get_server_status()
        if not bool(status.get("running")):
            return ""

        port = int(status.get("port") or self.plugin.server_port)
        probe = await self._probe_local_route(port, route_path.split("?", 1)[0])
        if not probe.get("ok"):
            return ""

        return f"http://127.0.0.1:{port}{route_path}"

    async def _probe_local_route(self, port: int, path: str) -> Dict[str, Any]:
        """探测本地路由是否可访问。"""
        url = f"http://127.0.0.1:{int(port)}{path}"
        result: Dict[str, Any] = {
            "url": url,
            "path": path,
            "ok": False,
        }

        timeout = aiohttp.ClientTimeout(total=LOCAL_WEB_READY_TIMEOUT_SECONDS)
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(url, allow_redirects=False) as resp:
                    result["status"] = int(resp.status)
                    result["ok"] = 200 <= resp.status < 400
        except Exception as exc:
            result["error"] = str(exc)
        return result
