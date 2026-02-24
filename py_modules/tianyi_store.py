# tianyi_store.py - 天翼下载状态存储
#
# 该模块只负责本地状态读写，不处理网络请求。

from __future__ import annotations

import json
import os
import threading
import time
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional


def _now_ts() -> int:
    """返回当前秒级时间戳。"""
    return int(time.time())


def _to_int(value: Any, default: int) -> int:
    """安全转换整数，失败时回退默认值。"""
    try:
        return int(value)
    except Exception:
        return default


def _to_bool(value: Any, default: bool = False) -> bool:
    """安全转换布尔值。"""
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    return default


@dataclass
class TianyiLoginState:
    """天翼登录态。"""

    cookie: str = ""
    user_account: str = ""
    updated_at: int = 0


@dataclass
class TianyiSettings:
    """下载相关设置。"""

    download_dir: str = ""
    install_dir: str = ""
    split_count: int = 16
    page_size: int = 50
    auto_delete_package: bool = False
    auto_install: bool = True


@dataclass
class TianyiTaskRecord:
    """下载任务记录。"""

    task_id: str
    gid: str
    game_id: str
    game_title: str
    share_code: str
    share_id: str
    file_id: str
    file_name: str
    download_dir: str
    local_path: str
    status: str
    progress: float
    speed: int
    openpath: str = ""
    install_status: str = "pending"
    install_message: str = ""
    installed_path: str = ""
    post_processed: bool = False
    error_reason: str = ""
    created_at: int = field(default_factory=_now_ts)
    updated_at: int = field(default_factory=_now_ts)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "TianyiTaskRecord":
        """从字典恢复任务对象。"""
        return cls(
            task_id=str(data.get("task_id", "")),
            gid=str(data.get("gid", "")),
            game_id=str(data.get("game_id", "")),
            game_title=str(data.get("game_title", "")),
            share_code=str(data.get("share_code", "")),
            share_id=str(data.get("share_id", "")),
            file_id=str(data.get("file_id", "")),
            file_name=str(data.get("file_name", "")),
            download_dir=str(data.get("download_dir", "")),
            local_path=str(data.get("local_path", "")),
            status=str(data.get("status", "waiting")),
            progress=float(data.get("progress", 0.0) or 0.0),
            speed=_to_int(data.get("speed", 0), 0),
            openpath=str(data.get("openpath", "") or ""),
            install_status=str(data.get("install_status", "pending") or "pending"),
            install_message=str(data.get("install_message", "") or ""),
            installed_path=str(data.get("installed_path", "") or ""),
            post_processed=_to_bool(data.get("post_processed", False), False),
            error_reason=str(data.get("error_reason", "") or ""),
            created_at=_to_int(data.get("created_at", _now_ts()), _now_ts()),
            updated_at=_to_int(data.get("updated_at", _now_ts()), _now_ts()),
        )


@dataclass
class TianyiInstalledGame:
    """已安装游戏记录。"""

    game_id: str
    game_title: str
    install_path: str
    source_path: str
    status: str = "installed"
    size_bytes: int = 0
    steam_app_id: int = 0
    playtime_seconds: int = 0
    playtime_sessions: int = 0
    playtime_last_played_at: int = 0
    playtime_active_started_at: int = 0
    playtime_active_app_id: int = 0
    created_at: int = field(default_factory=_now_ts)
    updated_at: int = field(default_factory=_now_ts)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "TianyiInstalledGame":
        """从字典恢复已安装游戏对象。"""
        return cls(
            game_id=str(data.get("game_id", "")),
            game_title=str(data.get("game_title", "")),
            install_path=str(data.get("install_path", "")),
            source_path=str(data.get("source_path", "")),
            status=str(data.get("status", "installed") or "installed"),
            size_bytes=max(0, _to_int(data.get("size_bytes", 0), 0)),
            steam_app_id=max(0, _to_int(data.get("steam_app_id", 0), 0)),
            playtime_seconds=max(0, _to_int(data.get("playtime_seconds", 0), 0)),
            playtime_sessions=max(0, _to_int(data.get("playtime_sessions", 0), 0)),
            playtime_last_played_at=max(0, _to_int(data.get("playtime_last_played_at", 0), 0)),
            playtime_active_started_at=max(0, _to_int(data.get("playtime_active_started_at", 0), 0)),
            playtime_active_app_id=max(0, _to_int(data.get("playtime_active_app_id", 0), 0)),
            created_at=_to_int(data.get("created_at", _now_ts()), _now_ts()),
            updated_at=_to_int(data.get("updated_at", _now_ts()), _now_ts()),
        )


class TianyiStateStore:
    """天翼模块状态存储器。"""

    def __init__(self, state_file: str):
        self._state_file = state_file
        self._lock = threading.RLock()
        self.login = TianyiLoginState()
        self.settings = TianyiSettings()
        self.tasks: List[TianyiTaskRecord] = []
        self.installed_games: List[TianyiInstalledGame] = []
        self.cloud_save_last_result: Dict[str, Any] = {}
        self.cloud_save_restore_last_result: Dict[str, Any] = {}

    @property
    def state_file(self) -> str:
        """返回状态文件路径。"""
        return self._state_file

    def load(self) -> None:
        """从磁盘加载状态。"""
        with self._lock:
            if not os.path.exists(self._state_file):
                return
            with open(self._state_file, "r", encoding="utf-8") as f:
                raw = json.load(f)

            login_raw = raw.get("login") if isinstance(raw, dict) else {}
            settings_raw = raw.get("settings") if isinstance(raw, dict) else {}
            tasks_raw = raw.get("tasks") if isinstance(raw, dict) else []
            installed_raw = raw.get("installed_games") if isinstance(raw, dict) else []
            cloud_save_raw = raw.get("cloud_save_last_result") if isinstance(raw, dict) else {}
            cloud_restore_raw = raw.get("cloud_save_restore_last_result") if isinstance(raw, dict) else {}

            if isinstance(login_raw, dict):
                self.login = TianyiLoginState(
                    cookie=str(login_raw.get("cookie", "")),
                    user_account=str(login_raw.get("user_account", "")),
                    updated_at=_to_int(login_raw.get("updated_at", 0), 0),
                )

            if isinstance(settings_raw, dict):
                self.settings = TianyiSettings(
                    download_dir=str(settings_raw.get("download_dir", "")),
                    install_dir=str(settings_raw.get("install_dir", "")),
                    split_count=max(1, min(64, _to_int(settings_raw.get("split_count", 16), 16))),
                    page_size=max(10, min(200, _to_int(settings_raw.get("page_size", 50), 50))),
                    auto_delete_package=_to_bool(settings_raw.get("auto_delete_package", False), False),
                    auto_install=_to_bool(settings_raw.get("auto_install", True), True),
                )

            next_tasks: List[TianyiTaskRecord] = []
            if isinstance(tasks_raw, list):
                for item in tasks_raw:
                    if not isinstance(item, dict):
                        continue
                    try:
                        record = TianyiTaskRecord.from_dict(item)
                    except Exception:
                        continue
                    if not record.task_id or not record.file_name:
                        continue
                    next_tasks.append(record)
            self.tasks = next_tasks

            next_installed: List[TianyiInstalledGame] = []
            if isinstance(installed_raw, list):
                for item in installed_raw:
                    if not isinstance(item, dict):
                        continue
                    try:
                        record = TianyiInstalledGame.from_dict(item)
                    except Exception:
                        continue
                    if not record.game_title or not record.install_path:
                        continue
                    next_installed.append(record)
            self.installed_games = next_installed

            if isinstance(cloud_save_raw, dict):
                self.cloud_save_last_result = dict(cloud_save_raw)
            else:
                self.cloud_save_last_result = {}

            if isinstance(cloud_restore_raw, dict):
                self.cloud_save_restore_last_result = dict(cloud_restore_raw)
            else:
                self.cloud_save_restore_last_result = {}

    def save(self) -> None:
        """将状态写入磁盘。"""
        with self._lock:
            os.makedirs(os.path.dirname(self._state_file), exist_ok=True)
            payload = {
                "login": asdict(self.login),
                "settings": asdict(self.settings),
                "tasks": [asdict(t) for t in self.tasks],
                "installed_games": [asdict(g) for g in self.installed_games],
                "cloud_save_last_result": dict(self.cloud_save_last_result or {}),
                "cloud_save_restore_last_result": dict(self.cloud_save_restore_last_result or {}),
            }
            tmp_path = f"{self._state_file}.tmp"
            replace_error: Optional[BaseException] = None

            try:
                with open(tmp_path, "w", encoding="utf-8") as f:
                    json.dump(payload, f, ensure_ascii=False, indent=2)
            except Exception:
                # 临时文件写入失败时直接回退到目标文件写入。
                with open(self._state_file, "w", encoding="utf-8") as f:
                    json.dump(payload, f, ensure_ascii=False, indent=2)
                return

            for attempt in range(2):
                try:
                    os.replace(tmp_path, self._state_file)
                    replace_error = None
                    break
                except Exception as exc:
                    replace_error = exc
                    # 某些环境会瞬时返回 "User canceled"，短暂重试一次。
                    if attempt == 0:
                        time.sleep(0.05)
                        continue

            if replace_error is None:
                return

            # rename 仍失败时回退为直接覆盖写入，避免调用链整体失败。
            with open(self._state_file, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
            try:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
            except Exception:
                pass

    def set_login(self, cookie: str, user_account: str) -> None:
        """更新登录态。"""
        with self._lock:
            self.login = TianyiLoginState(
                cookie=cookie.strip(),
                user_account=user_account.strip(),
                updated_at=_now_ts(),
            )
            self.save()

    def clear_login(self) -> None:
        """清除登录态。"""
        with self._lock:
            self.login = TianyiLoginState()
            self.save()

    def set_settings(
        self,
        download_dir: Optional[str] = None,
        install_dir: Optional[str] = None,
        split_count: Optional[int] = None,
        page_size: Optional[int] = None,
        auto_delete_package: Optional[bool] = None,
        auto_install: Optional[bool] = None,
    ) -> None:
        """更新设置。"""
        with self._lock:
            if download_dir is not None:
                self.settings.download_dir = str(download_dir).strip()
            if install_dir is not None:
                self.settings.install_dir = str(install_dir).strip()
            if split_count is not None:
                self.settings.split_count = max(1, min(64, int(split_count)))
            if page_size is not None:
                self.settings.page_size = max(10, min(200, int(page_size)))
            if auto_delete_package is not None:
                self.settings.auto_delete_package = bool(auto_delete_package)
            if auto_install is not None:
                self.settings.auto_install = bool(auto_install)
            self.save()

    def upsert_tasks(self, records: List[TianyiTaskRecord]) -> None:
        """批量新增任务。"""
        with self._lock:
            self.tasks.extend(records)
            self.save()

    def replace_tasks(self, records: List[TianyiTaskRecord]) -> None:
        """整体替换任务列表。"""
        with self._lock:
            self.tasks = list(records)
            self.save()

    def upsert_installed_game(self, record: TianyiInstalledGame) -> None:
        """新增或更新已安装游戏。"""
        with self._lock:
            now = _now_ts()
            target_game_id = str(record.game_id or "").strip()
            target_path = str(record.install_path or "").strip()
            for idx, current in enumerate(self.installed_games):
                same_game = target_game_id and current.game_id == target_game_id
                same_path = target_path and current.install_path == target_path
                if not same_game and not same_path:
                    continue
                next_record = TianyiInstalledGame(
                    game_id=target_game_id or current.game_id,
                    game_title=str(record.game_title or current.game_title),
                    install_path=target_path or current.install_path,
                    source_path=str(record.source_path or current.source_path),
                    status=str(record.status or current.status),
                    size_bytes=max(0, int(record.size_bytes or current.size_bytes or 0)),
                    steam_app_id=max(0, int(record.steam_app_id or current.steam_app_id or 0)),
                    playtime_seconds=max(0, int(record.playtime_seconds or current.playtime_seconds or 0)),
                    playtime_sessions=max(0, int(record.playtime_sessions or current.playtime_sessions or 0)),
                    playtime_last_played_at=max(
                        0,
                        int(record.playtime_last_played_at or current.playtime_last_played_at or 0),
                    ),
                    playtime_active_started_at=max(
                        0,
                        int(record.playtime_active_started_at or current.playtime_active_started_at or 0),
                    ),
                    playtime_active_app_id=max(
                        0,
                        int(record.playtime_active_app_id or current.playtime_active_app_id or 0),
                    ),
                    created_at=current.created_at or now,
                    updated_at=now,
                )
                self.installed_games[idx] = next_record
                self.save()
                return

            new_record = TianyiInstalledGame(
                game_id=target_game_id,
                game_title=str(record.game_title or "未命名游戏"),
                install_path=target_path,
                source_path=str(record.source_path or ""),
                status=str(record.status or "installed"),
                size_bytes=max(0, int(record.size_bytes or 0)),
                steam_app_id=max(0, int(record.steam_app_id or 0)),
                playtime_seconds=max(0, int(record.playtime_seconds or 0)),
                playtime_sessions=max(0, int(record.playtime_sessions or 0)),
                playtime_last_played_at=max(0, int(record.playtime_last_played_at or 0)),
                playtime_active_started_at=max(0, int(record.playtime_active_started_at or 0)),
                playtime_active_app_id=max(0, int(record.playtime_active_app_id or 0)),
                created_at=now,
                updated_at=now,
            )
            self.installed_games.append(new_record)
            self.save()

    def remove_installed_game(
        self,
        *,
        game_id: str = "",
        install_path: str = "",
    ) -> Optional[TianyiInstalledGame]:
        """移除已安装游戏记录并返回被移除项。"""
        target_game_id = str(game_id or "").strip()
        target_install_path = str(install_path or "").strip()
        if not target_game_id and not target_install_path:
            return None

        with self._lock:
            for idx, current in enumerate(self.installed_games):
                same_game = bool(target_game_id and current.game_id == target_game_id)
                same_path = bool(target_install_path and current.install_path == target_install_path)
                if not same_game and not same_path:
                    continue
                removed = self.installed_games.pop(idx)
                self.save()
                return removed
        return None

    def set_cloud_save_last_result(self, result: Optional[Dict[str, Any]]) -> None:
        """更新最近一次云存档上传结果。"""
        with self._lock:
            self.cloud_save_last_result = dict(result or {})
            self.save()

    def set_cloud_save_restore_last_result(self, result: Optional[Dict[str, Any]]) -> None:
        """更新最近一次云存档恢复结果。"""
        with self._lock:
            self.cloud_save_restore_last_result = dict(result or {})
            self.save()

