"""NewFreedeck — Steam 快捷方式导入
使用 vdf 模块正确读写 shortcuts.vdf，不破坏已有条目。"""

import os
import struct
from typing import Any, List, Optional


def _steam_user_dir() -> str:
    home = os.path.expanduser("~")
    return os.path.join(home, ".local", "share", "Steam", "userdata")


def _find_user_dir() -> Optional[str]:
    """找到当前活跃的 Steam userdata 目录（跳过 0/anonymous）。"""
    base = _steam_user_dir()
    if not os.path.isdir(base):
        return None

    # 优先从 loginusers.vdf 读取当前用户
    try:
        steam_root = os.path.dirname(os.path.dirname(base))
        login_path = os.path.join(steam_root, "config", "loginusers.vdf")
        if os.path.isfile(login_path):
            import re
            content = open(login_path, "r", encoding="utf-8").read()
            m = re.search(r'"(\d{17})"\s*\{[^}]*"mostrecent"\s*"1"', content, re.DOTALL)
            if m:
                steamid64 = int(m.group(1))
                acc_id = str(steamid64 - 76561197960265728)
                candidate = os.path.join(base, acc_id)
                if os.path.isdir(candidate):
                    return candidate
    except Exception:
        pass

    # 回退：找非 0 的最大数字目录
    best = None
    best_mtime = 0
    for d in os.listdir(base):
        p = os.path.join(base, d)
        if d in ("0", "anonymous") or not d.isdigit():
            continue
        if os.path.isdir(p):
            try:
                mtime = os.path.getmtime(p)
                if mtime > best_mtime:
                    best_mtime = mtime
                    best = p
            except Exception:
                pass
    return best


def _app_id(exe: str, name: str) -> int:
    """CRC32 生成非 Steam 游戏 AppID。"""
    crc = 0xFFFFFFFF
    data = f"{exe}{name}".encode("utf-8")
    for b in data:
        crc ^= b
        for _ in range(8):
            if crc & 1:
                crc = (crc >> 1) ^ 0xEDB88320
            else:
                crc >>= 1
    return (crc ^ 0xFFFFFFFF) | 0x80000000


def upsert_shortcut(
    app_name: str = "",
    exe: str = "",
    start_dir: str = "",
    launch_options: str = "",
    tags: List[str] = None,
    icon: str = "",
) -> int:
    """创建 Steam 快捷方式。返回 AppID。使用 vdf 模块正确读写。"""
    from vdf import binary_load, binary_dump

    user_dir = _find_user_dir()
    if not user_dir:
        raise RuntimeError("未找到 Steam 用户数据目录")

    appid = _app_id(exe, app_name)
    appid_signed = struct.unpack("i", struct.pack("I", appid & 0xFFFFFFFF))[0]

    config_dir = os.path.join(user_dir, "config")
    os.makedirs(config_dir, exist_ok=True)
    vdf_path = os.path.join(config_dir, "shortcuts.vdf")
    grid_dir = os.path.join(config_dir, "grid")
    os.makedirs(grid_dir, exist_ok=True)

    # 读取已有快捷方式
    try:
        if os.path.exists(vdf_path):
            with open(vdf_path, "rb") as f:
                data = binary_load(f)
        else:
            data = {}
    except Exception:
        data = {}

    shortcuts = data.get("shortcuts", {})
    if not isinstance(shortcuts, dict):
        shortcuts = {}

    # 检查是否已存在（按 appid 匹配）
    existing_idx = None
    for idx, entry in shortcuts.items():
        if isinstance(entry, dict):
            entry_appid = entry.get("appid")
            if entry_appid is not None:
                try:
                    if int(entry_appid) == appid_signed:
                        existing_idx = idx
                        break
                except (TypeError, ValueError):
                    pass

    # 确定索引
    if existing_idx is not None:
        new_idx = existing_idx
    else:
        used = set(int(k) for k in shortcuts.keys() if str(k).lstrip("-").isdigit())
        new_idx = 0
        while new_idx in used:
            new_idx += 1
        new_idx = str(new_idx)

    # 构建条目
    entry = {
        "appid": str(appid_signed),
        "AppName": app_name,
        "Exe": f'"{exe}"',
        "StartDir": start_dir,
        "icon": icon,
        "ShortcutPath": "",
        "LaunchOptions": launch_options,
        "IsHidden": "0",
        "AllowDesktopConfig": "1",
        "AllowOverlay": "1",
        "OpenVR": "0",
        "DevkitGameID": "",
        "Devkit": "1",
        "LastPlayTime": "0",
        "FlatpakAppID": "",
    }
    # tags
    for ti, tval in enumerate(tags or []):
        entry[f"tags/{ti}"] = tval

    shortcuts[str(new_idx)] = entry
    data["shortcuts"] = shortcuts

    # 写入
    with open(vdf_path, "wb") as f:
        binary_dump(data, f)

    return appid
