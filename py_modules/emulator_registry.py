"""NewFreedeck — 模拟器下载源注册表（emu.json 拉取 + 缓存）"""

import json
import os
import time as _time
from typing import Dict, Any, Optional

# ── 配置 ──────────────────────────────

EMU_JSON_URL = "http://panyiwei.xyz/freedeck-catalog/emu.json"
EMU_CACHE_SECONDS = 600
EMU_HTTP_TIMEOUT = 12.0

# ── 模拟器平台映射 ────────────────────

EMULATOR_PLATFORMS: Dict[str, Dict[str, str]] = {
    "switch":    {"id": "switch",  "display": "Switch",  "adapter": "eden",       "icon": "switch-yuzu.svg",     "game_id": "switch_emulator"},
    "ps1":       {"id": "ps1",     "display": "PS1",     "adapter": "duckstation", "icon": "ps1-duckstation.svg",  "game_id": "ps1_emulator"},
    "ps2":       {"id": "ps2",     "display": "PS2",     "adapter": "pcsx2",       "icon": "ps2-pcsx2.svg",       "game_id": "ps2_emulator"},
    "ps3":       {"id": "ps3",     "display": "PS3",     "adapter": "rpcs3",        "icon": "ps3-rpcs3.svg",       "game_id": "ps3_emulator"},
    "psp":       {"id": "psp",     "display": "PSP",     "adapter": "ppsspp",       "icon": "psp-ppsspp.png",      "game_id": "psp_emulator"},
    "3ds":       {"id": "3ds",     "display": "3DS",     "adapter": "lime3ds",      "icon": "3ds-lime.svg",        "game_id": "3ds_emulator"},
    "gba":       {"id": "gba",     "display": "GBA",     "adapter": "mgba",         "icon": "gba-mgba.png",        "game_id": "gba_emulator"},
    "nds":       {"id": "nds",     "display": "NDS",     "adapter": "melonds",      "icon": "nds.svg",             "game_id": "nds_emulator"},
    "fc":        {"id": "fc",      "display": "FC",      "adapter": "nestopia",     "icon": "fc-nestopia.svg",     "game_id": "fc_emulator"},
    "md":        {"id": "md",      "display": "MD",      "adapter": "blastem",      "icon": "md-blastem-og.png",   "game_id": "md_emulator"},
}

PLATFORM_BY_ADAPTER: Dict[str, str] = {
    v["adapter"]: k for k, v in EMULATOR_PLATFORMS.items()
}

# ── 全局缓存 ──────────────────────────

_cache: Dict[str, Any] = {}
_cache_file: Optional[str] = None


def _state_dir() -> str:
    home = os.path.expanduser("~")
    d = os.path.join(home, ".local", "share", "NewFreedeck")
    os.makedirs(d, exist_ok=True)
    return d


def init_cache() -> None:
    global _cache, _cache_file
    _cache_file = os.path.join(_state_dir(), "emu_cache.json")
    if os.path.exists(_cache_file):
        try:
            with open(_cache_file, "r") as f:
                _cache = json.load(f)
        except Exception:
            _cache = {}


def _save_cache() -> None:
    if _cache_file:
        try:
            with open(_cache_file, "w") as f:
                json.dump(_cache, f, indent=2, ensure_ascii=False)
        except Exception:
            pass


def _now_ts() -> int:
    return int(_time.time())


def _parse_json(data) -> Dict[str, Dict[str, Any]]:
    """解析 emu.json → {name: {share_url, pwd, file_name}}"""
    if isinstance(data, str):
        data = json.loads(data)
    if not isinstance(data, dict):
        return {}
    entries: Dict[str, Dict[str, Any]] = {}
    for name, info in data.items():
        name = str(name or "").strip().lower()
        link = str(info.get("share_url", info.get("link", "")) or "").strip()
        code = str(info.get("pwd", info.get("code", "")) or "").strip()
        if not name or not link:
            continue
        entries[name] = {
            "share_url": link,
            "pwd": code,
            "file_name": str(info.get("file_name", "") or ""),
        }
        # alias by adapter
        plat = EMULATOR_PLATFORMS.get(name)
        if plat:
            ad = plat["adapter"]
            if ad not in entries:
                entries[ad] = entries[name]
    return entries


def fetch_emulator_sources(force: bool = False) -> Dict[str, Dict[str, Any]]:
    """返回 {name: {share_url, pwd, file_name}}，优先缓存。
    首次加载时如果网络不通，立即返回空字典（不阻塞 UI）。"""
    now = _now_ts()
    if _cache.get("entries") and not force:
        age = now - max(0, int(_cache.get("fetched_at", 0)))
        if age < EMU_CACHE_SECONDS:
            return dict(_cache["entries"])

    # 无缓存时，尝试快速获取；失败则返回空
    import urllib.request as _req
    raw_data = None
    try:
        req = _req.Request(EMU_JSON_URL, headers={
            "Accept": "application/json, */*",
            "User-Agent": "Mozilla/5.0 (NewFreedeck/1.0)",
        })
        with _req.urlopen(req, timeout=max(3, EMU_HTTP_TIMEOUT)) as resp:
            raw = resp.read()
            raw_data = json.loads(raw.decode("utf-8", errors="replace"))
    except Exception:
        pass

    if not raw_data:
        if _cache.get("entries"):
            return dict(_cache["entries"])
        return {}  # 不抛异常，返回空让 UI 正常渲染

    entries = _parse_json(raw_data)
    if not entries:
        if _cache.get("entries"):
            return dict(_cache["entries"])
        return {}  # 解析失败也返回空，不阻塞 UI

    _cache["entries"] = entries
    _cache["fetched_at"] = now
    _save_cache()
    return dict(entries)


def get_emulator_source(platform: str) -> Dict[str, Any]:
    """获取单个模拟器的下载源 {share_url, pwd, file_name}"""
    plat = EMULATOR_PLATFORMS.get(platform)
    if not plat:
        raise ValueError(f"未知平台: {platform}")
    entries = fetch_emulator_sources()
    # 尝试多个键名
    for key in (platform, plat["adapter"], plat["game_id"], plat["display"].lower()):
        e = entries.get(key)
        if e:
            return e
    raise ValueError(f"emu.json 未找到 {platform} ({plat['display']}) 的下载源")
