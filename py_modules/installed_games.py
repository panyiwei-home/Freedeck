"""已安装游戏索引（installed_games.json 持久化 + 迁移 + callable 逻辑）。"""

from __future__ import annotations

import logging
import os
from typing import Optional

_LOGGER = logging.getLogger("NewFreedeck")


# ═══════════════════════════════════════════════
# JSON 持久化
# ═══════════════════════════════════════════════

def _json_path() -> str:
    home = os.environ.get("DECKY_USER_HOME", os.path.expanduser("~"))
    return os.path.join(home, ".local", "share", "NewFreedeck", "installed_games.json")


def load_installed_games() -> list[dict]:
    """读取 installed_games.json，返回游戏列表（不含 _migrated 标记）。"""
    path = _json_path()
    if not os.path.isfile(path):
        return []
    try:
        import json as _json
        with open(path, "r", encoding="utf-8") as f:
            data = _json.load(f)
        return [g for g in data if isinstance(g, dict) and not g.get("_migrated")]
    except Exception:
        return []


def save_installed_games(games: list[dict]):
    """写入 installed_games.json。"""
    import json as _json
    path = _json_path()
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        _json.dump(games, f, ensure_ascii=False, indent=2)


def is_migrated() -> bool:
    """检查是否已完成首次迁移。"""
    path = _json_path()
    if not os.path.isfile(path):
        return False
    try:
        import json as _json
        with open(path, "r", encoding="utf-8") as f:
            data = _json.load(f)
        return any(isinstance(g, dict) and g.get("_migrated") for g in data)
    except Exception:
        return False


def add_installed_game(game: dict):
    """添加一个已安装游戏到 JSON（去重）。"""
    import datetime as _dt
    games = load_installed_games()
    existing = {g.get("game_id"): i for i, g in enumerate(games)}
    game["installed_at"] = _dt.datetime.now().isoformat()
    if game.get("game_id") in existing:
        games[existing[game["game_id"]]] = game
    else:
        games.append(game)
    save_installed_games(games)


def remove_installed_game(game_id: str) -> bool:
    """从 JSON 中移除一个游戏。"""
    games = load_installed_games()
    new_games = [g for g in games if g.get("game_id") != game_id]
    if len(new_games) == len(games):
        return False
    save_installed_games(new_games)
    return True


def validate_games(games: list[dict]) -> list[dict]:
    """过滤掉 install_dir 不存在、为空目录、或已被手动删除的游戏。

    不修改 JSON 文件，仅过滤返回。
    """
    valid = []
    for g in games:
        install_dir = str(g.get("install_dir", ""))
        # 目录不存在 → 跳过
        if not install_dir or not os.path.isdir(install_dir):
            continue
        # 目录为空 → 跳过
        try:
            if not any(os.scandir(install_dir)):
                continue
        except Exception:
            continue
        valid.append(g)
    return valid


# ═══════════════════════════════════════════════
# 首次迁移
# ═══════════════════════════════════════════════

async def migrate_from_shortcuts(
    completed_downloads: list[dict],
    shortcuts: list[dict],
) -> list[dict]:
    """从 shortcuts.vdf + 已完成下载任务中迁移 installed_games.json。

    如果 _downloads 为空（插件重启后），则改为从 shortcuts.vdf
    直接提取安装目录在 FreedeckGames 下的快捷方式。
    """
    migrated = []

    if completed_downloads:
        # 有下载任务 → 精确匹配
        for task in completed_downloads:
            appid = int(task.get("steam_appid", 0) or 0)
            game_title = str(task.get("game_title", ""))
            extract_output = str(task.get("extract_output", ""))
            matched = None

            if appid:
                for sc in shortcuts:
                    if int(sc.get("_appid", 0) or 0) == appid:
                        matched = sc; break

            if not matched and game_title:
                for sc in shortcuts:
                    if str(sc.get("AppName", "")) == game_title:
                        matched = sc; break

            if not matched and extract_output:
                for sc in shortcuts:
                    sc_dir = os.path.dirname(str(sc.get("Exe", "")))
                    if sc_dir and sc_dir.startswith(extract_output):
                        matched = sc; break

            migrated.append({
                "game_id": str(task.get("game_id", "")),
                "title": game_title,
                "steam_appid": appid,
                "install_dir": extract_output,
                "size_bytes": int(task.get("file_size", 0) or 0),
                "cover_url": str(task.get("cover_url", "")),
            })
    else:
        # 无下载任务（插件重启后）→ 从 shortcuts.vdf 中提取
        # 过滤：仅安装目录在 FreedeckGames 下的快捷方式
        home = os.path.expanduser("~")
        freedeck_root = os.path.join(home, "FreedeckGames")
        seen_appids = set()
        for sc in shortcuts:
            appid = int(sc.get("_appid", 0) or 0)
            if not appid or appid in seen_appids:
                continue
            exe = str(sc.get("Exe", ""))
            install_dir = os.path.dirname(exe) if exe else ""
            # 仅过滤 FreedeckGames 目录下的游戏
            if not install_dir or not install_dir.startswith(freedeck_root):
                continue
            seen_appids.add(appid)
            # 估算目录大小
            size_bytes = 0
            try:
                import subprocess
                r = subprocess.run(
                    ["du", "-sb", install_dir],
                    capture_output=True, text=True, timeout=5,
                )
                if r.returncode == 0:
                    size_bytes = int(r.stdout.split()[0])
            except Exception:
                pass
            migrated.append({
                "game_id": str(appid),
                "title": str(sc.get("AppName", "")),
                "steam_appid": appid,
                "install_dir": install_dir,
                "size_bytes": size_bytes,
                "cover_url": "",
            })

    # 保存并写入迁移标记
    migrated.append({"_migrated": True})
    save_installed_games(migrated)
    count = len([g for g in migrated if not g.get("_migrated")])
    _LOGGER.info("installed_games migration: %d games saved", count)
    return [g for g in migrated if not g.get("_migrated")]


def _load_raw() -> list[dict]:
    """读取 installed_games.json 原始数据（含 _migrated 标记）。"""
    path = _json_path()
    if not os.path.isfile(path):
        return []
    try:
        import json as _json
        with open(path, "r", encoding="utf-8") as f:
            return _json.load(f)
    except Exception:
        return []


def _migrate_existing(raw_games: list[dict]) -> list[dict]:
    """将旧版 installed_games.json（无游戏元数据键）转为新版。"""
    fixed = []
    changed = False
    for g in raw_games:
        if isinstance(g, dict):
            fixed.append(g)
        else:
            changed = True
    if changed and fixed:
        save_installed_games(fixed)
    return [g for g in fixed if not g.get("_migrated")]


# ═══════════════════════════════════════════════
# Callable 逻辑
# ═══════════════════════════════════════════════

def find_by_appid(appid: int) -> Optional[dict]:
    """通过 steam_appid 查找已安装游戏（用于云存档自动触发）。

    遍历 installed_games.json，O(n) 时间复杂度。
    游戏数量通常 < 100，性能无影响。
    """
    if not appid:
        return None
    for g in load_installed_games():
        if int(g.get("steam_appid", 0) or 0) == appid:
            return g
    return None


def find_by_game_id(game_id: str) -> Optional[dict]:
    """通过 game_id 查找已安装游戏。"""
    if not game_id:
        return None
    for g in load_installed_games():
        if g.get("game_id") == game_id:
            return g
    return None


def get_managed_game_ids() -> set[str]:
    """获取所有通过插件安装的 game_id 集合（用于云存档下载过滤）。"""
    return {g["game_id"] for g in load_installed_games() if g.get("game_id")}


async def handle_list_installed_games(
    plugin_instance,  # Plugin class instance for _downloads
    _load_steam_shortcuts_fn,
) -> dict:
    """list_installed_games callable 逻辑。"""
    _LOGGER.info("Decky callable: list_installed_games")

    if not is_migrated():
        _LOGGER.info("list_installed_games: running first-time migration")
        try:
            import asyncio
            shortcuts = await asyncio.to_thread(_load_steam_shortcuts_fn)
            completed = [
                t for t in plugin_instance._downloads.values()
                if t.get("import_status") == "complete" and t.get("steam_appid")
            ]
            games = await migrate_from_shortcuts(completed, shortcuts)
        except Exception as e:
            _LOGGER.warning("list_installed_games: migration failed: %s", e)
            games = load_installed_games()
    else:
        games = load_installed_games()

    # 过滤无效目录（已被手动删除的）
    games = validate_games(games)

    return {"status": "success", "data": {"games": games}}


async def handle_uninstall_game(
    game_id: str,
) -> dict:
    """uninstall_game callable 逻辑。"""
    if not game_id:
        return {"status": "error", "message": "缺少 game_id 参数"}

    _LOGGER.info("Decky callable: uninstall_game game_id=%s", game_id)
    games = load_installed_games()
    target = next((g for g in games if g.get("game_id") == game_id), None)
    if not target:
        return {"status": "error", "message": "未找到该游戏记录"}

    install_dir = str(target.get("install_dir", ""))
    if install_dir and os.path.isdir(install_dir):
        try:
            import shutil
            import asyncio
            await asyncio.to_thread(shutil.rmtree, install_dir)
            _LOGGER.info("uninstall_game: removed dir %s", install_dir)
        except Exception as e:
            _LOGGER.warning("uninstall_game: failed to remove dir %s: %s", install_dir, e)

    remove_installed_game(game_id)
    return {"status": "success", "data": {"ok": True}}