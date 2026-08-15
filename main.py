"""NewFreedeck."""
import asyncio, base64, contextlib, ctypes, fcntl, json, os, re, sys, tempfile, traceback, time, uuid
from typing import Optional
import decky

# ── Rust 扩展（可选加载）──────────────────

_core = None
try:
    _plugin_dir = os.path.dirname(os.path.abspath(__file__))
    _so_path = os.path.join(_plugin_dir, "py_modules", "libnewfreedeck_core.so")
    if os.path.exists(_so_path):
        import ctypes
        _core = ctypes.CDLL(_so_path)

        # 设置函数签名
        _core.nfc_load.argtypes = [ctypes.c_char_p, ctypes.c_size_t]
        _core.nfc_load.restype = None

        _core.nfc_query.argtypes = [ctypes.c_uint32, ctypes.c_uint32, ctypes.c_char_p]
        _core.nfc_query.restype = ctypes.c_void_p

        _core.nfc_sign.argtypes = [ctypes.c_char_p]*4 + [ctypes.c_char_p, ctypes.c_uint32]
        _core.nfc_sign.restype = ctypes.c_int32

        _core.nfc_md5.argtypes = [ctypes.c_char_p]
        _core.nfc_md5.restype = ctypes.c_void_p

        _core.nfc_free.argtypes = [ctypes.c_void_p]
        _core.nfc_free.restype = None

        # 下载引擎
        _core.nfc_download_init.argtypes = [ctypes.c_char_p, ctypes.c_char_p]
        _core.nfc_download_init.restype = ctypes.c_int
        _core.nfc_download_start.argtypes = [ctypes.c_char_p]*4
        _core.nfc_download_start.restype = ctypes.c_char_p
        _core.nfc_download_poll.argtypes = [ctypes.c_char_p]
        _core.nfc_download_poll.restype = ctypes.c_char_p
        _core.nfc_download_cancel.argtypes = [ctypes.c_char_p]
        _core.nfc_download_cancel.restype = ctypes.c_int

        # 解压引擎
        _core.nfc_extract_init.argtypes = [ctypes.c_char_p]
        _core.nfc_extract_init.restype = ctypes.c_int
        _core.nfc_extract_detect.argtypes = [ctypes.c_char_p]
        _core.nfc_extract_detect.restype = ctypes.c_void_p
        _core.nfc_extract_start.argtypes = [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p]
        _core.nfc_extract_start.restype = ctypes.c_void_p
        _core.nfc_extract_poll.argtypes = [ctypes.c_char_p]
        _core.nfc_extract_poll.restype = ctypes.c_void_p
        _core.nfc_extract_cancel.argtypes = [ctypes.c_char_p]
        _core.nfc_extract_cancel.restype = ctypes.c_int
        _core.nfc_extract_list.argtypes = [ctypes.c_char_p, ctypes.c_char_p]
        _core.nfc_extract_list.restype = ctypes.c_void_p

        # Steam 导入引擎
        _core.nfc_steam_add_shortcut.argtypes = [ctypes.c_char_p]
        _core.nfc_steam_add_shortcut.restype = ctypes.c_void_p
        _core.nfc_steam_discover_exe.argtypes = [ctypes.c_char_p, ctypes.c_char_p]
        _core.nfc_steam_discover_exe.restype = ctypes.c_void_p
        _core.nfc_steam_get_steam_id.argtypes = []
        _core.nfc_steam_get_steam_id.restype = ctypes.c_void_p

        decky.logger.info("Rust core loaded: nfc_load nfc_query nfc_sign nfc_md5 + download + extract + steam")
except Exception as e:
    decky.logger.warning("Rust core not loaded: %s", e)

# 下载引擎（Rust 优先，Python 回退）
from download_engine import DownloadEngine

_rust_download = bool(_core)
if _rust_download:
    decky.logger.info("download engine: Rust")
else:
    decky.logger.info("download engine: Python fallback")

# ── SteamGridDB ───────────────────────────

SGDB_API_KEY = "3d057a658242e107af945bff275c9406"
SGDB_API_BASE = "https://www.steamgriddb.com/api/v2"
DEFAULT_SHORTCUT_COMPAT_TOOL = "proton_experimental"
LOGIN_CACHE_TTL_SECONDS = 15.0

# ── JSON 响应辅助 ────────────────────────

def _ok(data=None):
    return {"status": "success", "data": data or {}}

def _err(message: str, data=None):
    return {"status": "error", "message": message, "data": data or {}}


def _split_archive_base(filename: str) -> str:
    """检测分卷压缩包，返回基础名（不含分卷后缀）。

    匹配模式:
      game.7z.001 / game.7z.002 → "game.7z"
      game.part1.rar / game.part2.rar → "game"
      game.zip.001 → "game.zip"
      game.001 / game.002 → "game"

    如果不是分卷包，返回空字符串。
    """
    import re
    name = str(filename or "")
    # 模式 1: .xxx.001, .xxx.002 ... (例如 .7z.001, .zip.001, .rar.001)
    m = re.match(r"^(.+\.(?:7z|zip|rar|iso|gz|bz2|xz))\.(\d{2,4})$", name, re.I)
    if m:
        return m.group(1)
    # 模式 2: .partN.rar, .partNN.rar
    m = re.match(r"^(.+)\.part(\d+)\.(rar|7z|zip)$", name, re.I)
    if m:
        return m.group(1)
    # 模式 3: .rNN, .NNN (WinRAR 分卷)
    m = re.match(r"^(.+)\.r(\d{2,4})$", name, re.I)
    if m:
        return m.group(1)
    # 模式 4: .001, .002 (无扩展名分卷)
    m = re.match(r"^(.+)\.(\d{3,4})$", name)
    if m:
        base = m.group(1)
        ext = m.group(2)
        # 排除版本号：.v001 或 .v1 不是分卷
        if not re.search(r"\.v\d+$", base, re.I):
            # 检查是否有同名的非分卷文件
            return base
    return ""


# ── 状态目录 ─────────────────────────────

def _try_int(val, default=0):
    try: return int(val)
    except (TypeError, ValueError): return default


# ── 游戏分组合并 ────────────────────────

def _normalize_title_key(raw: str) -> str:
    """构造稳定分组键。去掉版本后缀，提取核心英文名或中文标题。"""
    raw = re.sub(r'\s+', ' ', str(raw or '')).strip()
    if not raw:
        return ''
    parts = [p.strip() for p in re.split(r'[/|｜]', raw) if p.strip()]
    eng_parts = [p for p in parts if re.search(r'[A-Za-z]', p)]
    eng = eng_parts[-1] if eng_parts else ''
    if eng and len(eng) > 6:
        key = re.sub(r'[\u2010-\u2015]', '-', eng)
        key = re.sub(r'[^A-Za-z0-9]+', ' ', key).strip().lower()
        key = re.sub(r'\b(digital\s+deluxe|deluxe|gold|ultimate|complete|definitive|collector|premium)\s+edition\b', '', key)
        key = re.sub(r'\s+edition\b', '', key)
        return re.sub(r'\s+', ' ', key).strip()
    cn = parts[0] if parts else raw
    key = re.sub(r'\s+', ' ', cn)
    key = re.sub(r'(?:\s*[（(【\[].*?[）)】\]])+$', '', key).strip()
    key = re.sub(r'(数字豪华版|豪华版|黄金版|终极版|完整版|完全版|决定版|年度版|传奇版|典藏版|加强版)\s*$', '', key).strip()
    return key.lower() if key else ''


def _has_disc_marker(text: str) -> bool:
    """检测文本中是否含有多光盘标记。"""
    return bool(re.search(
        r'(disc|disk|cd|dvd|umd|[ABCD一二三四]盘)',
        str(text or ''),
        re.IGNORECASE
    ))


def _extract_disc_order(text: str) -> int:
    """从文本中提取光盘序号（用于多盘排序）。"""
    t = str(text or '').strip()
    # Disc 1 / Disk 2 / CD 3 / DVD 4 / UMD 5
    m = re.search(r'(?:disc|disk|cd|dvd|umd)[\s._-]*(\d+)', t, re.IGNORECASE)
    if m:
        return int(m.group(1))
    # A盘 / B盘 / C盘 / D盘
    m = re.search(r'([ABCDabcd])盘', t)
    if m:
        return ord(m.group(1).upper()) - 64
    # 一盘 / 二盘 / 三盘 / 四盘
    cn_map = {'一': 1, '二': 2, '三': 3, '四': 4}
    m = re.search(r'([一二三四])盘', t)
    if m:
        return cn_map.get(m.group(1), 999)
    return 999


def _group_catalog_items(items: list[dict]) -> list[dict]:
    """将同一游戏的不同版本合并为一条记录（variants 数组）。"""
    groups: dict[str, list[dict]] = {}
    for g in items:
        plat = str(g.get('platform', '') or '').strip().lower() or 'unknown'
        title_key = str(g.get('title_zh', '') or g.get('title_en', '') or g.get('title', '')).strip()
        key = f'{plat}:{_normalize_title_key(title_key)}'
        if not key:
            key = f'{plat}:misc:{g.get("game_id", "")}'
        groups.setdefault(key, []).append(g)

    result: list[dict] = []
    for _key, variants in groups.items():
        if len(variants) == 1:
            result.append(variants[0])
            continue

        # 排序：多光盘优先、高级版沉底、大体积优先
        def _sort_key(g):
            title = str(g.get('title', ''))
            version = str(g.get('version', ''))
            ver_match = re.search(r'v?(\d+(?:[._-]\d+)*)', version or title)
            ver_parts = tuple(
                -int(p) for p in
                (ver_match.group(1).replace('.', ' ').replace('_', ' ').replace('-', ' ').split())
                if p.isdigit()
            ) if ver_match else ()
            tlower = title.lower()
            is_premium = 1 if re.search(
                r'(豪华|deluxe|gold|ultimate|complete|collector|premium|definitive|传奇|典藏|年度|加强)',
                tlower
            ) else 0
            disc_order = _extract_disc_order(title)
            disc_order = disc_order if disc_order != 999 else _extract_disc_order(str(g.get('version', '')))
            return (
                0 if disc_order != 999 else 1,  # 有光盘号优先
                disc_order,
                0 if ver_parts else 1,
                ver_parts,
                is_premium,
                len(title),
                -_try_int(g.get('size_bytes')),
            )
        variants.sort(key=_sort_key)

        primary = variants[0].copy()
        primary['grouped'] = True
        primary['variant_count'] = len(variants)
        primary['variants'] = variants
        # 多光盘检测
        disc_count = sum(1 for v in variants if _has_disc_marker(
            str(v.get('title', '')) + str(v.get('version', ''))
        ))
        primary['multi_disc'] = disc_count > 1
        primary['disc_count'] = disc_count if disc_count > 1 else 1
        result.append(primary)

    return result


# steamID64 → steamID3 (AccountID) 之差
STEAM_ID64_BASE = 76561197960265728

def _state_dir():
    home = os.environ.get("DECKY_USER_HOME", os.path.expanduser("~"))
    return os.path.join(home, ".local", "share", "NewFreedeck")

import logging
import logging.handlers

def _setup_plugin_log(state_dir: str) -> None:
    """将所有 decky.logger 输出双写至 plugin.log（每行即时刷盘）。

    文件路径: ~/.local/share/NewFreedeck/plugin.log
    限制: 单文件 5 MiB，保留 2 个备份（总计 15 MiB）。
    """
    log_path = os.path.join(state_dir, "plugin.log")
    try:
        handler = logging.handlers.RotatingFileHandler(
            log_path,
            maxBytes=5 * 1024 * 1024,
            backupCount=2,
            encoding="utf-8",
        )
        handler.setFormatter(logging.Formatter(
            fmt="%(asctime)s [%(levelname)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        ))
        handler.setLevel(logging.DEBUG)
        # 让每条日志立即刷入磁盘，避免缓冲丢失
        try:
            handler.stream.reconfigure(write_through=True)
        except Exception:
            pass

        root_logger = logging.getLogger()
        root_logger.addHandler(handler)
        decky.logger.info("plugin log file: %s", log_path)
    except Exception as e:
        decky.logger.warning("failed to setup plugin log file: %s", e)


def _tail_file(path: str, n: int) -> str:
    """读取文件尾部 N 行（高效，不将整个文件读入内存）。"""
    import collections
    with open(path, "rb") as f:
        # 从文件末尾向前扫描换行符
        f.seek(0, 2)  # EOF
        file_size = f.tell()
        if file_size == 0:
            return ""
        chunk_size = 4096
        lines_found: list[bytes] = []
        pos = file_size
        while pos > 0 and len(lines_found) <= n:
            read_size = min(chunk_size, pos)
            pos -= read_size
            f.seek(pos)
            chunk = f.read(read_size)
            # 在 chunk 中拆分行（反向处理）
            if pos == 0:
                lines_found[0:0] = chunk.split(b"\n")
            else:
                # 第一个元素可能是部分行，需要与之前的数据拼接
                if lines_found:
                    lines_found[0] = chunk.split(b"\n")[-1] + lines_found[0]
                    lines_found[0:0] = chunk.split(b"\n")[:-1]
                else:
                    lines_found[0:0] = chunk.split(b"\n")
        # 返回最后 n 行
        result = b"\n".join(lines_found[-n:]).decode("utf-8", errors="replace")
        return result

def _find_steam_userdata_dir() -> str:
    """返回当前登录 Steam 用户的 userdata 目录。

    处理 steamID64→AccountID 转换，含目录验证和回退扫描。
    不依赖 Rust FFI — 直接读 loginusers.vdf（文本 VDF）。
    """
    home = os.path.expanduser("~")
    steam_root = os.path.join(home, ".local", "share", "Steam")
    userdata_base = os.path.join(steam_root, "userdata")

    # 1. 尝试从 loginusers.vdf 读取当前活跃用户（文本 VDF，非二进制）
    login_path = os.path.join(steam_root, "config", "loginusers.vdf")
    if os.path.isfile(login_path):
        try:
            content = open(login_path, "r", encoding="utf-8").read()
            # 简单解析：找 mostrecent=1 的用户 ID（SteamID64, 17位数字）
            import re
            # 匹配块: "7656119XXXXXXXXXX" { ... "mostrecent" ... "1" ... }
            pattern = r'"(\d{17})"\s*\{([^}]*)"mostrecent"\s*"1"'
            m = re.search(pattern, content, re.DOTALL)
            if m:
                steamid64 = m.group(1)
                acc_id = str(int(steamid64) - STEAM_ID64_BASE)
                candidate = os.path.join(userdata_base, acc_id)
                if os.path.isdir(candidate):
                    return candidate
        except Exception:
            pass

    # 2. 回退：扫描 userdata/ 找非 0 的子目录（按修改时间降序）
    if os.path.isdir(userdata_base):
        entries = []
        for name in os.listdir(userdata_base):
            if name in ("0", "anonymous"):
                continue
            p = os.path.join(userdata_base, name)
            if os.path.isdir(p):
                entries.append((os.path.getmtime(p), p))
        if entries:
            entries.sort(reverse=True)
            return entries[0][1]

    # 3. 最终回退
    return userdata_base

def _url_encode_rfc3986(s: str) -> str:
    """URL 编码（RFC 3986），保留安全字符"""
    import urllib.parse
    return urllib.parse.quote(s, safe="")

def _wrap_text(text: str, max_chars: int) -> list:
    """将长文本按 max_chars 拆分为多行"""
    if len(text) <= max_chars:
        return [text]
    lines = []
    current = ""
    for ch in text:
        if len(current) >= max_chars:
            lines.append(current)
            current = ""
        current += ch
    if current:
        lines.append(current)
    return lines


def _safe_path_component(value: str, fallback: str = "item") -> str:
    """清洗路径片段，避免非法字符和过长目录名。"""
    import re
    text = str(value or "").strip().replace("\\", "/").split("/")[-1]
    text = re.sub(r'[\x00-\x1f<>:"|?*]+', " ", text)
    text = re.sub(r"\s+", " ", text).strip(" .")
    return text[:80] or fallback


def _archive_display_name(path_or_name: str) -> str:
    """从归档路径推导稳定的展示名。"""
    name = os.path.basename(str(path_or_name or ""))
    split_base = _split_archive_base(name)
    if split_base:
        name = split_base
    known_exts = {".7z", ".zip", ".rar", ".tar", ".gz", ".bz2", ".xz", ".iso"}
    while True:
        stem, ext = os.path.splitext(name)
        if not ext or ext.lower() not in known_exts:
            break
        name = stem
    return name or os.path.basename(str(path_or_name or ""))


def _build_extract_dir_name(task: dict, archive_file: str) -> str:
    """生成英文安装目录名（不用 game_id 命名）。

    优先级: 压缩包英文名 > 英文标题 (title_en) > game_id 兜底
    避免中文目录名以确保模拟器/Proton 兼容性。
    """
    archive_name = _archive_display_name(archive_file)
    title_en = str(task.get("game_title_en", "") or "").strip()

    # 1. 压缩包名是纯 ASCII（英文/数字）→ 直接使用
    if archive_name and all(ord(c) < 128 for c in archive_name) and not archive_name.isdigit():
        return _safe_path_component(archive_name, fallback="game")

    # 2. 英文标题
    if title_en and all(ord(c) < 128 for c in title_en):
        return _safe_path_component(title_en, fallback="game")

    # 3. 兜底 game_id
    game_id = str(task.get("game_id", "") or "").strip()
    if game_id:
        return _safe_path_component(game_id, fallback="game")

    return _safe_path_component(archive_name, fallback="game")


def _normalize_shortcut_path(value: str) -> str:
    text = str(value or "").strip().strip('"')
    if not text:
        return ""
    return os.path.normcase(os.path.normpath(text))


def _shortcut_runtime_appid(shortcut: dict) -> int:
    return (int(shortcut.get("appid", 0)) & 0xffffffff) | 0x80000000


def _shortcut_display_name(shortcut: dict) -> str:
    return str(shortcut.get("AppName", "") or "").strip()


def _resolve_shortcut_duplicates(shortcuts_path: str, exe_path: str, desired_title: str = "") -> dict | None:
    """查找并收敛同一 exe 的 shortcut，返回首选条目。"""
    if not shortcuts_path or not os.path.exists(shortcuts_path):
        return None

    from vdf import binary_dump, binary_load

    normalized_exe = _normalize_shortcut_path(exe_path)
    if not normalized_exe:
        return None

    last_error = None
    data = None
    for attempt in range(3):
        try:
            with open(shortcuts_path, "rb") as fh:
                data = binary_load(fh)
            break
        except Exception as e:
            last_error = e
            if attempt < 2:
                time.sleep(0.1)
    if data is None:
        raise last_error or RuntimeError("read shortcuts.vdf failed")

    shortcuts = data.get("shortcuts", {})
    matches: list[dict] = []
    exe_basename = os.path.basename(normalized_exe).lower()
    exe_stem = os.path.splitext(exe_basename)[0]
    desired_title_lower = str(desired_title or "").strip().lower()
    expected_start_dir = _normalize_shortcut_path(os.path.dirname(normalized_exe))

    for key, shortcut in shortcuts.items():
        if not isinstance(shortcut, dict):
            continue
        if _normalize_shortcut_path(shortcut.get("Exe", "")) != normalized_exe:
            continue
        app_name = _shortcut_display_name(shortcut)
        app_name_lower = app_name.lower()
        score = 0
        if desired_title_lower and app_name_lower == desired_title_lower:
            score += 50
        if app_name and app_name_lower not in {exe_basename, exe_stem}:
            score += 20
        if _normalize_shortcut_path(shortcut.get("StartDir", "")) == expected_start_dir:
            score += 10
        matches.append({
            "index": str(key),
            "shortcut": shortcut,
            "score": score,
        })

    if not matches:
        return None

    matches.sort(key=lambda item: (-int(item["score"]), int(item["index"]) if str(item["index"]).isdigit() else 10**9))
    preferred = matches[0]
    preferred_shortcut = preferred["shortcut"]
    changed = False

    if desired_title and _shortcut_display_name(preferred_shortcut) != desired_title:
        preferred_shortcut["AppName"] = desired_title
        changed = True

    removed = 0
    if len(matches) > 1:
        keep_index = preferred["index"]
        new_shortcuts = {}
        next_index = 0
        for key, shortcut in shortcuts.items():
            if (
                isinstance(shortcut, dict)
                and _normalize_shortcut_path(shortcut.get("Exe", "")) == normalized_exe
                and str(key) != keep_index
            ):
                removed += 1
                continue
            new_shortcuts[str(next_index)] = shortcut
            next_index += 1
        data["shortcuts"] = new_shortcuts
        changed = True

    if changed:
        with open(shortcuts_path, "wb") as fh:
            binary_dump(data, fh)

    return {
        "appid": _shortcut_runtime_appid(preferred_shortcut),
        "app_name": _shortcut_display_name(preferred_shortcut),
        "duplicates_removed": removed,
    }


def _normalize_image_format(value: str) -> str:
    fmt = str(value or "").strip().lower().lstrip(".")
    if fmt == "jpeg":
        return "jpg"
    if fmt in {"png", "jpg", "webp", "gif"}:
        return fmt
    return ""


def _guess_image_format(url: str = "", content_type: str = "", data: bytes = b"") -> str:
    import urllib.parse

    ctype = str(content_type or "").lower()
    if "png" in ctype:
        return "png"
    if "jpeg" in ctype or "jpg" in ctype:
        return "jpg"
    if "webp" in ctype:
        return "webp"
    if "gif" in ctype:
        return "gif"

    path = urllib.parse.urlparse(str(url or "")).path
    ext = _normalize_image_format(os.path.splitext(path)[1])
    if ext:
        return ext

    if data.startswith(b"\x89PNG\r\n\x1a\n"):
        return "png"
    if data[:3] == b"\xff\xd8\xff":
        return "jpg"
    if data.startswith(b"RIFF") and data[8:12] == b"WEBP":
        return "webp"
    if data[:6] in (b"GIF87a", b"GIF89a"):
        return "gif"
    return "png"


def _write_grid_artwork(grid_dir: str, appid: int, stem: str, data: bytes, image_format: str) -> str:
    os.makedirs(grid_dir, exist_ok=True)
    normalized = _normalize_image_format(image_format) or "png"
    for ext in ("png", "jpg", "webp", "gif"):
        stale_path = os.path.join(grid_dir, f"{appid}{stem}.{ext}")
        if ext != normalized and os.path.exists(stale_path):
            try:
                os.remove(stale_path)
            except OSError:
                pass
    out_path = os.path.join(grid_dir, f"{appid}{stem}.{normalized}")
    with open(out_path, "wb") as fh:
        fh.write(data)
    return out_path


def _http_ssl_context():
    """为 aiohttp/urllib 提供稳定的 CA 证书上下文。"""
    import ssl
    try:
        import certifi
        return ssl.create_default_context(cafile=certifi.where())
    except Exception:
        return ssl.create_default_context()


def _steam_root_from_userdata_dir(userdata_dir: str) -> str:
    normalized = os.path.realpath(os.path.expanduser(str(userdata_dir or "").strip()))
    if not normalized:
        return ""
    return os.path.dirname(os.path.dirname(normalized))


def _steam_config_vdf_path_from_userdata_dir(userdata_dir: str) -> str:
    steam_root = _steam_root_from_userdata_dir(userdata_dir)
    if not steam_root:
        return ""
    return os.path.join(steam_root, "config", "config.vdf")


def _lock_file_path(path: str) -> str:
    normalized = os.path.realpath(os.path.expanduser(str(path or "").strip()))
    directory = os.path.dirname(normalized)
    basename = os.path.basename(normalized)
    return os.path.join(directory, f".newfreedeck-lock-{basename}")


@contextlib.contextmanager
def _exclusive_file_lock(path: str):
    lock_path = _lock_file_path(path)
    os.makedirs(os.path.dirname(lock_path), exist_ok=True)
    with open(lock_path, "a+", encoding="utf-8") as fh:
        fcntl.flock(fh.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(fh.fileno(), fcntl.LOCK_UN)


def _atomic_write_text(path: str, content: str) -> None:
    target = os.path.realpath(os.path.expanduser(str(path or "").strip()))
    directory = os.path.dirname(target)
    os.makedirs(directory, exist_ok=True)

    mode = 0o644
    try:
        mode = int(os.stat(target).st_mode) & 0o777
    except Exception:
        pass

    fd, temp_path = tempfile.mkstemp(prefix=".newfreedeck-", suffix=".tmp", dir=directory)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(str(content or ""))
            fh.flush()
            os.fsync(fh.fileno())
        os.chmod(temp_path, mode)
        os.replace(temp_path, target)
    except Exception:
        try:
            os.remove(temp_path)
        except OSError:
            pass
        raise


def _compat_mapping_entry_text(app_id: int, compat_tool: str) -> str:
    app_id_unsigned = int(app_id) & 0xFFFFFFFF
    tool_name = str(compat_tool or "").strip() or DEFAULT_SHORTCUT_COMPAT_TOOL
    return (
        f'\n\t\t\t\t\t"{app_id_unsigned}"\n'
        f"\t\t\t\t\t{{\n"
        f'\t\t\t\t\t\t"name"\t\t"{tool_name}"\n'
        f'\t\t\t\t\t\t"config"\t\t""\n'
        f'\t\t\t\t\t\t"priority"\t\t"250"\n'
        f"\t\t\t\t\t}}"
    )


def _set_shortcut_compat_tool(userdata_dir: str, app_id: int, compat_tool: str = DEFAULT_SHORTCUT_COMPAT_TOOL) -> dict:
    import re

    config_path = _steam_config_vdf_path_from_userdata_dir(userdata_dir)
    if not config_path or not os.path.isfile(config_path):
        return {"ok": False, "changed": False, "message": f"config.vdf 不存在: {config_path or '(empty)'}"}

    app_id_unsigned = int(app_id) & 0xFFFFFFFF
    app_id_str = str(app_id_unsigned)
    tool_name = str(compat_tool or "").strip() or DEFAULT_SHORTCUT_COMPAT_TOOL

    try:
        with _exclusive_file_lock(config_path):
            with open(config_path, "r", encoding="utf-8", errors="ignore") as fh:
                content = fh.read()

            marker = '"CompatToolMapping"'
            marker_pos = content.find(marker)
            compat_entry = _compat_mapping_entry_text(app_id_unsigned, tool_name)

            if marker_pos >= 0:
                brace_pos = content.find("{", marker_pos)
                if brace_pos < 0:
                    return {"ok": False, "changed": False, "message": "CompatToolMapping 结构异常"}

                entry_pattern = re.compile(rf'\s*"{re.escape(app_id_str)}"\s*\{{[^}}]*\}}', re.DOTALL)
                if entry_pattern.search(content):
                    updated = entry_pattern.sub(compat_entry, content, count=1)
                else:
                    updated = content[: brace_pos + 1] + compat_entry + content[brace_pos + 1 :]
            else:
                steam_marker = '"Steam"'
                steam_pos = content.find(steam_marker)
                if steam_pos < 0:
                    return {"ok": False, "changed": False, "message": 'config.vdf 中缺少 "Steam" 配置块'}
                steam_brace_pos = content.find("{", steam_pos)
                if steam_brace_pos < 0:
                    return {"ok": False, "changed": False, "message": '"Steam" 配置块结构异常'}
                compat_block = (
                    '\n\t\t\t\t"CompatToolMapping"\n'
                    '\t\t\t\t{'
                    f"{compat_entry}\n"
                    '\t\t\t\t}'
                )
                updated = content[: steam_brace_pos + 1] + compat_block + content[steam_brace_pos + 1 :]

            if updated == content:
                return {"ok": True, "changed": False, "compat_tool": tool_name, "message": "兼容层映射保持不变"}

            _atomic_write_text(config_path, updated)
    except Exception as e:
        return {"ok": False, "changed": False, "compat_tool": tool_name, "message": f"写入兼容层映射失败: {e}"}

    return {"ok": True, "changed": True, "compat_tool": tool_name, "message": f"已设置兼容层: {tool_name}"}


# ── PC 游戏存档扫描辅助 ──────────────

def _load_steam_shortcuts() -> list[dict]:
    """读取 Steam shortcuts.vdf，返回所有已添加的非 Steam 游戏列表。"""
    userdata = _find_steam_userdata_dir()
    shortcuts_path = os.path.join(userdata, "config", "shortcuts.vdf")
    if not os.path.isfile(shortcuts_path):
        return []
    try:
        from vdf import binary_load
        with open(shortcuts_path, "rb") as f:
            data = binary_load(f)
        result: list[dict] = []
        for key, entry in data.get("shortcuts", {}).items():
            if isinstance(entry, dict):
                appid = int(entry.get("appid", 0) or 0)
                entry["_appid"] = appid
                result.append(entry)
        return result
    except Exception:
        return []


def _get_installed_steam_appids() -> list[int]:
    """获取所有已安装的 Steam 游戏 AppID（通过解析 libraryfolders.vdf）。"""
    home = os.path.expanduser("~")
    steam_root = os.path.join(home, ".local", "share", "Steam")
    library_vdf = os.path.join(steam_root, "steamapps", "libraryfolders.vdf")
    appids: list[int] = []
    if not os.path.isfile(library_vdf):
        return appids
    try:
        from vdf import binary_load
        with open(library_vdf, "rb") as f:
            data = binary_load(f)
        for _key, folder in data.get("libraryfolders", {}).items():
            if isinstance(folder, dict):
                apps = folder.get("apps", {})
                if isinstance(apps, dict):
                    for appid_str in apps:
                        try:
                            appids.append(int(appid_str))
                        except (ValueError, TypeError):
                            pass
    except Exception:
        pass
    return appids


def _get_steam_game_install_dir(appid: int) -> str:
    """获取 Steam 游戏的安装目录（通过 appmanifest）。"""
    home = os.path.expanduser("~")
    steam_root = os.path.join(home, ".local", "share", "Steam")
    manifest_path = os.path.join(steam_root, "steamapps", f"appmanifest_{appid}.acf")
    if not os.path.isfile(manifest_path):
        # 尝试其他 library
        library_vdf = os.path.join(steam_root, "steamapps", "libraryfolders.vdf")
        try:
            from vdf import binary_load
            with open(library_vdf, "rb") as f:
                lib_data = binary_load(f)
            for _key, folder in lib_data.get("libraryfolders", {}).items():
                if isinstance(folder, dict):
                    lib_path = str(folder.get("path", ""))
                    alt_manifest = os.path.join(lib_path, "steamapps", f"appmanifest_{appid}.acf")
                    if os.path.isfile(alt_manifest):
                        manifest_path = alt_manifest
                        break
        except Exception:
            pass

    if not os.path.isfile(manifest_path):
        return ""

    try:
        content = open(manifest_path, "r", encoding="utf-8").read()
        m = re.search(r'"installdir"\s*"([^"]+)"', content)
        if m:
            dir_name = m.group(1)
            lib_dir = os.path.dirname(os.path.dirname(manifest_path))
            install_path = os.path.join(lib_dir, "common", dir_name)
            if os.path.isdir(install_path):
                return install_path
    except Exception:
        pass
    return ""


def _get_steam_game_name(appid: int) -> str:
    """获取 Steam 游戏的名称（通过 appmanifest）。"""
    home = os.path.expanduser("~")
    steam_root = os.path.join(home, ".local", "share", "Steam")
    manifest_path = os.path.join(steam_root, "steamapps", f"appmanifest_{appid}.acf")
    if not os.path.isfile(manifest_path):
        library_vdf = os.path.join(steam_root, "steamapps", "libraryfolders.vdf")
        try:
            from vdf import binary_load
            with open(library_vdf, "rb") as f:
                lib_data = binary_load(f)
            for _key, folder in lib_data.get("libraryfolders", {}).items():
                if isinstance(folder, dict):
                    lib_path = str(folder.get("path", ""))
                    alt = os.path.join(lib_path, "steamapps", f"appmanifest_{appid}.acf")
                    if os.path.isfile(alt):
                        manifest_path = alt
                        break
        except Exception:
            pass

    if not os.path.isfile(manifest_path):
        return ""

    try:
        content = open(manifest_path, "r", encoding="utf-8").read()
        m = re.search(r'"name"\s*"([^"]+)"', content)
        if m:
            return m.group(1)
    except Exception:
        pass
    return ""


def _get_compatdata_save_path(appid: int) -> str:
    """获取 Steam compatdata 下的存档根路径。"""
    home = os.path.expanduser("~")
    compat = os.path.join(
        home, ".steam", "steam", "steamapps", "compatdata", str(appid),
        "pfx", "drive_c", "users", "steamuser",
    )
    if os.path.isdir(compat):
        return compat
    return ""


class Plugin:
    def __init__(self):
        self._store = None
        self._qr = None
        self._server = None
        self._ok = False
        self._http_ok = False
        self._catalog = None
        self._catalog_items: list[dict] = []
        self._catalog_loaded = False
        self._platform_cache: dict[str, list[dict]] = {}  # platform → games 内存缓存
        self._downloads: dict[str, dict] = {}   # task_id → download state
        self._engine: Optional[DownloadEngine] = None
        self._cached_login: dict | None = None  # 持久化登录状态缓存
        self._cached_login_checked_at = 0.0
        self._login_refresh_generation = 0
        self._login_refresh_task: Optional[asyncio.Task] = None
        self._pending_artwork: dict = {}         # appid → {grid, hero, logo} => {data, format}

    def _empty_login_state(self, message: str = "") -> dict:
        return {"logged_in": False, "account_name": "", "message": str(message or "")}

    def _normalize_login_state(self, payload=None, message: str | None = None) -> dict:
        data = payload if isinstance(payload, dict) else {}
        account_name = str(data.get("account_name", data.get("user_account", "")) or "")
        next_state = {
            "logged_in": bool(data.get("logged_in")),
            "account_name": account_name,
            "message": str(data.get("message", "") or ""),
        }
        if message is not None:
            next_state["message"] = str(message or "")
        return next_state

    def _set_cached_login(self, payload=None, message: str | None = None) -> dict:
        self._cached_login = self._normalize_login_state(payload, message=message)
        self._cached_login_checked_at = time.time()
        return dict(self._cached_login)

    def _prime_login_cache_from_store(self) -> dict:
        if not self._ok or not self._store or not self._session_mgr:
            return self._set_cached_login(self._empty_login_state())
        try:
            from cloud import CloudProviderId
            cred = self._session_mgr.load(CloudProviderId.TIANYI)
        except Exception:
            cred = None
        if cred and str(getattr(cred, "cookie", "") or "").strip():
            return self._set_cached_login(
                {
                    "logged_in": True,
                    "account_name": str(getattr(cred, "account_name", "") or ""),
                    "message": "cached",
                }
            )
        return self._set_cached_login(self._empty_login_state())

    def _login_cache_is_fresh(self, ttl_seconds: float = LOGIN_CACHE_TTL_SECONDS) -> bool:
        if not self._cached_login:
            return False
        return (time.time() - float(self._cached_login_checked_at or 0.0)) < float(ttl_seconds)

    def _invalidate_login_refresh(self) -> None:
        self._login_refresh_generation += 1
        task = self._login_refresh_task
        if task and not task.done():
            task.cancel()
        self._login_refresh_task = None

    def _schedule_login_refresh(self, *, force: bool = False) -> Optional[asyncio.Task]:
        if not self._ok:
            return None
        if not force and self._login_cache_is_fresh():
            return self._login_refresh_task
        task = self._login_refresh_task
        if task and not task.done():
            return task
        generation = self._login_refresh_generation
        self._login_refresh_task = asyncio.create_task(self._refresh_login_cache(generation))
        return self._login_refresh_task

    async def _main(self):
        decky.logger.info("NewFreedeck _main")
        sd = _state_dir()
        os.makedirs(sd, exist_ok=True)

        # ── 日志文件（设置页可查看）──────────
        _setup_plugin_log(sd)

        # 游戏目录 + 封面索引 — 后台加载，不阻塞 QR 启动
        asyncio.create_task(self._load_catalog_async())

        # 下载引擎
        self._engine = DownloadEngine(
            plugin_dir=os.path.dirname(__file__),
            work_dir=os.path.join(_state_dir(), "downloads"),
        )
        asyncio.create_task(self._init_downloads())

        # 后台预热所有平台数据
        asyncio.create_task(self._warm_platform_cache())

        # 云盘子系统
        try:
            from cloud import SessionStore, SessionManager, TianyiCloudProvider, CloudProviderId
            self._store = SessionStore(os.path.join(sd, "credentials.json"))
            self._session_mgr = SessionManager(self._store)
            self._tianyi = TianyiCloudProvider()
            self._ok = True
            decky.logger.info("cloud ready")
        except Exception as e:
            decky.logger.error("cloud init: %s", e)

        # HTTP 服务 (QR 登录)
        try:
            from cloud.tianyi.qr_login import QrLoginManager
            from server_manager import LocalServer
            from tianyi_http import TianyiHttpRoutes

            self._qr = QrLoginManager(self._store) if self._store else None
            if self._qr:
                self._server = LocalServer(port=20065)
                routes = TianyiHttpRoutes(self._qr)
                routes.setup(self._server.app())
                await self._server.start()
                self._http_ok = True
                decky.logger.info("HTTP server: %s", self._server.base_url)
        except Exception as e:
            decky.logger.error("HTTP: %s\n%s", e, traceback.format_exc())

        # 预热登录缓存：先从本地凭证做快照，再后台联网校验
        self._prime_login_cache_from_store()
        self._schedule_login_refresh(force=True)

    async def _unload(self):
        if self._server:
            await self._server.stop()
        if self._engine:
            self._engine.stop()
            decky.logger.info("download engine stopped")

    async def _load_catalog_async(self):
        """加载游戏目录：本地缓存优先，后台拉取远端。"""
        import json as _json
        items: list[dict] = []

        cache_path = os.path.join(_state_dir(), "catalog_cache.json")

        # 1. 先从本地缓存加载（线程池，不阻塞事件循环）
        try:
            if os.path.exists(cache_path):
                def _read():
                    with open(cache_path, "r", encoding="utf-8") as f:
                        return _json.load(f)
                items = await asyncio.get_event_loop().run_in_executor(None, _read)
                if items:
                    decky.logger.info("catalog loaded from cache: %d games", len(items))
        except Exception as e:
            decky.logger.warning("catalog cache read failed: %s", e)

        # 2. 立即标记已加载，不阻塞前端
        self._catalog_items = items
        self._catalog_loaded = True

        # 3. 后台拉取远端更新
        asyncio.create_task(self._refresh_catalog_async(cache_path))

    async def _refresh_catalog_async(self, cache_path: str):
        """从远端拉取目录并写入缓存。"""
        import json as _json
        try:
            import aiohttp, ssl
            url = "http://panyiwei.xyz/freedeck-catalog/all.json"
            timeout = aiohttp.ClientTimeout(total=60, connect=10)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(url) as resp:
                    text = await resp.text()
                    items = _json.loads(text) if isinstance(_json.loads(text), list) else []
            if items:
                decky.logger.info("catalog refreshed from remote: %d games", len(items))
                # 写入缓存
                try:
                    with open(cache_path, "w", encoding="utf-8") as f:
                        _json.dump(items, f, ensure_ascii=False)
                except Exception:
                    pass
                self._catalog_items = items
                self._catalog_loaded = True
        except Exception as e:
            decky.logger.error("catalog remote failed: %s", e)

    # ── 登录相关 ──

    async def tianyi_check_login(self, payload=None, force: bool = False):
        if isinstance(payload, dict):
            force = bool(payload.get("force", force))
        if not self._ok:
            state = self._set_cached_login(self._empty_login_state())
            return _ok(state)
        if not force and self._cached_login:
            cached = dict(self._cached_login)
            self._schedule_login_refresh()
            return _ok(cached)
        try:
            from cloud.errors import LoginExpiredError
            cred = await self._session_mgr.ensure_valid(self._tianyi)
            return _ok(self._set_cached_login({"logged_in": True, "account_name": cred.account_name, "message": "ok"}))
        except LoginExpiredError:
            return _ok(self._set_cached_login(self._empty_login_state("已过期")))
        except Exception as e:
            return _err(str(e))

    async def tianyi_clear_login(self):
        if self._ok:
            from cloud import CloudProviderId
            self._session_mgr.delete(CloudProviderId.TIANYI)
        self._invalidate_login_refresh()
        return _ok(self._set_cached_login(self._empty_login_state("已退出")))

    async def tianyi_resolve_share(self, share_url: str = "", access_code: str = ""):
        if not self._ok:
            return _err("云盘子系统中未就绪")
        share_url = str(share_url or "").strip()
        if not share_url:
            return _err("请输入分享链接")
        try:
            from cloud.tianyi.resolver import resolve_share
            from cloud.errors import LoginExpiredError, ShareNotFoundError, ShareAccessDeniedError
            cred = await self._session_mgr.ensure_valid(self._tianyi)
            info = await resolve_share(share_url, cred, access_code=str(access_code or "").strip())
            return _ok({
                "share_code": info.share_code,
                "share_id": info.share_id,
                "access_code": info.access_code,
                "title": info.title,
                "files": [
                    {"file_id": f.file_id, "name": f.name, "size_bytes": f.size_bytes,
                     "is_directory": f.is_directory, "path": f.path}
                    for f in info.files
                ],
            })
        except LoginExpiredError:
            return _err("登录已过期，请重新登录")
        except ShareAccessDeniedError as e:
            return _err(str(e))
        except ShareNotFoundError as e:
            return _err(str(e))
        except Exception as e:
            decky.logger.error("resolve_share: %s", e)
            return _err(str(e))

    async def tianyi_fetch_link(self, file_id: str = "", share_id: str = ""):
        if not self._ok:
            return _err("云盘子系统中未就绪")
        try:
            from cloud.tianyi.link_fetcher import fetch_direct_link
            from cloud.errors import LoginExpiredError
            cred = await self._session_mgr.ensure_valid(self._tianyi)
            link = await fetch_direct_link(
                file_id=str(file_id or "").strip(),
                share_id=str(share_id or "").strip(),
                credential=cred,
            )
            return _ok({"url": link.url, "expires_at": link.expires_at})
        except LoginExpiredError:
            return _err("登录已过期")
        except Exception as e:
            return _err(str(e))

    async def tianyi_login_url(self):
        if self._http_ok and self._server:
            return _ok({"url": f"{self._server.base_url}/login_bridge.html"})
        return _err("HTTP 服务未启动")

    # ── QR 登录 callable 代理 ────────────

    async def _qr_guard(self):
        if not self._qr:
            return _err("QR 服务未初始化")
        return None

    async def tianyi_qr_start(self):
        if (guard := await self._qr_guard()): return guard
        try:
            data = await self._qr.start_qr_login()
            if str(data.get("stage", "") or "") == "completed":
                self._invalidate_login_refresh()
                self._set_cached_login(
                    {
                        "logged_in": True,
                        "account_name": str(data.get("user_account", "") or ""),
                        "message": str(data.get("message", "") or "ok"),
                    }
                )
            sid = data.get("session_id", "")
            if sid and data.get("stage") == "running":
                try:
                    img_bytes, content_type = await self._qr.get_qr_image(session_id=sid)
                    data["qr_data_url"] = f"data:{content_type};base64,{base64.b64encode(img_bytes).decode('ascii')}"
                except Exception:
                    pass
            return _ok(data)
        except Exception as e:
            return _err(str(e))

    async def tianyi_qr_status(self, payload=None, session_id: str = ""):
        if (guard := await self._qr_guard()): return guard
        try:
            if isinstance(payload, dict):
                session_id = str(payload.get("session_id", session_id) or "")
            result = await self._qr.poll_qr_login(session_id=session_id)
            if str(result.get("stage", "") or "") == "completed":
                account_name = str(result.get("user_account", "") or "").strip()
                self._invalidate_login_refresh()
                if account_name:
                    self._set_cached_login(
                        {
                            "logged_in": True,
                            "account_name": account_name,
                            "message": str(result.get("message", "") or "ok"),
                        }
                    )
                else:
                    self._prime_login_cache_from_store()
            return _ok(result)
        except Exception as e:
            return _err(str(e))

    async def tianyi_qr_stop(self, payload=None, session_id: str = ""):
        if (guard := await self._qr_guard()): return guard
        try:
            if isinstance(payload, dict):
                session_id = str(payload.get("session_id", session_id) or "")
            return _ok(await self._qr.stop_qr_login(session_id=session_id))
        except Exception as e:
            return _err(str(e))

    async def tianyi_qr_image(self, session_id: str = ""):
        if (guard := await self._qr_guard()): return guard
        try:
            img_bytes, content_type = await self._qr.get_qr_image(session_id=session_id)
            b64 = base64.b64encode(img_bytes).decode("ascii")
            return _ok({"data_url": f"data:{content_type};base64,{b64}", "content_type": content_type})
        except Exception as e:
            return _err(str(e))

    async def fetch_remote_catalog(self):
        """返回已在后台加载的远程目录。"""
        return _ok({
            "items": self._catalog_items,
            "total": len(self._catalog_items),
            "loaded": self._catalog_loaded,
        })

    async def get_shelf_games(self, payload=None, limit: int = 24):
        """返回智能推荐书架用的游戏子集（随机 shuffle）。"""
        import random, json
        try:
            n = int(limit or 24)
        except (TypeError, ValueError):
            n = 24
        if isinstance(payload, dict):
            try:
                n = int(payload.get("limit", n) or n)
            except (TypeError, ValueError):
                pass
        n = max(1, min(n, 100))
        items = list(self._catalog_items) if self._catalog_items else []
        random.shuffle(items)
        result = self._enrich_cover(items[:n])
        resp = _ok({"games": result, "total": len(self._catalog_items), "loaded": self._catalog_loaded})
        decky.logger.info("get_shelf_games: n=%d total=%d loaded=%s resp_len=%d",
                          n, len(self._catalog_items), self._catalog_loaded, len(json.dumps(resp)))
        return resp

    def _enrich_one(self, g: dict) -> dict:
        """补全单条记录。"""
        from urllib.parse import quote
        cover = str(g.get("cover_url", "")).strip()
        if cover and (cover.startswith("http://") or cover.startswith("https://")):
            cover = f"http://127.0.0.1:20065/proxy/cover?url={quote(cover, safe='')}"
        elif cover and not cover.startswith("http"):
            cover = ""
        enriched = {
            "game_id": str(g.get("game_id", "")),
            "title": str(g.get("title", "")),
            "title_zh": str(g.get("title_zh", "")),
            "title_en": str(g.get("title_en", "")),
            "platform": str(g.get("platform", "")).strip(),
            "cover_url": cover,
            "size_text": str(g.get("size_text", "")),
            "genre": str(g.get("genre", "")),
            "down_url": str(g.get("down_url", "")),
            "file_name": str(g.get("file_name", "")),
            "size_bytes": _try_int(g.get("size_bytes")),
            "pwd": str(g.get("pwd", "")),
            "server": str(g.get("server", "")),
            "version": str(g.get("version", "")),
            "validity": str(g.get("validity", "")),
            "install_size_text": str(g.get("install_size_text", "")),
            "install_size_bytes": _try_int(g.get("install_size_bytes")),
        }
        if g.get("grouped"):
            enriched["grouped"] = True
            enriched["variant_count"] = int(g.get("variant_count", 0))
            enriched["multi_disc"] = bool(g.get("multi_disc"))
            enriched["disc_count"] = int(g.get("disc_count", 1))
            enriched["variants"] = [self._enrich_one(v) for v in (g.get("variants") or [])]
        return enriched

    def _enrich_cover(self, games: list[dict]) -> list[dict]:
        """补全下载所需字段。支持分组后的 variants 嵌套。"""
        return [self._enrich_one(g) for g in games]

    async def get_platform_games(self, payload=None, platform: str = "", page: int = 1, page_size: int = 50):
        """返回指定平台的游戏列表（内存缓存优先 → 远程 → CSV）。"""
        import json as _json
        platform = str(platform or "").strip().lower()
        if isinstance(payload, dict):
            platform = str(payload.get("platform", platform) or "").strip().lower()
            page = int(payload.get("page", page) or 1)
            page_size = int(payload.get("page_size", payload.get("pageSize", page_size)) or 50)
        if not platform:
            return _err("platform required")

        # 1. 内存缓存命中 → 直接返回（秒开），后台刷新
        cached = self._platform_cache.get(platform)
        if cached:
            items = cached
            # 后台异步刷新（不阻塞本次响应）
            asyncio.create_task(self._refresh_platform_async(platform))
        else:
            # 2. 远程拉取
            items = await self._fetch_platform_remote(platform)
            # 3. 回退本地 CSV
            if not items and self._catalog_items:
                items = [it for it in self._catalog_items if (it.get("platform") or "").lower() == platform]

        # 合并同款不同版本
        try:
            grouped = _group_catalog_items(items)
        except Exception as e:
            decky.logger.error("get_platform_games: grouping failed for %s: %s", platform, e)
            grouped = items  # 回退：不分组直接返回
        total = len(grouped)
        decky.logger.info("get_platform_games: %s raw=%d grouped=%d page=%d", platform, len(items), total, page)
        page = max(1, page)
        page_size = min(200, max(1, page_size))
        start = (page - 1) * page_size
        end = start + page_size
        result = self._enrich_cover(grouped[start:end])
        return _ok({
            "games": result, "total": total,
            "page": page, "page_size": page_size, "has_more": end < total,
        })

    async def _fetch_platform_remote(self, platform: str) -> list[dict]:
        """从远程拉取平台数据并写入内存缓存。"""
        import json as _json
        try:
            import aiohttp, ssl
            url = f"http://panyiwei.xyz/freedeck-catalog/platform/{platform}.json"
            timeout = aiohttp.ClientTimeout(total=10)
            ssl_ctx = None
            try:
                ssl_ctx = ssl.create_default_context()
                ssl_ctx.check_hostname = False
                ssl_ctx.verify_mode = ssl.CERT_NONE
            except Exception:
                ssl_ctx = False
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(url, ssl=ssl_ctx) as resp:
                    data = _json.loads(await resp.text())
            items = data if isinstance(data, list) else data.get("games", data.get("items", []))
            if items:
                self._platform_cache[platform] = items
                decky.logger.info("platform cache: fetched %s (%d items)", platform, len(items))
            return items
        except Exception as e:
            decky.logger.warning("platform remote failed for %s: %s", platform, e)
            return []

    async def _warm_platform_cache(self):
        """启动时后台预热全部平台缓存。"""
        platforms = ["switch", "ps1", "ps2", "ps3", "psp", "3ds", "gba", "nds", "fc", "md"]
        tasks = [asyncio.create_task(self._fetch_platform_remote(p)) for p in platforms]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        ok = sum(1 for r in results if not isinstance(r, Exception) and r)
        decky.logger.info("platform cache warm-up: %d/%d platforms loaded", ok, len(platforms))

    async def _refresh_platform_async(self, platform: str):
        """后台刷新平台缓存。"""
        try:
            items = await self._fetch_platform_remote(platform)
            if items:
                self._platform_cache[platform] = items
        except Exception:
            pass

    async def get_curated_shelf(self, payload=None, platform: str = "", limit: int = 20):
        """返回某平台精选游戏（优先远程 JSON，回退本地 CSV top N）。"""
        platform = str(platform or "").strip().lower()
        if isinstance(payload, dict):
            platform = str(payload.get("platform", platform) or "").strip().lower()
            try:
                limit = int(payload.get("limit", limit) or limit)
            except (TypeError, ValueError):
                pass
        # 平台别名：ns ↔ switch
        platform_map = {"ns": "switch", "switch": "ns"}
        curated_platform = platform_map.get(platform, platform)
        if not platform:
            return _err("platform required")
        limit = max(1, min(limit, 50))
        items: list[dict] = []

        try:
            import aiohttp, json as _json, ssl
            url = f"http://panyiwei.xyz/freedeck-catalog/curated/{curated_platform}.json"
            timeout = aiohttp.ClientTimeout(total=8)
            ssl_ctx = None
            try:
                ssl_ctx = ssl.create_default_context()
                ssl_ctx.check_hostname = False
                ssl_ctx.verify_mode = ssl.CERT_NONE
            except Exception:
                ssl_ctx = False  # aiohttp: False = skip SSL verification
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(url, ssl=ssl_ctx) as resp:
                    data = _json.loads(await resp.text())
            items = data if isinstance(data, list) else (data.get("games", data.get("items", [])) if isinstance(data, dict) else [])
        except Exception as e:
            decky.logger.warning("curated fetch failed for %s (%s): %s", platform, curated_platform, e)

        if not items and self._catalog_items:
            items = [it for it in self._catalog_items if (it.get("platform") or "").lower() == platform]

        # 合并同款不同版本
        try:
            grouped = _group_catalog_items(items)
        except Exception as e:
            decky.logger.error("get_curated_shelf: grouping failed for %s: %s", platform, e)
            grouped = items
        if limit < len(grouped):
            grouped = grouped[:limit]
        decky.logger.info("get_curated_shelf: %s raw=%d grouped=%d", platform, len(items), len(grouped))

        result = self._enrich_cover(grouped[:limit])
        return _ok({"games": result, "total": len(result), "platform": platform})

    async def _init_downloads(self):
        """启动下载引擎（Rust 优先，Python 回退）。"""
        global _rust_download
        if _rust_download and _core:
            state_dir = _state_dir()
            loop = asyncio.get_event_loop()
            try:
                plugin_dir = os.path.dirname(__file__)
                work_dir = os.path.join(state_dir, "downloads")
                # 线程池调用，避免 C 函数阻塞事件循环
                rc = await loop.run_in_executor(
                    None,
                    lambda: _core.nfc_download_init(
                        ctypes.c_char_p(plugin_dir.encode()),
                        ctypes.c_char_p(work_dir.encode()),
                    )
                )
                if rc == 0:
                    decky.logger.info("download engine (Rust) ready")
                    # 不 return —— 同时初始化 Python 引擎作为主引擎
                else:
                    decky.logger.warning("Rust download engine init returned %d, falling back", rc)
            except Exception as e:
                decky.logger.warning("Rust download engine init failed: %s", e)
            _rust_download = False

            # 初始化解压引擎
            try:
                _core.nfc_extract_init(ctypes.c_char_p(plugin_dir.encode()))
                decky.logger.info("extract engine (Rust) ready")
            except Exception as e:
                decky.logger.warning("extract engine init failed: %s", e)

        # Python 回退
        try:
            if self._engine:
                await self._engine.start()
                decky.logger.info("download engine (Python) ready")
        except Exception as e:
            decky.logger.warning("download engine (Python) init failed: %s", e)

    # ── 下载 callable ──────────────────────

    async def download_start(self, payload=None, share_url: str = "", file_id: str = "",
                             file_name: str = "", file_size: int = 0, game_id: str = "",
                             game_title: str = "", access_code: str = ""):
        share_url = str(payload.get("share_url", share_url) if isinstance(payload, dict) else str(share_url or ""))
        file_id = str(payload.get("file_id", file_id) if isinstance(payload, dict) else str(file_id or ""))
        file_name = str(payload.get("file_name", file_name) if isinstance(payload, dict) else str(file_name or ""))
        file_size = int(payload.get("file_size", file_size) if isinstance(payload, dict) else (file_size or 0))
        game_id = str(payload.get("game_id", game_id) if isinstance(payload, dict) else str(game_id or ""))
        game_title = str(payload.get("game_title", game_title) if isinstance(payload, dict) else str(game_title or ""))
        access_code = str(payload.get("access_code", access_code) if isinstance(payload, dict) else str(access_code or ""))

        if not share_url or not file_name:
            return _err("参数不足")
        if not self._ok:
            return _err("云盘子系统未就绪")

        # ── 多盘游戏检测：找到所有兄弟盘，一次性下载全部 ──
        # catalog_items 是扁平列表，先分组才能找到多盘条目
        grouped_catalog = _group_catalog_items(self._catalog_items)
        disc_variants: list[dict] = []
        is_multi_disc = False
        for item in grouped_catalog:
            if item.get("grouped") and len(item.get("variants", [])) >= 2:
                for v in item.get("variants", []):
                    if str(v.get("game_id", "")) == game_id:
                        is_multi_disc = True
                        disc_variants = item.get("variants", [])
                        decky.logger.info("download_start: multi-disc game detected, %d discs", len(disc_variants))
                        break
            if is_multi_disc:
                break

        if is_multi_disc and disc_variants:
            # 所有盘共享同一个分享链接 → 解析一次，下载全部匹配文件
            download_dir = self._get_dl_dir()
            os.makedirs(download_dir, exist_ok=True)

            # 清理旧的 .aria2 控制文件
            for _dn in all_disc_names:
                _af = os.path.join(download_dir, _dn + ".aria2")
                if os.path.exists(_af):
                    os.remove(_af)
                    decky.logger.info("download_start: cleaned stale aria2 file %s", _af)

            # 收集所有盘的文件名和密码
            all_disc_names = []
            all_disc_sizes = []
            disc_pwd = access_code if access_code else "oldmanemu.net"

            for i, disc in enumerate(disc_variants):
                disc_name = str(disc.get("file_name", "")).strip()
                disc_size = _try_int(disc.get("size_bytes", 0))
                if disc_name:
                    all_disc_names.append(disc_name)
                    all_disc_sizes.append(disc_size)
                    decky.logger.info("download_start: disc %d/%d %s", i+1, len(disc_variants), disc_name)

            if not all_disc_names:
                return _err("多盘文件名为空")

            # 解析一次分享链接，获取所有文件
            try:
                cred = await self._session_mgr.ensure_valid(self._tianyi)
                resolved = await self._tianyi.resolve_share(share_url, cred, access_code=access_code)
                all_files = [f for f in resolved.files if not f.is_directory]
                if not all_files:
                    return _err("分享内没有可下载文件")
            except Exception as e:
                decky.logger.error("download_start: resolve failed: %s", e)
                return _err(str(e))

            # 匹配所有盘文件
            dl_urls, dl_names, dl_sizes = [], [], []
            total_size = 0
            for disc_name in all_disc_names:
                target = None
                for f in all_files:
                    if f.name == disc_name:
                        target = f
                        break
                if not target:
                    decky.logger.warning("download_start: disc file not found: %s", disc_name)
                    continue
                try:
                    link = await self._tianyi.fetch_direct_link(
                        file_id=target.file_id, share_id=resolved.share_id, credential=cred)
                    u = str(link.url or "").strip()
                    if u:
                        dl_urls.append(u)
                        dl_names.append(target.name)
                        dl_sizes.append(target.size_bytes)
                        total_size += target.size_bytes
                except Exception as e:
                    decky.logger.warning("download_start: fetch link failed for %s: %s", disc_name, e)

            if not dl_urls:
                return _err("所有盘直链获取失败")

            decky.logger.info("download_start: multi-disc, %d/%d files matched, total=%d",
                              len(dl_urls), len(all_disc_names), total_size)

            # 创建统一下载任务
            if self._engine and self._engine.ready:
                gids = []
                for u, name in zip(dl_urls, dl_names):
                    gid = await self._engine.rpc.add_uri(uri=u, dir_=download_dir, out=name)
                    gids.append(gid)

                task_id = uuid.uuid4().hex[:12]
                task = {
                    "task_id": task_id, "gids": gids, "gid": gids[0] if gids else "",
                    "game_id": game_id, "game_title": game_title,
                    "file_names": dl_names, "file_name": dl_names[0] if dl_names else file_name,
                    "file_sizes": dl_sizes, "file_size": total_size,
                    "status": "downloading", "progress": 0.0, "speed": 0,
                    "completed": 0, "total": total_size, "error": "",
                    "archive_password": disc_pwd,
                    "multi_disc": True, "disc_variant_index": 0,
                    "file_path": os.path.join(download_dir, dl_names[0]) if dl_names else "",
                }
                self._downloads[task_id] = task
                return _ok(task)

            return _err("下载引擎未就绪")

        # ── 单盘/非多盘：正常下载 ──
        try:
            from cloud.errors import (
                DirectLinkError,
                DirectLinkRateLimited,
                LoginExpiredError,
                ShareAccessDeniedError,
                ShareNotFoundError,
            )

            cred = await self._session_mgr.ensure_valid(self._tianyi)
            decky.logger.info("download_start: resolving share_url=%s", share_url[:80])
            resolved = await self._tianyi.resolve_share(
                share_url,
                cred,
                access_code=access_code,
            )
            if not resolved or not resolved.files:
                decky.logger.error("download_start: resolve_share returned empty, resolved=%s", resolved)
                return _err("分享解析失败")

            # ── 文件匹配（install_pipeline 模块化处理）──
            from install_pipeline import match_files, match_folder, classify_game, is_archive

            game_type = classify_game(game_id, "", file_name)
            all_files = resolved.files  # 不过滤目录，PS3 需要

            # PS3：先找文件夹
            dl_files: list = []
            if game_type == "emulator_rom":
                plat = self._get_platform_for_game(game_id)
                if plat in ("ps3",):
                    folder = match_folder(file_name, all_files)
                    if folder:
                        dl_files = [f for f in all_files
                                    if not f.is_directory and str(f.path or "").startswith(str(folder.path or ""))]
                        decky.logger.info("download_start: PS3 folder=%s matched, %d files",
                                          folder.name, len(dl_files))
                    else:
                        return _err(f"PS3: 未找到匹配文件夹（目标: {file_name}）")

            if not dl_files:
                # 非 PS3 路径：正常文件匹配
                non_dirs = [f for f in all_files if not f.is_directory]
                if not non_dirs:
                    return _err("分享内没有可下载文件")

                # 用 pipeline 模块匹配
                matched = match_files(file_name, non_dirs, plat if game_type == "emulator_rom" else "")
                if matched:
                    dl_files = matched
                    decky.logger.info("download_start: matched %d file(s) for '%s'", len(dl_files), file_name)
                elif len(non_dirs) == 1:
                    dl_files = non_dirs
                    decky.logger.info("download_start: single file share")
                else:
                    # 多文件但没匹配到 → 检查是否分卷
                    base_name = _split_archive_base(file_name)
                    if base_name:
                        dl_files = [f for f in non_dirs if f.name.startswith(base_name)]
                        if dl_files:
                            decky.logger.info("download_start: split archive base=%s, %d parts",
                                              base_name, len(dl_files))
                    if not dl_files:
                        return _err(f"未找到匹配文件（目标: {file_name}，可用: {[f.name[:30] for f in non_dirs[:5]]}...）")

            decky.logger.info("download_start: share=%s selected %d files", resolved.share_id, len(dl_files))

            # 批量获取直链
            all_urls: list[str] = []
            all_names: list[str] = []
            all_sizes: list[int] = []
            total_size = 0

            for f in dl_files:
                try:
                    link = await self._tianyi.fetch_direct_link(
                        file_id=f.file_id,
                        share_id=resolved.share_id,
                        credential=cred,
                    )
                    u = str(link.url or "").strip()
                    if u:
                        all_urls.append(u)
                        all_names.append(f.name)
                        all_sizes.append(f.size_bytes)
                        total_size += f.size_bytes
                except Exception as e:
                    decky.logger.warning("fetch_direct_link failed for %s: %s", f.name, e)

            if not all_urls:
                return _err("获取直链失败")

            decky.logger.info(
                "download_start: %d files, total_size=%d, names=%s",
                len(all_urls), total_size, all_names[:3],
            )

            # Python 引擎（优先，asyncio 适配 Decky 环境）
            if self._engine and self._engine.ready:
                download_dir = self._get_dl_dir()
                os.makedirs(download_dir, exist_ok=True)
                # 清理旧的 .aria2 控制文件 + 不完整数据文件，
                # 防止暂停后重试时 aria2 续传导致文件损坏
                for _n in all_names:
                    _af = os.path.join(download_dir, _n + ".aria2")
                    if os.path.exists(_af):
                        os.remove(_af)
                        _df = os.path.join(download_dir, _n)
                        if os.path.exists(_df):
                            os.remove(_df)
                            decky.logger.info(
                                "download_start: removed incomplete file %s", _n
                            )
                gids: list[str] = []
                for u, name in zip(all_urls, all_names):
                    gid = await self._engine.rpc.add_uri(uri=u, dir_=download_dir, out=name)
                    gids.append(gid)
                    decky.logger.info("  add_uri: %s → gid=%s", name, gid)

                task_id = uuid.uuid4().hex[:12]
                task = {
                    "task_id": task_id,
                    "gids": gids,           # 多个 gid
                    "gid": gids[0] if gids else "",
                    "game_id": game_id,
                    "game_title": game_title,
                    "file_names": all_names,
                    "file_name": all_names[0] if all_names else file_name,
                    "file_sizes": all_sizes,
                    "file_size": total_size,
                    "status": "downloading",
                    "progress": 0.0,
                    "speed": 0,
                    "completed": 0,
                    "total": total_size,
                    "error": "",
                    "archive_password": access_code if access_code else "",
                    "file_path": os.path.join(download_dir, all_names[0]) if all_names else "",
                }
                self._downloads[task_id] = task
                decky.logger.info("download_start: using Python engine, %d files, task=%s", len(gids), task_id)
                return _ok(task)

            # Rust 引擎（回退 — 同步 RPC，无 thread::spawn）
            if _rust_download and _core:
                urls_json = ctypes.c_char_p(json.dumps(all_urls).encode())
                names_json = ctypes.c_char_p(json.dumps(all_names).encode())
                sizes_json = ctypes.c_char_p(json.dumps(all_sizes).encode())
                cookie_c = ctypes.c_char_p(b"")
                result_c = _core.nfc_download_start(urls_json, names_json, sizes_json, cookie_c)
                result = result_c.decode() if result_c else ""
                _core.nfc_free(result_c)
                if result.startswith("__err__"):
                    return _err(result[7:])
                task_id = result
                task = {
                    "task_id": task_id,
                    "gids": [],
                    "gid": "",
                    "game_id": game_id,
                    "game_title": game_title,
                    "file_names": all_names,
                    "file_name": all_names[0] if all_names else file_name,
                    "file_sizes": all_sizes,
                    "file_size": total_size,
                    "status": "downloading",
                    "progress": 0.0,
                    "speed": 0,
                    "completed": 0,
                    "total": total_size,
                    "error": "",
                    "archive_password": access_code if access_code else "",
                    "file_path": os.path.join(download_dir, all_names[0]) if all_names else "",
                }
                self._downloads[task_id] = task
                return _ok(task)

            return _err("下载引擎未就绪")
        except LoginExpiredError:
            return _err("登录已过期，请重新登录")
        except ShareAccessDeniedError as e:
            return _err(str(e))
        except ShareNotFoundError as e:
            return _err(str(e))
        except DirectLinkRateLimited as e:
            return _err(str(e))
        except DirectLinkError as e:
            return _err(str(e))
        except Exception as e:
            decky.logger.error("download_start failed: %s", e)
            return _err(str(e))

    async def download_poll(self, payload=None, task_id: str = ""):
        task_id = str(payload.get("task_id", task_id) if isinstance(payload, dict) else str(task_id or ""))
        if not task_id or task_id not in self._downloads:
            return _err("任务不存在")
        task = self._downloads[task_id]

        # ── 解压进度（_auto_extract 已用 asyncio 子进程，进度由后台协程直接写入 task）──

        # Python 引擎（优先）— 支持多文件
        gids = task.get("gids", [])
        if not gids:
            gid = task.get("gid", "")
            if gid:
                gids = [gid]
        if gids and self._engine and self._engine.ready:
            try:
                total_completed = 0
                total_size = 0
                total_speed = 0
                all_complete = True
                any_failed = False
                errors: list[str] = []

                for gid in gids:
                    status = await self._engine.rpc.tell_status(gid)
                    t = int(status.get("totalLength", 0) or 0)
                    c = int(status.get("completedLength", 0) or 0)
                    s = int(status.get("downloadSpeed", 0) or 0)
                    st = str(status.get("status", ""))

                    total_completed += c
                    total_size += t
                    total_speed += s

                    if st == "error":
                        any_failed = True
                        errors.append(str(status.get("errorMessage", "")))
                    elif st != "complete":
                        all_complete = False

                if total_size > 0:
                    task["total"] = total_size
                    task["progress"] = round(total_completed * 100.0 / total_size, 1)
                task["completed"] = total_completed
                task["speed"] = total_speed

                if all_complete:
                    task["status"] = "complete"
                    task["progress"] = 100.0
                    # 等 1 秒确保文件刷盘，再触发解压
                    if not task.get("extract_task_id") and not task.get("extract_status"):
                        asyncio.create_task(self._delayed_extract(task_id, task))
                elif any_failed:
                    task["status"] = "failed"
                    task["error"] = "; ".join(e for e in errors if e) or "下载失败"
                return _ok(task)
            except Exception as e:
                task["error"] = str(e)
                return _ok(task)

        # Rust 引擎（回退）— 只处理 Rust 下载，不阻挡 Python 引擎
        rust_handled = False
        if _rust_download and _core:
            tid_c = ctypes.c_char_p(task_id.encode())
            result_c = _core.nfc_download_poll(tid_c)
            result = result_c.decode() if result_c else "{}"
            _core.nfc_free(result_c)
            if not result.startswith("__err__") and result != "{}":
                try:
                    import json as _j
                    data = _j.loads(result)
                    if isinstance(data, dict):
                        files = data.get("files", [])
                        if files and isinstance(files, list):
                            f0 = files[0]
                            total = int(f0.get("size_bytes", 0))
                            dl = int(f0.get("downloaded", 0))
                            state = str(f0.get("state", ""))
                            if total > 0:
                                task["total"] = total
                                task["progress"] = round(dl*100.0/total, 1)
                            task["completed"] = dl
                            task["speed"] = 0
                            if "Complete" in state:
                                task["status"] = "complete"
                                task["progress"] = 100.0
                                rust_handled = True
                                if not task.get("extract_task_id") and not task.get("extract_status"):
                                    asyncio.create_task(self._auto_extract(task_id, task))
                            elif "Failed" in state:
                                task["status"] = "failed"
                                task["error"] = str(f0.get("state", "失败"))
                                rust_handled = True
                        task_state = data.get("task_state", "")
                        if "Complete" in str(task_state):
                            task["status"] = "complete"
                            rust_handled = True
                            if not task.get("extract_task_id") and not task.get("extract_status"):
                                asyncio.create_task(self._auto_extract(task_id, task))
                        elif "Failed" in str(task_state):
                            task["status"] = "failed"
                            task["error"] = str(task_state)
                            rust_handled = True
                except Exception:
                    pass
            if rust_handled:
                return _ok(task)

        # Python 回退
        gid = task.get("gid", "")
        if not gid or not self._engine or not self._engine.ready:
            return _ok(task)
        try:
            status = await self._engine.rpc.tell_status(gid)
            total = int(status.get("totalLength", 0) or 0)
            completed = int(status.get("completedLength", 0) or 0)
            speed = int(status.get("downloadSpeed", 0) or 0)
            aria_status = str(status.get("status", ""))
            if total > 0:
                task["total"] = total
                task["progress"] = round(completed * 100.0 / total, 1)
            task["completed"] = completed
            task["speed"] = speed
            if aria_status == "complete":
                task["status"] = "complete"
                task["progress"] = 100.0
                if not task.get("extract_task_id") and not task.get("extract_status"):
                    asyncio.create_task(self._auto_extract(task_id, task))
            elif aria_status == "error":
                task["status"] = "failed"
                task["error"] = str(status.get("errorMessage", "下载失败"))
            elif aria_status == "paused":
                task["status"] = "paused"
            elif aria_status in ("active", "waiting"):
                task["status"] = "downloading"
        except Exception as e:
            task["error"] = str(e)
        return _ok(task)

    async def _auto_import_after_extract(self, task_id: str, task: dict):
        """解压完成后自动导入 Steam（后台异步）— 支持 PC / 模拟器本体 / ROM"""
        # 多盘游戏：只有第一张盘导入 Steam，其余盘只解压
        if task.get("multi_disc") and task.get("disc_variant_index", 0) > 0:
            task["import_status"] = "complete"
            decky.logger.info("_auto_import: disc %d (multi-disc), skipping Steam import",
                              task.get("disc_variant_index", 0))
            return

        extract_output = task.get("extract_output", "")
        if not extract_output or not os.path.isdir(extract_output):
            decky.logger.info("_auto_import: no extract output dir")
            return

        game_id = str(task.get("game_id", ""))
        game_title = str(task.get("game_title", ""))
        if not game_id:
            decky.logger.info("_auto_import: no game_id, skip")
            return

        # 查目录获取 openpath + platform
        openpath = ""
        platform = ""
        for item in self._catalog_items:
            if str(item.get("game_id", "")) == game_id:
                openpath = str(item.get("openpath", "") or "")
                platform = str(item.get("platform", "") or "").lower()
                if not game_title:
                    game_title = str(item.get("title_zh", "") or item.get("title", ""))
                break
        decky.logger.info("_auto_import: game_id=%s platform=%s catalog_items=%d",
                          game_id, platform or "(not found)", len(self._catalog_items))

        try:
            from install_pipeline import classify_game, is_rom, find_emulator_appimage, build_rom_launch_args
        except Exception as _e:
            decky.logger.error("_auto_import: install_pipeline import failed: %s", _e)
            task["import_status"] = "failed"
            task["import_error"] = f"pipeline import: {_e}"
            return

        game_type = classify_game(game_id, platform, str(task.get("file_name", "")))
        decky.logger.info("_auto_import: game_type=%s platform=%s", game_type, platform)

        # ═══ ROM 导入：模拟器作启动器 + ROM 作参数 ═══
        if game_type == "emulator_rom":
            rom_path = task.get("rom_path", "")
            if not rom_path:
                # 在 extract_output 中递归搜索 ROM 文件，多盘游戏优先 Disc 1/A
                found_roms = []
                for root, _, files in os.walk(extract_output):
                    for fname in files:
                        if is_rom(fname, platform):
                            found_roms.append(os.path.join(root, fname))
                if found_roms:
                    # 排序：Disc 1/A 排前面
                    found_roms.sort(key=lambda p: os.path.basename(p).lower())
                    rom_path = found_roms[0]
                    decky.logger.info("_auto_import: selected ROM %s (of %d found)",
                                      os.path.basename(rom_path), len(found_roms))
            if not rom_path:
                task["import_status"] = "failed"
                task["import_error"] = f"未找到 {platform.upper()} ROM 文件（解压目录: {extract_output}）"
                decky.logger.info("_auto_import: no ROM found in %s", extract_output)
                return

            # 找模拟器 AppImage
            emu_appimage = find_emulator_appimage(platform)
            if not emu_appimage:
                task["import_status"] = "failed"
                task["import_error"] = f"未安装 {platform.upper()} 模拟器，请先在设置中安装"
                decky.logger.info("_auto_import: no emulator for platform=%s", platform)
                return

            # 构建启动参数
            launch_opts = build_rom_launch_args(platform, rom_path)
            decky.logger.info("_auto_import: ROM import emu=%s rom=%s launch=%s", emu_appimage, rom_path, launch_opts)

            userdata_dir = _find_steam_userdata_dir()
            steam_id = os.path.basename(userdata_dir)
            shortcuts_path = os.path.join(userdata_dir, "config", "shortcuts.vdf")

            import subprocess as _sp
            loop = asyncio.get_event_loop()
            _minimal_env = {
                "PATH": "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
                "HOME": os.environ.get("HOME", os.path.expanduser("~")),
                "USER": os.environ.get("USER", "deck"),
                "LANG": "en_US.UTF-8",
                "LC_ALL": "en_US.UTF-8",
            }

            try:
                # 前端 AddShortcut 会创建快捷方式并回传 appid
                task["import_status"] = "need_shortcut"
                task["steam_exe"] = emu_appimage
                task["game_title"] = game_title
                task["launch_options"] = launch_opts
                task["start_dir"] = os.path.dirname(rom_path) or os.path.dirname(emu_appimage)
                task["rom_path"] = rom_path
                decky.logger.info("_auto_import: ROM need_shortcut exe=%s title=%s", emu_appimage, game_title)
                return

            except Exception as e:
                decky.logger.error("_auto_import: ROM import failed: %s", e)
                task["import_status"] = "failed"
                task["import_error"] = f"ROM 导入失败: {e}"
                return

        # ═══ 以下是 PC 游戏 / 模拟器本体的导入逻辑 ═══

        if not _core:
            task["import_status"] = "failed"
            task["import_error"] = "Rust core not loaded"
            return

        try:
            task["import_status"] = "discovering"
            loop = asyncio.get_event_loop()

            # Step 1: 发现 exe
            decky.logger.info("_auto_import: discovering exe in %s, openpath=%s", extract_output, openpath)
            disc_c = await loop.run_in_executor(
                None,
                lambda: _core.nfc_steam_discover_exe(
                    ctypes.c_char_p(extract_output.encode()),
                    ctypes.c_char_p(openpath.encode()),
                )
            )
            disc_str = ctypes.cast(disc_c, ctypes.c_char_p).value.decode() if disc_c else "{}"
            _core.nfc_free(disc_c)
            disc = json.loads(disc_str) if isinstance(disc_str, str) else {}

            is_emulator_tool = str(game_id).endswith("_emulator")
            if is_emulator_tool:
                task["import_status"] = "complete"
                decky.logger.info("_auto_import: emulator tool, skipping Steam import")
                return

            if not disc.get("found"):
                # 模拟器本体回退：搜索 .AppImage
                if is_emulator_tool:
                    import glob as _glob
                    appimages = _glob.glob(os.path.join(extract_output, "**", "*.AppImage"), recursive=True)
                    if appimages:
                        exe_path = appimages[0]
                        decky.logger.info("_auto_import: found AppImage=%s (emulator)", exe_path)
                    else:
                        task["import_status"] = "failed"
                        task["import_error"] = "AppImage not found in extract dir"
                        decky.logger.info("_auto_import: AppImage not found in %s", extract_output)
                        return
                else:
                    task["import_status"] = "failed"
                    task["import_error"] = f"exe not found (method={disc.get('method','?')})"
                    decky.logger.info("_auto_import: exe not found, candidates=%s", disc.get("candidates", []))
                    return
            else:
                exe_path = disc.get("exe_path", "")
                decky.logger.info("_auto_import: found exe=%s method=%s", exe_path, disc.get("method"))

            # 模拟器本体：复制到安装目录后重指 exe_path
            if is_emulator_tool:
                import shutil as _shutil
                plat_name = str(game_id).replace("_emulator", "")
                s = self._load_settings_dict()
                emu_install_dir = os.path.join(
                    s.get("install_path", os.path.join(os.path.expanduser("~"), "FreedeckGames")),
                    "Emulators", plat_name,
                )
                os.makedirs(emu_install_dir, exist_ok=True)
                # 复制所有文件
                for item in os.listdir(extract_output):
                    s = os.path.join(extract_output, item)
                    d = os.path.join(emu_install_dir, item)
                    try:
                        if os.path.isdir(s):
                            if not os.path.exists(d):
                                _shutil.copytree(s, d)
                        else:
                            _shutil.copy2(s, d)
                    except Exception as e:
                        decky.logger.warning("_auto_import: copy failed %s: %s", item, e)
                # 更新 exe_path 指向安装目录
                exe_name = os.path.basename(exe_path)
                exe_path = os.path.join(emu_install_dir, exe_name)
                # 确保可执行
                try:
                    os.chmod(exe_path, os.stat(exe_path).st_mode | 0o111)
                except Exception:
                    pass

                # 运行模拟器配置（语言/手柄/BIOS/便携模式）
                try:
                    from emulator_adapters import bootstrap_emulator
                    boot_result = bootstrap_emulator(plat_name, emu_install_dir)
                    decky.logger.info("_auto_import: emulator bootstrap ok=%s messages=%s",
                                      boot_result.get("ok"), boot_result.get("messages"))
                except Exception as e:
                    decky.logger.warning("_auto_import: emulator bootstrap failed: %s", e)

                # 更新 game_title
                from emulator_registry import EMULATOR_PLATFORMS
                plat_info = EMULATOR_PLATFORMS.get(plat_name, {})
                if not game_title:
                    game_title = plat_info.get("display", plat_name) + " 模拟器"
                decky.logger.info("_auto_import: emulator copied to %s, exe=%s", emu_install_dir, exe_path)

            # Step 2: 先检查是否已存在对应 shortcut，避免重复导入
            userdata_dir = _find_steam_userdata_dir()
            shortcuts_path = os.path.join(userdata_dir, "config", "shortcuts.vdf")
            steam_id = os.path.basename(userdata_dir)
            decky.logger.info("_auto_import: userdata_dir=%s", userdata_dir)

            appid = None
            try:
                existing = _resolve_shortcut_duplicates(shortcuts_path, exe_path, game_title)
                if existing:
                    appid = int(existing.get("appid", 0) or 0)
                    removed = int(existing.get("duplicates_removed", 0) or 0)
                    decky.logger.info(
                        "_auto_import: reusing existing shortcut appid=%s removed_duplicates=%s",
                        appid,
                        removed,
                    )
            except Exception as e:
                decky.logger.warning("_auto_import: existing shortcut lookup failed: %s", e)

            # 前端 AddShortcut 会创建快捷方式并回传 appid
            if not appid:
                task["import_status"] = "need_shortcut"
                task["steam_exe"] = exe_path
                task["game_title"] = game_title
                task["start_dir"] = os.path.dirname(exe_path)
                decky.logger.info("_auto_import: PC need_shortcut exe=%s title=%s", exe_path, game_title)
                return

            # 已有快捷方式（appid 已存在）：直接走后续配置
            task["steam_appid"] = appid
            task["steam_exe"] = exe_path
            task["game_title"] = game_title

            # PC 游戏挂 Proton Experimental 兼容层；模拟器游戏不挂（原生 Linux 模拟器）
            is_pc = (platform == "pc")
            decky.logger.info("_auto_import: compat decision platform=%r is_pc=%s appid=%s",
                              platform, is_pc, appid)
            if is_pc:
                decky.logger.info("_auto_import: setting compat tool %s for appid=%s userdata=%s",
                                  DEFAULT_SHORTCUT_COMPAT_TOOL, appid, userdata_dir)
                compat_result = _set_shortcut_compat_tool(
                    userdata_dir,
                    appid,
                    DEFAULT_SHORTCUT_COMPAT_TOOL,
                )
                task["compat_tool"] = DEFAULT_SHORTCUT_COMPAT_TOOL
                if compat_result.get("ok"):
                    decky.logger.info(
                        "_auto_import: compat tool set ok appid=%s tool=%s changed=%s",
                        appid,
                        DEFAULT_SHORTCUT_COMPAT_TOOL,
                        bool(compat_result.get("changed")),
                    )
                else:
                    decky.logger.warning(
                        "_auto_import: compat tool set FAILED appid=%s msg=%s",
                        appid,
                        compat_result.get("message", "unknown error"),
                    )
            else:
                decky.logger.info("_auto_import: skip compat tool for platform=%s", platform)

            decky.logger.info("_auto_import: appid=%s title=%s", appid, game_title)

            # 模拟器工具：清理旧重复条目 + 设 QT_QPA_PLATFORM=wayland 防闪退
            if is_emulator_tool:
                try:
                    _resolve_shortcut_duplicates(shortcuts_path, exe_path, game_title)
                except Exception as e:
                    decky.logger.warning("_auto_import: dedup failed: %s", e)
                try:
                    from vdf import binary_load, binary_dump
                    with open(shortcuts_path, "rb") as f:
                        vdf_data = binary_load(f)
                    shortcuts = vdf_data.get("shortcuts", {})
                    for _idx, _entry in shortcuts.items():
                        if isinstance(_entry, dict) and str(_entry.get("appid", "")) == str(appid):
                            _entry["LaunchOptions"] = "QT_QPA_PLATFORM=wayland"
                            break
                    with open(shortcuts_path, "wb") as f:
                        binary_dump(vdf_data, f)
                    decky.logger.info("_auto_import: VDF patched LaunchOptions for emulator appid=%s", appid)
                except Exception as e:
                    decky.logger.warning("_auto_import: VDF patch failed: %s", e)

            # 设置 import_status = "applying"，前端会用 SteamClient API 即时应用名称和封面
            task["import_status"] = "applying"

            # Step 4: 后台异步下载封面（不阻塞，完成后前端轮询 get_pending_artwork）
            game_info = {}
            for item in self._catalog_items:
                if str(item.get("game_id", "")) == game_id:
                    game_info = item
                    break
            asyncio.create_task(self._fetch_and_apply_covers_async(
                appid, steam_id, game_info, extract_output, task_id
            ))

        except Exception as e:
            decky.logger.error("_auto_import: %s", e)
            task["import_status"] = "failed"
            task["import_error"] = str(e)

    def _get_platform_for_game(self, game_id: str) -> str:
        """从 game_id 推导平台（已归一化）。"""
        from install_pipeline import normalize_platform
        for item in self._catalog_items:
            if str(item.get("game_id", "")) == str(game_id):
                return normalize_platform(str(item.get("platform", "") or ""))
        return ""

    async def _delayed_extract(self, task_id: str, task: dict):
        """等 1 秒确保 aria2 文件刷盘后再解压。"""
        await asyncio.sleep(1)
        await self._auto_extract(task_id, task)

    async def _auto_extract(self, task_id: str, task: dict):
        """下载完成后自动触发解压（后台异步）— 使用 7zz 子进程 + asyncio 实时进度"""
        file_names = task.get("file_names", []) or []
        if not file_names:
            return

        from install_pipeline import needs_extraction, is_rom

        game_id = str(task.get("game_id", ""))
        platform = self._get_platform_for_game(game_id)

        download_dir = self._get_dl_dir()

        # 收集所有需要解压的归档文件（多盘游戏可能有多个）
        archive_files = []
        for fname in file_names:
            fpath = os.path.join(download_dir, fname)
            if os.path.isfile(fpath) and needs_extraction(fname, platform):
                archive_files.append(fpath)

        # 非归档文件（ROM）→ 跳过解压，标记完成
        if not archive_files:
            decky.logger.info("_auto_extract: no archive, skipping extraction (platform=%s files=%s)",
                              platform, file_names)
            task["extract_status"] = "complete"
            task["extract_progress"] = 100
            first_file = os.path.join(download_dir, file_names[0]) if file_names else ""
            task["extract_output"] = download_dir
            if first_file and os.path.isfile(first_file):
                task["rom_path"] = first_file
            if task.get("game_id") and not task.get("import_status"):
                asyncio.create_task(self._auto_import_after_extract(task_id, task))
            return

        # 找 7zz 二进制
        plugin_dir = os.path.dirname(os.path.abspath(__file__))
        seven_zip = os.path.join(plugin_dir, "defaults", "bin", "7zz")
        if not os.path.isfile(seven_zip):
            decky.logger.warning("_auto_extract: 7zz not found at %s", seven_zip)
            return

        extract_root = os.path.join(os.path.expanduser("~"), "FreedeckGames")
        out_dir = os.path.join(extract_root, _build_extract_dir_name(task, archive_files[0]))
        import shutil

        # 第一个盘：清空目录；后续盘在循环中追加
        shutil.rmtree(out_dir, ignore_errors=True)
        os.makedirs(out_dir, exist_ok=True)

        task["extract_status"] = "running"
        task["extract_progress"] = 0
        task["extract_output"] = out_dir
        task["extract_task_id"] = task_id

        passwords_to_try = []
        catalog_pwd = task.get("archive_password", "")
        if catalog_pwd:
            passwords_to_try.append(catalog_pwd)
        passwords_to_try.append("oldmanemu.net")

        all_extracted = True
        last_error = ""

        # 逐个解压所有归档到同一目录
        for af in archive_files:
            decky.logger.info("_auto_extract: extracting %s -> %s", af, out_dir)
            extracted = False
            for pwd in passwords_to_try:
                try:
                    cmd_args = [seven_zip, "x", f"-p{pwd}", af, f"-o{out_dir}",
                                "-bsp1", "-y", "-aoa", "-sccUTF-8"]
                    proc = await asyncio.create_subprocess_exec(
                        *cmd_args, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL)

                    async def _read_progress(p):
                        import re
                        buf = b""; lp = 0.0
                        while True:
                            try: chunk = await p.stdout.read(4096)
                            except Exception: break
                            if not chunk: break
                            buf += chunk
                            pcts = re.findall(r'(\d{1,3})\s*%', buf.decode("utf-8", errors="replace"))
                            if pcts:
                                try: lp = float(pcts[-1]); task["extract_progress"] = min(99.0, lp)
                                except: pass
                            if len(buf) > 65536: buf = buf[-32768:]
                        return lp

                    pt = asyncio.create_task(_read_progress(proc))
                    await proc.wait()
                    try: await asyncio.wait_for(pt, timeout=5.0)
                    except: pass

                    if proc.returncode == 0:
                        extracted = True
                        decky.logger.info("_auto_extract: done %s", af)
                        break
                    else:
                        last_error = f"7zz exit code {proc.returncode} for {af}"
                        decky.logger.warning("_auto_extract: password retry (rc=%d) for %s", proc.returncode, af)
                except FileNotFoundError:
                    last_error = "7zz not found"; break
                except Exception as e:
                    last_error = str(e); break
            if not extracted:
                all_extracted = False
                decky.logger.error("_auto_extract: failed %s: %s", af, last_error)

        if not all_extracted:
            task["extract_status"] = "failed"
            task["extract_error"] = f"解压失败: {last_error}"
            return

        task["extract_progress"] = 100.0
        task["extract_status"] = "complete"
        decky.logger.info("_auto_extract: all %d archives extracted", len(archive_files))

        # 触发 Steam 导入（PC 游戏和模拟器都走同一链路）
        _gid = task.get("game_id", "")
        if _gid and not task.get("import_status"):
            asyncio.create_task(self._auto_import_after_extract(task_id, task))

    # ── SGDB 封面匹配 (6-layer fallback) ──

    async def _fetch_and_apply_covers_async(self, appid: int, steam_id: str, game: dict, extract_dir: str, task_id: str):
        """后台异步下载封面，不阻塞导入流程"""
        try:
            await self._fetch_and_apply_covers(appid, steam_id, game, extract_dir)
            decky.logger.info("_fetch_and_apply_covers_async: done for appid=%s", appid)
        except Exception as e:
            decky.logger.warning("_fetch_and_apply_covers_async: failed: %s", e)

    async def _fetch_and_apply_covers(self, appid: int, steam_id: str, game: dict, extract_dir: str):
        """6 层回退封面匹配 → 存入 _pending_artwork + 同步写 grid 目录备份"""
        title_en = str(game.get("title_en", "") or "").strip()
        title_zh = str(game.get("title_zh", "") or "").strip()
        title_full = str(game.get("title", "") or "").strip()
        dir_name = os.path.basename(extract_dir.rstrip("/"))
        cover_url = str(game.get("cover_url", "") or "").strip()

        userdata_dir = _find_steam_userdata_dir()
        grid_dir = os.path.join(userdata_dir, "config", "grid")
        os.makedirs(grid_dir, exist_ok=True)

        def save_asset(key: str, stem: str, data: bytes, image_format: str):
            normalized = _normalize_image_format(image_format) or "png"
            _write_grid_artwork(grid_dir, appid, stem, data, normalized)
            pending[key] = {
                "data": base64.b64encode(data).decode("ascii"),
                "format": normalized,
            }

        pending = {}

        # ── Layer 1-4: SGDB 多关键词搜索 ──
        search_terms = []
        seen = set()
        for t in [title_en, title_zh, title_full, dir_name]:
            t = t.strip()
            if t and t not in seen:
                search_terms.append(t)
                seen.add(t)

        sgdb_game_id = None
        for term in search_terms:
            decky.logger.info("_sgdb: searching term=%s", term[:60])
            sgdb_game_id = await self._sgdb_search(term)
            if sgdb_game_id:
                decky.logger.info("_sgdb: found game_id=%s via term=%s", sgdb_game_id, term[:60])
                break

        if sgdb_game_id:
            tasks_ = [
                self._sgdb_fetch_best("grids", sgdb_game_id),
                self._sgdb_fetch_best("heroes", sgdb_game_id),
                self._sgdb_fetch_best("logos", sgdb_game_id),
            ]
            results = await asyncio.gather(*tasks_, return_exceptions=True)

            asset_keys = [("grid", "p"), ("hero", "_hero"), ("logo", "_logo")]
            for (key, stem), result in zip(asset_keys, results):
                if isinstance(result, Exception) or not result:
                    continue
                try:
                    asset = await self._download_image(result, timeout=15)
                    if asset and asset.get("data"):
                        save_asset(key, stem, asset["data"], asset.get("format", "png"))
                        decky.logger.info("_sgdb: %s ready (SGDB)", key)
                except Exception as e:
                    decky.logger.warning("_sgdb: %s failed: %s", key, e)

            if pending:
                # SGDB 部分命中时补全 portrait：grids 端点返回的是 landscape 横版，
                # 但 Steam Deck 库主页需要竖版封面。缺失则从 panyiwei.xyz cover_url 兜底。
                if "grid" in pending and cover_url:
                    try:
                        asset = await self._download_image(cover_url, timeout=15)
                        if asset and asset.get("data"):
                            save_asset("grid", "p", asset["data"], asset.get("format", "png"))
                            decky.logger.info("_sgdb: portrait filled from cover_url=%s", cover_url[:80])
                    except Exception as e:
                        decky.logger.warning("_sgdb: portrait fill failed: %s", e)
                self._pending_artwork[appid] = pending
                decky.logger.info("_sgdb: SGDB success, %d assets pending", len(pending))
                return

        # ── Layer 2: Steam CDN ──
        decky.logger.info("_sgdb: Layer 2 Steam CDN search for %s", title_en)
        steam_match = await self._steam_search(title_en)
        if not steam_match and title_zh:
            steam_match = await self._steam_search(title_zh)
        steam_appid = int(steam_match.get("appid", 0) or 0) if isinstance(steam_match, dict) else 0
        if steam_appid:
            steam_assets = await self._steam_fetch(steam_appid, steam_match)
            if steam_assets:
                cdn_pending = {}
                for key, asset in steam_assets.items():
                    try:
                        stem = {"grid": "p", "hero": "_hero", "logo": "_logo"}[key]
                        save_asset(key, stem, asset["data"], asset.get("format", "png"))
                        cdn_pending[key] = pending[key]
                    except Exception as e:
                        decky.logger.warning("_sgdb: Steam CDN %s failed: %s", key, e)
                if cdn_pending:
                    # Steam CDN 部分命中时同样补全 portrait
                    if "grid" in cdn_pending and cover_url:
                        try:
                            asset = await self._download_image(cover_url, timeout=15)
                            if asset and asset.get("data"):
                                save_asset("grid", "p", asset["data"], asset.get("format", "png"))
                                decky.logger.info("_sgdb: portrait filled from cover_url for CDN")
                        except Exception as e:
                            decky.logger.warning("_sgdb: portrait fill failed: %s", e)
                    self._pending_artwork[appid] = cdn_pending
                    decky.logger.info("_sgdb: Steam CDN success, %d assets pending", len(cdn_pending))
                    return

        # ── Layer 3: 目录 cover_url 兜底 ──
        if cover_url:
            decky.logger.info("_sgdb: Layer 5 fallback cover_url=%s", cover_url[:80])
            try:
                asset = await self._download_image(cover_url, timeout=15)
                if asset and asset.get("data"):
                    save_asset("grid", "p", asset["data"], asset.get("format", "png"))
                    self._pending_artwork[appid] = pending
                    decky.logger.info("_sgdb: Layer 5 cover ready")
                    return
            except Exception as e:
                decky.logger.warning("_sgdb: Layer 5 failed: %s", e)

        decky.logger.info("_sgdb: no remote artwork resolved for %s", title_zh or title_en or title_full or dir_name)

    async def get_pending_artwork(self, payload=None, appid: int = 0):
        """前端调用：获取待应用的封面数据。"""
        appid = int(payload.get("appid", appid) if isinstance(payload, dict) else (appid or 0))
        data = self._pending_artwork.get(appid)
        return {"status": "success", "data": data if data else None}

    async def import_applied(self, payload=None, task_id: str = ""):
        """前端调用：SetShortcutName + SetCustomArtwork 完成，标记导入完成"""
        task_id = str(payload.get("task_id", task_id) if isinstance(payload, dict) else str(task_id or ""))
        if task_id and task_id in self._downloads:
            appid = int(self._downloads[task_id].get("steam_appid", 0) or 0)
            if appid:
                self._pending_artwork.pop(appid, None)
            self._downloads[task_id]["import_status"] = "complete"
            decky.logger.info("import_applied: task %s marked complete", task_id)

            # 写入 installed_games.json
            from installed_games import add_installed_game
            t = self._downloads[task_id]
            game_id = str(t.get("game_id", ""))
            title_zh = str(t.get("game_title", ""))
            # 从 catalog 反查英文名
            title_en = ""
            for item in self._catalog_items:
                if str(item.get("game_id", "")) == game_id:
                    title_en = str(item.get("title_en", "") or item.get("title", ""))
                    break
            add_installed_game({
                "game_id": game_id,
                "title_zh": title_zh,
                "title_en": title_en,
                "steam_appid": int(appid or 0),
                "install_dir": str(t.get("extract_output", "")),
                "size_bytes": int(t.get("file_size", 0) or 0),
                "cover_url": str(t.get("cover_url", "")),
                "platform": str(t.get("platform", "")).strip().lower(),
                "game_type": str(t.get("game_type", "")),
            })
        return _ok({"ok": True})

    async def _sgdb_search(self, term: str):
        """搜索 SGDB，返回第一个匹配的 game id（失败返回 None）"""
        import aiohttp, urllib.parse
        try:
            url = f"{SGDB_API_BASE}/search/autocomplete/{_url_encode_rfc3986(term)}"
            headers = {"Authorization": f"Bearer {SGDB_API_KEY}", "Accept": "application/json"}
            timeout = aiohttp.ClientTimeout(total=10, connect=5)
            connector = aiohttp.TCPConnector(ssl=_http_ssl_context())
            async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
                async with session.get(url, headers=headers) as resp:
                    if resp.status != 200:
                        body = await resp.text()
                        decky.logger.warning("_sgdb_search(%s): http %s body=%s", term[:40], resp.status, body[:160])
                        return None
                    data = await resp.json()
                    if data.get("success") and data.get("data"):
                        return data["data"][0].get("id")
        except Exception as e:
            decky.logger.warning("_sgdb_search(%s): %s", term[:40], e)
        return None

    async def _sgdb_fetch_best(self, asset_type: str, game_id):
        """拉取 SGDB 资产列表，返回第一张图片的 URL（失败返回 None）"""
        import aiohttp, urllib.parse
        try:
            # 不设过滤，拉全部资产，取第一张（SGDB 默认按得分排序）
            url = f"{SGDB_API_BASE}/{asset_type}/game/{game_id}?page=0"
            headers = {"Authorization": f"Bearer {SGDB_API_KEY}", "Accept": "application/json"}
            timeout = aiohttp.ClientTimeout(total=10, connect=5)
            connector = aiohttp.TCPConnector(ssl=_http_ssl_context())
            async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
                async with session.get(url, headers=headers) as resp:
                    if resp.status != 200:
                        body = await resp.text()
                        decky.logger.warning("_sgdb_fetch(%s/%s): http %s body=%s", asset_type, game_id, resp.status, body[:160])
                        return None
                    data = await resp.json()
                    if data.get("success") and data.get("data"):
                        return data["data"][0].get("url")
        except Exception as e:
            decky.logger.warning("_sgdb_fetch(%s/%s): %s", asset_type, game_id, e)
        return None

    async def _download_image(self, url: str, timeout: int = 15):
        """下载图片，返回 {data, content_type, format}。"""
        import aiohttp
        try:
            t = aiohttp.ClientTimeout(total=timeout, connect=5)
            connector = aiohttp.TCPConnector(ssl=_http_ssl_context())
            async with aiohttp.ClientSession(timeout=t, connector=connector) as session:
                async with session.get(url) as resp:
                    if resp.status == 200:
                        data = await resp.read()
                        if data:
                            content_type = str(resp.headers.get("Content-Type", "") or "")
                            return {
                                "data": data,
                                "content_type": content_type,
                                "format": _guess_image_format(url=url, content_type=content_type, data=data),
                            }
        except Exception as e:
            decky.logger.warning("_download_image: %s", e)
        return None

    # ── Steam CDN 封面拉取（免费，无 API Key）────

    async def _steam_search(self, name: str):
        """搜索 Steam 商店，返回第一个匹配条目（失败返回 None）"""
        import aiohttp, urllib.parse
        try:
            url = f"https://steamcommunity.com/actions/SearchApps/{urllib.parse.quote(name)}"
            timeout = aiohttp.ClientTimeout(total=10, connect=5)
            connector = aiohttp.TCPConnector(ssl=_http_ssl_context())
            async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
                async with session.get(url) as resp:
                    if resp.status != 200:
                        return None
                    results = await resp.json()
                    if not isinstance(results, list) or not results:
                        return None
                    # 精确匹配（大小写不敏感）
                    name_lower = name.strip().lower()
                    for item in results:
                        if not isinstance(item, dict):
                            continue
                        item_name = str(item.get("name", "")).strip().lower()
                        if item_name == name_lower:
                            return item
                    # 模糊匹配：第一个结果的 name 包含搜索词
                    first = results[0]
                    if isinstance(first, dict):
                        fn = str(first.get("name", "")).strip().lower()
                        if name_lower in fn or fn in name_lower:
                            return first
        except Exception as e:
            decky.logger.warning("_steam_search(%s): %s", name[:40], e)
        return None

    async def _steam_fetch(self, steam_appid: int, steam_match: dict | None = None):
        """从 Steam 官方资源拉取 grid/hero/logo，返回 {key: {data, format}}。"""
        import aiohttp, urllib.parse
        assets = {}
        cdn_base = f"https://cdn.cloudflare.steamstatic.com/steam/apps/{steam_appid}"
        targets = [
            ("grid", f"{cdn_base}/library_600x900.jpg"),
            ("hero", f"{cdn_base}/library_hero.jpg"),
            ("logo", f"{cdn_base}/logo.png"),
        ]
        timeout = aiohttp.ClientTimeout(total=10, connect=5)
        connector = aiohttp.TCPConnector(ssl=_http_ssl_context())
        async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
            for key, url in targets:
                try:
                    async with session.get(url) as resp:
                        if resp.status == 200:
                            data = await resp.read()
                            if data and len(data) > 1024:  # 至少 1KB
                                content_type = str(resp.headers.get("Content-Type", "") or "")
                                assets[key] = {
                                    "data": data,
                                    "content_type": content_type,
                                    "format": _guess_image_format(url=url, content_type=content_type, data=data),
                                }
                except Exception:
                    pass

            missing = {"grid", "hero", "logo"} - set(assets.keys())
            if missing:
                try:
                    appdetails_url = "https://store.steampowered.com/api/appdetails?" + urllib.parse.urlencode({
                        "appids": str(int(steam_appid)),
                        "cc": "US",
                        "l": "english",
                    })
                    async with session.get(appdetails_url, headers={"Accept": "application/json", "User-Agent": "Mozilla/5.0"}) as resp:
                        if resp.status == 200:
                            payload = await resp.json()
                            entry = ((payload or {}).get(str(int(steam_appid))) or {}).get("data") or {}
                            fallback_urls = {
                                "grid": [
                                    entry.get("library_600x900"),
                                    entry.get("header_image"),
                                    entry.get("capsule_image"),
                                    (steam_match or {}).get("logo"),
                                    (steam_match or {}).get("icon"),
                                ],
                                "hero": [
                                    entry.get("library_hero"),
                                    entry.get("background_raw"),
                                    entry.get("background"),
                                    entry.get("header_image"),
                                ],
                                "logo": [
                                    entry.get("logo"),
                                    (steam_match or {}).get("logo"),
                                    entry.get("capsule_imagev5"),
                                    entry.get("capsule_image"),
                                    (steam_match or {}).get("icon"),
                                ],
                            }
                            for key in list(missing):
                                for fallback_url in fallback_urls.get(key, []):
                                    if not fallback_url:
                                        continue
                                    asset = await self._download_image(str(fallback_url), timeout=15)
                                    if asset and asset.get("data"):
                                        assets[key] = asset
                                        break
                except Exception as e:
                    decky.logger.warning("_steam_fetch(%s): appdetails fallback failed: %s", steam_appid, e)
        return assets

    async def download_list(self, payload=None):
        return _ok({"tasks": list(self._downloads.values())})

    async def download_cancel(self, payload=None, task_id: str = ""):
        task_id = str(payload.get("task_id", task_id) if isinstance(payload, dict) else str(task_id or ""))
        if not task_id or task_id not in self._downloads:
            return _err("任务不存在")
        task = self._downloads.pop(task_id, None)
        if _rust_download and _core:
            tid_c = ctypes.c_char_p(task_id.encode())
            _core.nfc_download_cancel(tid_c)
        elif self._engine and self._engine.ready and task:
            gid = task.get("gid", "")
            if gid:
                try:
                    await self._engine.rpc.remove(gid)
                except Exception:
                    pass
        return _ok({"cancelled": True})

    async def download_pause(self, payload=None, task_id: str = ""):
        task_id = str(payload.get("task_id", task_id) if isinstance(payload, dict) else str(task_id or ""))
        if not task_id or task_id not in self._downloads:
            return _err("任务不存在")
        task = self._downloads[task_id]
        if self._engine and self._engine.ready:
            gid = task.get("gid", "")
            if gid:
                await self._engine.rpc.pause(gid)
                task["status"] = "paused"
        return _ok(task)

    async def download_resume(self, payload=None, task_id: str = ""):
        task_id = str(payload.get("task_id", task_id) if isinstance(payload, dict) else str(task_id or ""))
        if not task_id or task_id not in self._downloads:
            return _err("任务不存在")
        task = self._downloads[task_id]
        if self._engine and self._engine.ready:
            gid = task.get("gid", "")
            if gid:
                await self._engine.rpc.resume(gid)
                task["status"] = "downloading"
        return _ok(task)

    # ── 解压 callable ──────────────────────

    async def extract_detect(self, payload=None, archive_path: str = ""):
        """检测归档信息：格式、是否分卷、主卷路径、同目录所有分卷"""
        archive_path = str(payload.get("archive_path", archive_path) if isinstance(payload, dict) else str(archive_path or ""))
        if not archive_path:
            return _err("archive_path required")
        if not _core:
            return _err("Rust core not loaded")
        try:
            loop = asyncio.get_event_loop()
            result_c = await loop.run_in_executor(
                None,
                lambda: _core.nfc_extract_detect(ctypes.c_char_p(archive_path.encode()))
            )
            result = ctypes.cast(result_c, ctypes.c_char_p).value.decode() if result_c else "{}"
            _core.nfc_free(result_c)
            return _ok(json.loads(result))
        except Exception as e:
            return _err(str(e))

    async def extract_start(self, payload=None, archive_path: str = "",
                            output_dir: str = "", password: str = ""):
        """启动解压（自动处理分卷主卷定位）"""
        archive_path = str(payload.get("archive_path", archive_path) if isinstance(payload, dict) else str(archive_path or ""))
        output_dir = str(payload.get("output_dir", output_dir) if isinstance(payload, dict) else str(output_dir or ""))
        password = str(payload.get("password", password) if isinstance(payload, dict) else str(password or ""))
        if not archive_path:
            return _err("archive_path required")
        if not output_dir:
            output_dir = os.path.join(
                os.path.dirname(archive_path),
                "_extracted",
                _safe_path_component(_archive_display_name(archive_path), fallback="game"),
            )
        if not _core:
            return _err("Rust core not loaded")
        try:
            loop = asyncio.get_event_loop()
            result_c = await loop.run_in_executor(
                None,
                lambda: _core.nfc_extract_start(
                    ctypes.c_char_p(archive_path.encode()),
                    ctypes.c_char_p(output_dir.encode()),
                    ctypes.c_char_p(password.encode()),
                )
            )
            result = ctypes.cast(result_c, ctypes.c_char_p).value.decode() if result_c else "{}"
            _core.nfc_free(result_c)
            data = json.loads(result)
            if "error" in data:
                return _err(data["error"])
            return _ok(data)
        except Exception as e:
            return _err(str(e))

    async def extract_poll(self, payload=None, task_id: str = ""):
        """轮询解压进度"""
        task_id = str(payload.get("task_id", task_id) if isinstance(payload, dict) else str(task_id or ""))
        if not task_id:
            return _err("task_id required")
        if not _core:
            return _err("Rust core not loaded")
        try:
            loop = asyncio.get_event_loop()
            result_c = await loop.run_in_executor(
                None,
                lambda: _core.nfc_extract_poll(ctypes.c_char_p(task_id.encode()))
            )
            result = ctypes.cast(result_c, ctypes.c_char_p).value.decode() if result_c else "{}"
            _core.nfc_free(result_c)
            return _ok(json.loads(result))
        except Exception as e:
            return _err(str(e))

    async def extract_cancel(self, payload=None, task_id: str = ""):
        """取消解压"""
        task_id = str(payload.get("task_id", task_id) if isinstance(payload, dict) else str(task_id or ""))
        if not task_id:
            return _err("task_id required")
        if not _core:
            return _err("Rust core not loaded")
        try:
            loop = asyncio.get_event_loop()
            rc = await loop.run_in_executor(
                None,
                lambda: _core.nfc_extract_cancel(ctypes.c_char_p(task_id.encode()))
            )
            if rc == 0:
                return _ok({"cancelled": True})
            return _err("task not found")
        except Exception as e:
            return _err(str(e))

    async def extract_list(self, payload=None, archive_path: str = "", password: str = ""):
        """列出归档内文件"""
        archive_path = str(payload.get("archive_path", archive_path) if isinstance(payload, dict) else str(archive_path or ""))
        password = str(payload.get("password", password) if isinstance(payload, dict) else str(password or ""))
        if not archive_path:
            return _err("archive_path required")
        if not _core:
            return _err("Rust core not loaded")
        try:
            loop = asyncio.get_event_loop()
            result_c = await loop.run_in_executor(
                None,
                lambda: _core.nfc_extract_list(
                    ctypes.c_char_p(archive_path.encode()),
                    ctypes.c_char_p(password.encode()),
                )
            )
            result = ctypes.cast(result_c, ctypes.c_char_p).value.decode() if result_c else "[]"
            _core.nfc_free(result_c)
            return _ok({"entries": json.loads(result)})
        except Exception as e:
            return _err(str(e))

    async def tianyi_get_panel_state(self):
        login = dict(self._cached_login) if self._cached_login else self._prime_login_cache_from_store()
        self._schedule_login_refresh()
        return _ok({"login": login, "tasks": [], "installed": []})

    async def _refresh_login_cache(self, generation: int | None = None):
        """后台刷新登录状态缓存"""
        try:
            if not self._ok:
                self._set_cached_login(self._empty_login_state())
                return
            cred = await self._session_mgr.ensure_valid(self._tianyi)
            if generation is not None and generation != self._login_refresh_generation:
                return
            self._set_cached_login({"logged_in": True, "account_name": cred.account_name, "message": "ok"})
        except asyncio.CancelledError:
            raise
        except Exception:
            if generation is not None and generation != self._login_refresh_generation:
                return
            self._set_cached_login(self._empty_login_state("已过期"))
        finally:
            if generation is None or generation == self._login_refresh_generation:
                self._login_refresh_task = None

    # ── 游戏目录 ────────────

    async def list_catalog(self, payload=None, page: int = 1, page_size: int = 30, query: str = "", platform: str = ""):
        if not self._catalog_loaded:
            return _err("目录未加载")
        try:
            if isinstance(payload, dict):
                page = int(payload.get("page", page) or 1)
                page_size = int(payload.get("page_size", payload.get("pageSize", page_size)) or 30)
                query = str(payload.get("query", payload.get("q", query)) or "")
                platform = str(payload.get("platform", platform) or "")
            elif isinstance(page, dict):
                legacy_payload = page
                page = int(legacy_payload.get("page", 1) or 1)
                page_size = int(legacy_payload.get("page_size", legacy_payload.get("pageSize", page_size)) or 30)
                query = str(legacy_payload.get("query", legacy_payload.get("q", query)) or "")
                platform = str(legacy_payload.get("platform", platform) or "")

            # 按平台筛选
            items = self._catalog_items
            platform = platform.strip().lower()
            if platform and platform != "all":
                items = [it for it in items if (it.get("platform") or "").lower() == platform]

            # 搜索筛选
            query = query.strip().lower()
            if query:
                items = [it for it in items if (
                    query in (it.get("title_zh") or "").lower()
                    or query in (it.get("title") or "").lower()
                    or query in (it.get("title_en") or "").lower()
                )]

            total = len(items)
            page = max(1, page)
            page_size = min(100, max(1, page_size))
            start = (page - 1) * page_size
            end = start + page_size
            page_items = self._enrich_cover(items[start:end])

            return _ok({
                "items": page_items,
                "page": page,
                "page_size": page_size,
                "total": total,
                "has_more": end < total,
            })
        except Exception as e:
            decky.logger.error("list_catalog failed: %s", e)
            return _err(str(e))

    # ── 已安装游戏 ──────

    async def list_installed_games(self):
        """返回已安装游戏列表（首次调用时从 shortcuts.vdf 迁移）。"""
        from installed_games import handle_list_installed_games
        result = await handle_list_installed_games(self, _load_steam_shortcuts)
        return _ok(result["data"]) if result.get("status") == "success" else _err(result.get("message", ""))

    async def uninstall_game(self, payload=None, game_id: str = ""):
        """卸载游戏：删除安装目录 + 清理 JSON。"""
        game_id = str(payload.get("game_id", game_id) if isinstance(payload, dict) else str(game_id or ""))
        from installed_games import handle_uninstall_game
        result = await handle_uninstall_game(game_id)
        return _ok(result["data"]) if result.get("status") == "success" else _err(result.get("message", ""))

    # ── 模拟器管理 ──────

    async def emulator_list(self):
        """列出所有可用模拟器及安装状态。"""
        decky.logger.info("Decky callable: emulator_list")
        from emulator_registry import EMULATOR_PLATFORMS, fetch_emulator_sources, init_cache
        init_cache()
        sources = {}
        try:
            sources = await asyncio.to_thread(fetch_emulator_sources)
        except Exception as e:
            decky.logger.warning("emulator_list: csv fetch failed %s", e)
        result = []
        for plat_id, info in EMULATOR_PLATFORMS.items():
            installed = os.path.isdir(self._emulator_install_dir(info))
            have_appimage = bool(self._find_emulator_appimage(info))
            src = {}
            for key in (plat_id, info["adapter"], info["game_id"]):
                s = sources.get(key)
                if s: src = s; break
            result.append({
                "platform": plat_id,
                "display": info["display"],
                "adapter": info["adapter"],
                "icon": info["icon"],
                "installed": installed and have_appimage,
                "has_download": bool(src.get("share_url")),
            })
        return _ok({"emulators": result})

    async def emulator_download(self, payload=None, platform: str = ""):
        """下载模拟器安装包（复用现有 download_start 流程）。"""
        if isinstance(payload, dict):
            platform = str(payload.get("platform", platform) or "")
        from emulator_registry import get_emulator_source, EMULATOR_PLATFORMS
        plat = EMULATOR_PLATFORMS.get(platform)
        if not plat:
            return _err(f"未知平台: {platform}")
        src = get_emulator_source(platform)
        if not src or not src.get("share_url"):
            return _err(f"{plat['display']} 下载链接为空")
        # 复用 download_start 的逻辑
        return await self.download_start(
            payload=None,
            share_url=src["share_url"],
            file_name=src.get("file_name") or f"{plat['adapter']}.7z",
            game_id=plat["game_id"],
            game_title=plat["display"] + " 模拟器",
            access_code=src.get("pwd", ""),
            file_size=src.get("file_size", 0),
        )

    async def emulator_launch_appimage(self, payload=None, platform: str = ""):
        """获取已安装模拟器 AppImage 路径供前端启动。"""
        if isinstance(payload, dict):
            platform = str(payload.get("platform", platform) or "")
        decky.logger.info("Decky callable: emulator_launch_appimage platform=%s", platform)
        from emulator_registry import EMULATOR_PLATFORMS
        plat = EMULATOR_PLATFORMS.get(platform)
        if not plat:
            return _err(f"未知平台: {platform}")
        appimage = self._find_emulator_appimage(plat)
        if not appimage:
            return _err(f"未找到 {plat['display']} AppImage")
        return _ok({
            "appimage": appimage,
            "install_dir": self._emulator_install_dir(plat),
            "platform": platform,
            "display": plat["display"],
        })

    async def steam_finish_import(self, payload=None, task_id: str = "", appid: int = 0):
        """前端 AddShortcut 完成后回传 appid，后端继续兼容层/封面等配置。"""
        if isinstance(payload, dict):
            task_id = str(payload.get("task_id", task_id) or "")
            appid = int(payload.get("appid", appid) or 0)
        decky.logger.info("Decky callable: steam_finish_import task_id=%s appid=%s", task_id, appid)
        if not task_id or not appid:
            return _err("参数不足：需要 task_id 和 appid")
        task = self._downloads.get(task_id)
        if not task:
            return _err(f"任务不存在: {task_id}")
        task["steam_appid"] = appid
        task["import_status"] = "applying"
        decky.logger.info("steam_finish_import: appid=%s set, continuing import", appid)

        userdata_dir = _find_steam_userdata_dir()
        steam_id = os.path.basename(userdata_dir)
        shortcuts_path = os.path.join(userdata_dir, "config", "shortcuts.vdf")
        game_id = str(task.get("game_id", ""))
        game_title = str(task.get("game_title", ""))
        exe_path = str(task.get("steam_exe", ""))
        platform = ""

        for item in self._catalog_items:
            if str(item.get("game_id", "")) == game_id:
                platform = str(item.get("platform", "") or "").lower()
                break

        # PC 游戏挂兼容层
        is_pc = (platform == "pc")
        if is_pc:
            compat_result = _set_shortcut_compat_tool(userdata_dir, appid, DEFAULT_SHORTCUT_COMPAT_TOOL)
            task["compat_tool"] = DEFAULT_SHORTCUT_COMPAT_TOOL
            decky.logger.info("steam_finish_import: compat tool set ok=%s", compat_result.get("ok"))

        # 模拟器工具：设置 LaunchOptions
        is_emulator_tool = str(game_id).endswith("_emulator")
        if is_emulator_tool:
            try:
                _resolve_shortcut_duplicates(shortcuts_path, exe_path, game_title)
            except Exception:
                pass
            try:
                from vdf import binary_load, binary_dump
                with open(shortcuts_path, "rb") as f:
                    vdf_data = binary_load(f)
                for _idx, _entry in vdf_data.get("shortcuts", {}).items():
                    if isinstance(_entry, dict) and str(_entry.get("appid", "")) == str(appid):
                        _entry["LaunchOptions"] = "QT_QPA_PLATFORM=wayland"
                        break
                with open(shortcuts_path, "wb") as f:
                    binary_dump(vdf_data, f)
            except Exception as e:
                decky.logger.warning("steam_finish_import: VDF patch failed: %s", e)

        # ROM 游戏：VDF 中修改 AppName 和 LaunchOptions
        launch_opts = str(task.get("launch_options", ""))
        rom_path = str(task.get("rom_path", ""))
        if launch_opts or rom_path:
            try:
                from vdf import binary_load, binary_dump
                with open(shortcuts_path, "rb") as f:
                    vdf_data = binary_load(f)
                for _idx, _entry in vdf_data.get("shortcuts", {}).items():
                    if isinstance(_entry, dict) and str(_entry.get("appid", "")) == str(appid):
                        if game_title:
                            _entry["AppName"] = game_title
                        if launch_opts:
                            _entry["LaunchOptions"] = launch_opts
                        if rom_path:
                            _entry["StartDir"] = os.path.dirname(rom_path) or os.path.dirname(exe_path)
                        break
                with open(shortcuts_path, "wb") as f:
                    binary_dump(vdf_data, f)
                decky.logger.info("steam_finish_import: VDF patched name=%s launch=%s", game_title, launch_opts[:60])
            except Exception as e:
                decky.logger.warning("steam_finish_import: VDF patch failed: %s", e)

        # 封面
        extract_output = str(task.get("extract_output", ""))
        game_info = {}
        for item in self._catalog_items:
            if str(item.get("game_id", "")) == game_id:
                game_info = item
                break
        asyncio.create_task(self._fetch_and_apply_covers_async(appid, steam_id, game_info, extract_output, task_id))

        return _ok({"appid": appid, "task_id": task_id})

    async def emulator_bootstrap(self, payload=None, platform: str = ""):
        """执行模拟器安装后初始化配置。"""
        if isinstance(payload, dict):
            platform = str(payload.get("platform", platform) or "")
        decky.logger.info("Decky callable: emulator_bootstrap platform=%s", platform)
        from emulator_adapters import bootstrap_emulator, get_adapter
        from emulator_registry import EMULATOR_PLATFORMS
        plat = EMULATOR_PLATFORMS.get(platform)
        if not plat:
            return _err(f"未知平台: {platform}")
        install_dir = self._emulator_install_dir(plat)

        # 检查安装目录是否已有 AppImage
        adapter = get_adapter(platform)
        appimage = adapter.find_appimage(install_dir) if adapter else None

        if not appimage:
            # 从解压目录复制文件到安装目录
            download_dir = os.path.join(_state_dir(), "downloads")
            extract_root = os.path.join(download_dir, "_extracted")
            if os.path.isdir(extract_root):
                import shutil
                for name in os.listdir(extract_root):
                    src_dir = os.path.join(extract_root, name)
                    if not os.path.isdir(src_dir):
                        continue
                    test_appimage = adapter.find_appimage(src_dir) if adapter else None
                    if test_appimage:
                        decky.logger.info("emulator_bootstrap: copying extracted files from %s to %s", src_dir, install_dir)
                        for item in os.listdir(src_dir):
                            s = os.path.join(src_dir, item)
                            d = os.path.join(install_dir, item)
                            try:
                                if os.path.isdir(s):
                                    if not os.path.exists(d):
                                        shutil.copytree(s, d)
                                else:
                                    shutil.copy2(s, d)
                            except Exception as e:
                                decky.logger.warning("emulator_bootstrap: copy failed %s: %s", item, e)
                        break

        result = bootstrap_emulator(platform, install_dir)
        decky.logger.info("emulator_bootstrap result: ok=%s messages=%s", result.get("ok"), result.get("messages"))
        return _ok(result)

    async def emulator_import_steam(self, payload=None, platform: str = ""):
        """将已安装的模拟器导入 Steam 库。"""
        if isinstance(payload, dict):
            platform = str(payload.get("platform", platform) or "")
        decky.logger.info("Decky callable: emulator_import_steam platform=%s", platform)
        from emulator_adapters import get_adapter
        from emulator_registry import EMULATOR_PLATFORMS
        plat = EMULATOR_PLATFORMS.get(platform)
        if not plat:
            return _err(f"未知平台: {platform}")
        adapter = get_adapter(platform)
        if not adapter:
            return _err(f"未找到 {platform} 适配器")
        install_dir = self._emulator_install_dir(plat)
        appimage = adapter.find_appimage(install_dir)
        if not appimage:
            return _err(f"未找到 {plat['display']} AppImage")
        # Steam 快捷方式导入
        try:
            from steam_shortcuts import upsert_shortcut
            appid = upsert_shortcut(
                app_name=plat["display"] + " 模拟器",
                exe=appimage,
                start_dir=install_dir,
                launch_options="",
                tags=["Freedeck", "Emulator"],
            )
            decky.logger.info("emulator_import_steam: upsert_shortcut OK appid=%s platform=%s", appid, platform)
            return _ok({"appid": appid, "platform": platform})
        except Exception as e:
            decky.logger.error("emulator_import_steam: upsert_shortcut failed: %s", e)
            return _ok({"appimage": appimage, "platform": platform, "steam_import_failed": str(e)})

    def _emulator_install_dir(self, info: dict) -> str:
        """模拟器安装目录：{install_path}/Emulators/{platform}"""
        import json as _json
        settings = self._load_settings_dict()
        base = settings.get("install_path") or os.path.join(os.path.expanduser("~"), "FreedeckGames")
        d = os.path.join(base, "Emulators", info["id"])
        os.makedirs(d, exist_ok=True)
        return d

    def _find_emulator_appimage(self, info: dict) -> Optional[str]:
        d = self._emulator_install_dir(info)
        if not os.path.isdir(d):
            return None
        for root, _, files in os.walk(d):
            for f in files:
                if f.lower().endswith(".appimage"):
                    return os.path.join(root, f)
        return None

    # ── 模拟器管理中心 callable ──────────

    async def emu_manager_list(self):
        """列出所有模拟器状态（供 EmuManagerPage 使用）。"""
        import glob as _glob
        emu_base = os.path.join(
            self._load_settings_dict().get("install_path", os.path.join(os.path.expanduser("~"), "FreedeckGames")),
            "Emulators",
        )
        platforms = [
            {"id":"switch","name":"Switch","icon":"switch-yuzu.svg","color":"#e74a2f"},
            {"id":"ps1","name":"PS1","icon":"ps1-duckstation.svg","color":"#7f8cff"},
            {"id":"ps2","name":"PS2","icon":"ps2-pcsx2.svg","color":"#3b83ff"},
            {"id":"ps3","name":"PS3","icon":"ps3-rpcs3.svg","color":"#7f6cff"},
            {"id":"psp","name":"PSP","icon":"psp-ppsspp.png","color":"#f39a30"},
            {"id":"3ds","name":"3DS","icon":"3ds-lime.svg","color":"#db7a34"},
            {"id":"gba","name":"GBA","icon":"gba-mgba.png","color":"#8db43f"},
            {"id":"nds","name":"NDS","icon":"nds.svg","color":"#57b198"},
            {"id":"fc","name":"FC","icon":"fc-nestopia.svg","color":"#f08a35"},
            {"id":"md","name":"MD","icon":"md-blastem-og.png","color":"#4d96e8"},
        ]
        STEAM_GUID = "03000000de280000ff11000001000000"
        result = []
        for pl in platforms:
            d = os.path.join(emu_base, pl["id"])
            aimg = None
            if os.path.isdir(d):
                for r, _, fs in os.walk(d):
                    for f in fs:
                        if f.lower().endswith(".appimage"):
                            aimg = os.path.join(r, f); break
                    if aimg: break
            installed = aimg is not None

            # 配置检测
            cfg_ok, cfg_total = 0, 5
            if installed:
                cfgs = {"switch":"user/config/qt-config.ini","ps1":"portable_data/settings/settings.ini",
                        "ps2":"inis/PCSX2.ini","ps3":"config.yml","psp":"memstick/PSP/SYSTEM/ppsspp.ini",
                        "3ds":"user/config/qt-config.ini","gba":"qt.ini","nds":"melonDS.ini",
                        "fc":"nestopia.cfg","md":"default.cfg"}
                cp = os.path.join(d, cfgs.get(pl["id"],""))
                if not os.path.isfile(cp):
                    for a in _glob.glob(os.path.join(d,"**","*.ini"),recursive=True): cp=a;break
                try:
                    with open(cp,encoding="utf-8",errors="ignore") as fh: c=fh.read().lower()
                    chk = [
                        STEAM_GUID.lower() in c or "28de-11ff" in c,
                        any(k in c for k in ["button_a","buttoncross","joypad_a","joy_a","joy_","player_0","pad.0."]),
                        any(k in c for k in ["language = zh","language=zh","language_index"]),
                        any(k in c for k in ["language_index","region_index","system_language","language = 1"]),
                        any(k in c for k in ["vulkan","opengl","renderer","backend"]),
                    ]
                    cfg_ok = sum(1 for v in chk if v)
                except: pass

            # BIOS
            bios_s = "n/a"
            pats = {"switch":["prod.keys","title.keys"],"ps1":["scph"],"ps2":["scph","ps2","bios"],
                    "ps3":["PS3UPDAT.PUP"],"nds":["firmware.bin","bios7.bin","bios9.bin"],
                    "gba":["gba_bios.bin"]}.get(pl["id"],[])
            if pats and installed:
                ok = sum(1 for p in pats if any(p.lower() in f.lower() for _,_,fs in os.walk(d) for f in fs))
                bios_s = f"{ok}/{len(pats)}"

            result.append({"platform":pl["id"],"name":pl["name"],"icon":pl["icon"],"color":pl["color"],
                           "installed":installed,"config_ok":cfg_ok,"config_total":cfg_total,"bios":bios_s})
        return _ok(result)

    async def emu_manager_launch(self, payload=None, platform: str = ""):
        """启动模拟器。"""
        if isinstance(payload, dict):
            platform = str(payload.get("platform", platform) or "")
        emu_base = os.path.join(
            self._load_settings_dict().get("install_path", os.path.join(os.path.expanduser("~"), "FreedeckGames")),
            "Emulators",
        )
        d = os.path.join(emu_base, platform)
        aimg = None
        if os.path.isdir(d):
            for r, _, fs in os.walk(d):
                for f in fs:
                    if f.lower().endswith(".appimage"): aimg = os.path.join(r,f); break
                if aimg: break
        if not aimg:
            return _err(f"未找到 {platform} AppImage")
        import subprocess as _sp
        try:
            _sp.Popen([aimg], start_new_session=True, stdout=_sp.DEVNULL, stderr=_sp.DEVNULL)
            return _ok({"launched": True})
        except Exception as e:
            return _err(str(e))

    async def emu_manager_bootstrap(self, payload=None, platform: str = ""):
        """触发模拟器自动配置。"""
        if isinstance(payload, dict):
            platform = str(payload.get("platform", platform) or "")
        try:
            from emulator_adapters import bootstrap_emulator
            emu_base = os.path.join(
                self._load_settings_dict().get("install_path", os.path.join(os.path.expanduser("~"), "FreedeckGames")),
                "Emulators",
            )
            result = bootstrap_emulator(platform, os.path.join(emu_base, platform))
            return _ok(result)
        except Exception as e:
            return _err(str(e))

    def _load_settings_dict(self) -> dict:
        """加载当前设置（合并默认值）。"""
        import json as _json, copy as _copy
        defaults = self._default_settings()
        path = self._settings_path()
        if os.path.exists(path):
            try:
                with open(path, "r") as f:
                    saved = _json.load(f)
                defaults.update(saved)
            except Exception:
                pass
        return defaults

    def _get_dl_dir(self) -> str:
        s = self._load_settings_dict()
        dl = s.get("download_path") or os.path.join(os.path.expanduser("~"), "FreedeckGames", "freedeckInstall")
        os.makedirs(dl, exist_ok=True)
        return dl

    # ── 设置 ────────────

    def _settings_path(self):
        return os.path.join(_state_dir(), "settings.json")

    async def get_settings(self):
        """读取所有设置项（缺失键用默认值补齐，旧路径自动迁移）。"""
        import json
        defaults = self._default_settings()
        try:
            path = self._settings_path()
            if os.path.exists(path):
                with open(path, "r") as f:
                    saved = json.load(f)
                # 迁移旧默认路径
                home = os.path.expanduser("~")
                old_bad = {os.path.join(home, "Downloads"), os.path.join(home, "Games"), home, "/home/deck"}
                needs_save = False
                for key in ("download_path", "install_path"):
                    if saved.get(key) in old_bad:
                        saved[key] = defaults[key]
                        needs_save = True
                # 补齐缺失键
                for k, v in defaults.items():
                    if k not in saved:
                        saved[k] = v
                        needs_save = True
                if needs_save:
                    try:
                        with open(path, "w") as fw:
                            json.dump(saved, fw, indent=2, ensure_ascii=False)
                    except Exception:
                        pass
                return _ok(saved)
        except Exception as e:
            decky.logger.error("get_settings: %s", e)
        return _ok(defaults)

    async def set_settings(self, payload=None, **kwargs):
        """保存设置项（合并写入，确保所有键完整）。"""
        import json
        try:
            defaults = self._default_settings()
            current = dict(defaults)
            path = self._settings_path()
            if os.path.exists(path):
                try:
                    with open(path, "r") as f:
                        saved = json.load(f)
                    current.update(saved)
                except Exception:
                    pass
            if isinstance(payload, dict):
                current.update(payload)
            if kwargs:
                current.update(kwargs)
            # 确保设置的目录存在
            for key in ("download_path", "install_path"):
                val = current.get(key, "")
                if val and isinstance(val, str):
                    try: os.makedirs(val, exist_ok=True)
                    except Exception: pass
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "w") as f:
                json.dump(current, f, indent=2, ensure_ascii=False)
            return _ok(current)
        except Exception as e:
            decky.logger.error("set_settings: %s", e)
            return _err(str(e))

    async def delete_archive(self, file_path: str = ""):
        """安装完成后删除压缩包（依赖 auto_delete_after_install 设置）。"""
        try:
            fp = str(file_path or "").strip()
            if fp and os.path.isfile(fp):
                os.remove(fp)
                decky.logger.info("delete_archive: removed %s", fp)
                return _ok({"deleted": fp})
            return _ok({"deleted": ""})
        except Exception as e:
            decky.logger.error("delete_archive: %s", e)
            return _err(str(e))

    async def list_media_mounts(self):
        """列出可用的外部存储挂载点（递归扫描 /run/media）。"""
        mounts = []
        base = "/run/media"
        try:
            if os.path.isdir(base):
                for root, dirs, _files in os.walk(base, followlinks=True):
                    for d in dirs:
                        mp = os.path.join(root, d)
                        try:
                            if os.path.ismount(mp):
                                try:
                                    st = os.statvfs(mp)
                                    free = st.f_frsize * st.f_bavail
                                    total = st.f_frsize * st.f_blocks
                                    mounts.append({"path": mp, "label": d, "free_bytes": free, "total_bytes": total})
                                except Exception:
                                    mounts.append({"path": mp, "label": d, "free_bytes": 0, "total_bytes": 0})
                        except Exception:
                            pass
        except Exception:
            pass
        # 也把 home 加入
        home = os.path.expanduser("~")
        try:
            st = os.statvfs(home)
            free = st.f_frsize * st.f_bavail
            total = st.f_frsize * st.f_blocks
            mounts.append({"path": home, "label": "内置存储", "free_bytes": free, "total_bytes": total})
        except Exception:
            pass
        return _ok({"mounts": mounts})

    async def get_catalog_version(self):
        """获取游戏目录版本信息。"""
        return _ok({"date": "本地CSV", "total": len(self._catalog_items)})

    async def update_catalog(self):
        """重新加载游戏目录。"""
        await self._load_catalog_async()
        return _ok({"total": len(self._catalog_items)})

    @staticmethod
    def _default_settings():
        home = os.path.expanduser("~")
        dl = os.path.join(home, "FreedeckGames", "freedeckInstall")
        inst = os.path.join(home, "FreedeckGames")
        # 确保默认目录存在
        for d in (dl, inst):
            try: os.makedirs(d, exist_ok=True)
            except Exception: pass
        return {
            "download_path": dl,
            "install_path": inst,
            "auto_delete_after_install": False,
            "auto_suspend_on_download": False,
            "fast_mode": True,
            "force_ipv4": False,
            "auto_switch_line": True,
            "split_count": 4,
            "show_playtime_widget": True,
            "show_freedeck_library_entry": True,
            "screen_off_auto_suspend": False,
            "cloud_save_auto_upload": False,
            "cloud_save_provider": "",
        }

    # ── Rust crypto 桥接 ────────────

    async def sign_download(self, access_token: str = "", file_id: str = "", share_id: str = ""):
        """使用 Rust crypto 模块生成 MD5 下载签名。"""
        import hashlib
        import time as _time
        ts = str(int(_time.time() * 1000))
        if _core is not None:
            try:
                sig_buf = ctypes.create_string_buffer(64)
                _core.nfc_sign(
                    access_token.encode(), file_id.encode(), share_id.encode(),
                    ts.encode(), sig_buf, 64,
                )
                return _ok({"signature": sig_buf.value.decode(), "timestamp": ts})
            except Exception as e:
                decky.logger.error("Rust sign failed, fallback: %s", e)
        # Python 回退
        source = (
            f"AccessToken={access_token}"
            f"&Timestamp={ts}"
            f"&dt=1"
            f"&fileId={file_id}"
            f"&shareId={share_id}"
        )
        sig = hashlib.md5(source.encode("utf-8")).hexdigest()
        return _ok({"signature": sig, "timestamp": ts})

    async def fetch_remote_catalog(self):
        """返回游戏目录（本地 CSV 或远程）。"""
        if not self._catalog_loaded:
            return _err("目录正在加载，请稍候")
        return _ok({"items": self._catalog_items, "total": len(self._catalog_items)})

    async def read_log(self, payload=None, lines: int = 200):
        """读取最近 N 行插件日志（从本地日志文件）。

        回退：如果日志文件不存在，尝试 journalctl。
        """
        if isinstance(payload, dict):
            lines = int(payload.get("lines", lines) or lines)
        lines = max(10, min(lines, 2000))

        # ── 优先读本地日志文件 ──
        log_path = os.path.join(_state_dir(), "plugin.log")
        try:
            if os.path.exists(log_path):
                content = await asyncio.get_event_loop().run_in_executor(
                    None, lambda: _tail_file(log_path, lines)
                )
                return _ok({"log": content or "暂无日志", "total": content.count(chr(10)) if content else 0})
        except Exception as e:
            decky.logger.warning("read_log from file failed: %s", e)

        # ── 回退 journalctl ──
        import subprocess
        try:
            result = subprocess.run(
                ["journalctl", "-u", "plugin_loader", "--no-pager", "-n", str(lines)],
                capture_output=True, text=True, timeout=5,
            )
            # 清洗 ANSI 转义码
            import re as _re
            cleaned = _re.sub(r"\x1b\[[0-9;]*m", "", result.stdout or "")
            return _ok({"log": cleaned or "暂无日志", "total": cleaned.count(chr(10)) if cleaned else 0})
        except Exception as e:
            return _err(str(e))

    # ── 云存档 ─────────────────────────

    async def cloud_save_upload(self, payload=None) -> dict:
        """扫描已安装游戏的存档（仅 installed_games.json 中的游戏），按游戏逐个上传。"""
        if not self._ok:
            return _err("云盘子系统未就绪")

        try:
            from cloud_save import CloudSyncEngine, SaveScanner, get_history
            from installed_games import load_installed_games

            installed = load_installed_games()
            if not installed:
                return _ok({
                    "uploaded": [], "count": 0,
                    "message": "还没有通过 Freedeck 安装的游戏。",
                    "history": get_history(),
                })

            # 收集有 platform 字段的模拟器平台集合
            managed_platforms = {
                g["platform"] for g in installed
                if g.get("platform") and g.get("game_type") in ("emulator_rom", "emulator_tool")
            }

            scanner = SaveScanner()
            engine = CloudSyncEngine(
                self._session_mgr, self._tianyi,
                plugin_logger=decky.logger,
            )

            emu_saves = scanner.scan_all_emulators()
            total_uploaded = 0
            all_results: list[dict] = []

            # 模拟器：仅上传 platform 在 installed_games 中有记录的
            for platform, paths in emu_saves.items():
                if platform not in managed_platforms:
                    decky.logger.info("cloud_save_upload: skip %s (not in installed_games)", platform)
                    continue
                try:
                    result = await engine.upload_game(platform, paths)
                    decky.logger.info("cloud_save_upload: %s -> %s (%d files)",
                                      platform, result.get("status"), result.get("count", 0))
                    if result.get("count", 0) > 0:
                        total_uploaded += result["count"]
                        all_results.extend(result.get("uploaded", []))
                except Exception as e:
                    decky.logger.error("cloud_save_upload: %s failed: %s", platform, e)

            # PC 游戏：仅上传 installed_games.json 中有 install_dir 的游戏
            from installed_games import load_installed_games
            from cloud_save import _scan_common_pc_save_paths
            installed = load_installed_games()
            for game in installed:
                try:
                    appid = int(game.get("steam_appid", 0) or 0)
                    name = str(game.get("title", ""))
                    install_dir = str(game.get("install_dir", ""))
                    game_id = str(game.get("game_id", ""))
                    if not install_dir or not os.path.isdir(install_dir):
                        continue
                    pc_paths = _scan_common_pc_save_paths(name, install_dir, appid)
                    if not pc_paths:
                        continue
                    result = await engine.upload_game(game_id, pc_paths)
                    if result.get("count", 0) > 0:
                        total_uploaded += result["count"]
                        all_results.extend(result.get("uploaded", []))
                except Exception as e:
                    decky.logger.warning("cloud_save_upload: pc %s failed: %s", game.get("title", ""), e)

            return _ok({
                "uploaded": all_results,
                "count": total_uploaded,
                "message": f"已上传 {total_uploaded} 个文件",
                "history": get_history(),
            })
        except Exception as e:
            decky.logger.error("cloud_save_upload failed: %s", e)
            return _err(str(e))

    async def cloud_save_download(self, payload=None) -> dict:
        """从天翼云盘下载存档并恢复（仅 installed_games.json 中的游戏）。"""
        if not self._ok:
            return _err("云盘子系统未就绪")

        try:
            from cloud_save import CloudSyncEngine, SaveScanner, get_history
            from installed_games import load_installed_games

            installed = load_installed_games()
            if not installed:
                return _ok({
                    "downloaded": [], "count": 0,
                    "message": "还没有通过 Freedeck 安装的游戏。",
                    "history": get_history(),
                })

            managed_platforms = {
                g["platform"] for g in installed
                if g.get("platform") and g.get("game_type") in ("emulator_rom", "emulator_tool")
            }

            scanner = SaveScanner()
            engine = CloudSyncEngine(
                self._session_mgr, self._tianyi,
                plugin_logger=decky.logger,
            )

            emu_saves = scanner.scan_all_emulators()
            total_downloaded = 0
            all_results: list[dict] = []

            # 模拟器：仅恢复 platform 在 installed_games 中有记录的
            for platform, paths in emu_saves.items():
                if platform not in managed_platforms:
                    continue
                try:
                    result = await engine.download_game(platform, paths)
                    decky.logger.info("cloud_save_download: %s -> %s (%d files)",
                                      platform, result.get("status"), result.get("count", 0))
                    if result.get("count", 0) > 0:
                        total_downloaded += result["count"]
                        all_results.extend(result.get("downloaded", []))
                except Exception as e:
                    decky.logger.error("cloud_save_download: %s failed: %s", platform, e)

            # PC 游戏
            from installed_games import load_installed_games
            installed = load_installed_games()
            for game in installed:
                try:
                    game_id = str(game.get("game_id", ""))
                    install_dir = str(game.get("install_dir", ""))
                    if not install_dir or not os.path.isdir(install_dir):
                        continue
                    result = await engine.download_game(game_id, [install_dir])
                    if result.get("count", 0) > 0:
                        total_downloaded += result["count"]
                        all_results.extend(result.get("downloaded", []))
                except Exception as e:
                    decky.logger.warning("cloud_save_download: pc %s failed: %s", game.get("title", ""), e)

            return _ok({
                "downloaded": all_results,
                "count": total_downloaded,
                "message": f"已恢复 {total_downloaded} 个文件",
                "history": get_history(),
            })
        except Exception as e:
            decky.logger.error("cloud_save_download failed: %s", e)
            return _err(str(e))

    async def cloud_save_history(self, payload=None) -> dict:
        """返回同步历史记录。"""
        try:
            from cloud_save import get_history
            return _ok({"records": get_history()})
        except Exception as e:
            return _err(str(e))

    # ── 云存档 自动同步 ─────────────────

    async def cloud_save_sync_on_launch(self, payload=None) -> dict:
        """游戏启动时执行下载同步。

        参数: { appid: int }
        通过 installed_games.json 反查 game_id + install_dir。
        不在 JSON 中的游戏直接忽略（返回 not_managed）。
        """
        if not self._ok:
            return _err("云盘子系统未就绪")

        appid = int(payload.get("appid", 0) or 0) if isinstance(payload, dict) else 0
        if not appid:
            return _err("appid required")

        from installed_games import find_by_appid
        game = find_by_appid(appid)
        if not game:
            return _ok({"status": "not_managed", "message": "非插件安装的游戏"})

        game_id = str(game.get("game_id", ""))
        install_dir = str(game.get("install_dir", ""))
        if not install_dir or not os.path.isdir(install_dir):
            return _ok({"status": "not_installed", "message": "安装目录不存在"})

        try:
            from cloud_save import CloudSyncEngine, SaveScanner

            scanner = SaveScanner()
            engine = CloudSyncEngine(
                self._session_mgr, self._tianyi, plugin_logger=decky.logger,
            )

            local_paths: list[str] = [install_dir]

            # 模拟器：platform 匹配
            emu_saves = scanner.scan_all_emulators()
            for platform, paths in emu_saves.items():
                if game_id == platform or game_id.startswith(f"emu:{platform}"):
                    local_paths.extend(paths)

            # 网络检查
            try:
                import aiohttp
                async with aiohttp.ClientSession() as s:
                    async with s.get("https://cloud.189.cn", timeout=aiohttp.ClientTimeout(total=3)) as _:
                        pass
            except Exception:
                return _ok({"status": "offline", "message": "离线模式"})

            result = await engine.download_game(game_id, local_paths)
            decky.logger.info("cloud_save_sync_on_launch: %s -> %s", game_id, result.get("status"))
            return _ok(result)
        except Exception as e:
            decky.logger.error("cloud_save_sync_on_launch failed: %s", e)
            return _err(str(e))

    async def cloud_save_sync_on_exit(self, payload=None) -> dict:
        """游戏退出时执行上传同步。

        参数: { appid: int }
        通过 installed_games.json 反查 game_id + install_dir。
        """
        if not self._ok:
            return _err("云盘子系统未就绪")

        appid = int(payload.get("appid", 0) or 0) if isinstance(payload, dict) else 0
        if not appid:
            return _err("appid required")

        from installed_games import find_by_appid
        game = find_by_appid(appid)
        if not game:
            return _ok({"status": "not_managed", "message": "非插件安装的游戏"})

        game_id = str(game.get("game_id", ""))
        install_dir = str(game.get("install_dir", ""))
        if not install_dir or not os.path.isdir(install_dir):
            return _ok({"status": "not_installed", "message": "安装目录不存在"})

        try:
            from cloud_save import CloudSyncEngine, SaveScanner, LocalIndex

            scanner = SaveScanner()
            engine = CloudSyncEngine(
                self._session_mgr, self._tianyi, plugin_logger=decky.logger,
            )

            local_paths: list[str] = [install_dir]
            emu_saves = scanner.scan_all_emulators()
            for platform, paths in emu_saves.items():
                if game_id == platform or game_id.startswith(f"emu:{platform}"):
                    local_paths.extend(paths)

            # 网络检查
            try:
                import aiohttp
                async with aiohttp.ClientSession() as s:
                    async with s.get("https://cloud.189.cn", timeout=aiohttp.ClientTimeout(total=3)) as _:
                        pass
            except Exception:
                index = LocalIndex()
                index.mark_pending(game_id)
                return _ok({"status": "offline", "message": "离线模式，已标记待上传"})

            result = await engine.upload_game(game_id, local_paths)
            decky.logger.info("cloud_save_sync_on_exit: %s -> %s", game_id, result.get("status"))
            return _ok(result)
        except Exception as e:
            decky.logger.error("cloud_save_sync_on_exit failed: %s", e)
            return _err(str(e))

    async def cloud_save_resolve_conflict(self, payload=None) -> dict:
        """解决存档冲突。

        参数: {game_id: str, choice: "use_cloud" | "keep_local"}
        """
        if not self._ok:
            return _err("云盘子系统未就绪")

        try:
            game_id = str(payload.get("game_id", "") if isinstance(payload, dict) else "")
            choice = str(payload.get("choice", "") if isinstance(payload, dict) else "")

            if not game_id or not choice:
                return _err("game_id and choice required")

            from cloud_save import CloudSyncEngine, SaveScanner

            scanner = SaveScanner()
            engine = CloudSyncEngine(
                self._session_mgr, self._tianyi, plugin_logger=decky.logger,
            )

            local_paths: list[str] = []
            emu_saves = scanner.scan_all_emulators()
            for platform, paths in emu_saves.items():
                if game_id == platform or game_id.startswith(f"emu:{platform}"):
                    local_paths.extend(paths)

            result = await engine.resolve_conflict(game_id, choice, local_paths)
            decky.logger.info("cloud_save_resolve_conflict: %s -> %s", game_id, choice)

            return _ok(result)
        except Exception as e:
            decky.logger.error("cloud_save_resolve_conflict failed: %s", e)
            return _err(str(e))

    async def ping(self):
        return _ok({"ok": True, "http": self._http_ok, "rust": _core is not None})
