"""NewFreedeck — 安装管道（文件匹配 / 类型检测 / ROM 导入）
模块化设计，方便后续按平台扩展。"""

import os
import re
from typing import Dict, Any, List, Optional, Tuple

# ── 归档扩展名 ─────────────────────────

ARCHIVE_EXTENSIONS = {
    ".7z", ".zip", ".rar", ".tar", ".gz", ".bz2", ".xz", ".iso",
    ".001", ".002", ".003",  # 分卷后缀（检测时看完整名）
}

# ── ROM 扩展名（按平台）────────────────

ROM_EXTENSIONS: Dict[str, set] = {
    "switch":  {".xci", ".nsp", ".nsz", ".xcz"},
    "ps1":     {".cue", ".chd", ".pbp", ".m3u", ".bin", ".iso", ".img"},
    "ps2":     {".iso", ".chd", ".cso", ".gz"},
    "ps3":     {".pkg", ".iso"},  # PS3 多为文件夹格式，单独处理
    "psp":     {".iso", ".cso", ".pbp"},
    "3ds":     {".3ds", ".cia", ".cci", ".cxi"},
    "gba":     {".gba", ".agb", ".gb", ".gbc"},
    "nds":     {".nds", ".ids"},
    "fc":      {".nes", ".fds", ".unf", ".unif"},
    "md":      {".md", ".bin", ".gen", ".smd", ".32x"},
}

# ── 平台别名归一化 ─────────────────────

PLATFORM_ALIASES: Dict[str, str] = {
    "ns": "switch",
    "nes": "fc",
    "famicom": "fc",
    "snes": "sfc",
    "megadrive": "md",
    "genesis": "md",
    "gameboy": "gb",
    "gameboyadvance": "gba",
    "nintendods": "nds",
    "psx": "ps1",
    "playstation": "ps1",
    "playstation2": "ps2",
    "playstation3": "ps3",
    "playstationportable": "psp",
}


def normalize_platform(platform: str) -> str:
    """归一化平台名。ns→switch, nes→fc 等。"""
    p = str(platform or "").strip().lower()
    return PLATFORM_ALIASES.get(p, p)


# ── 平台是否为文件夹分发 ─────────────────

FOLDER_BASED_PLATFORMS = {"ps3"}

# ── ShareFile 兼容 ─────────────────────
# 天翼云盘返回 dataclass 对象，不是 dict。统一用此函数读取属性。

def _fget(file_obj, key: str, default=None):
    """兼容 dict 和 dataclass 的属性读取。"""
    if isinstance(file_obj, dict):
        return file_obj.get(key, default)
    return getattr(file_obj, key, default)


# ── 检测函数 ───────────────────────────

def file_type(filename: str, platform: str = "") -> str:
    """判断文件类型：archive | rom | unknown"""
    name = str(filename or "").strip().lower()
    if not name:
        return "unknown"

    # 分卷包
    if re.search(r"\.(?:7z|zip|rar)\.[\d]{2,4}$", name):
        return "archive"
    if re.search(r"\.part[\d]+\.(?:rar|7z|zip)$", name):
        return "archive"
    if re.search(r"\.r[\d]{2,4}$", name):
        return "archive"

    # ROM 优先（歧义格式 .iso .bin 等先按 ROM 处理）
    _, ext = os.path.splitext(name)
    platform_norm = normalize_platform(platform)
    exts = ROM_EXTENSIONS.get(platform_norm, set())
    if ext in exts:
        return "rom"

    # 常规归档
    if ext in ARCHIVE_EXTENSIONS:
        return "archive"

    return "unknown"


def is_archive(filename: str) -> bool:
    return file_type(filename) == "archive"


def is_rom(filename: str, platform: str = "") -> bool:
    return file_type(filename, platform) == "rom"


def needs_extraction(filename: str, platform: str = "") -> bool:
    """是否需要解压。

    规则：
    - 纯归档 (.7z .zip .rar .tar .gz .bz2 .xz) → 需要解压
    - 分卷包 (.001 .part1 .r01) → 需要解压
    - ROM 格式 (.xci .nsp .gba .nes 等) → 跳过
    - 歧义格式 (.iso .bin .chd)：如果当前平台把它当 ROM 格式 → 跳过
    """
    if is_rom(filename, platform):
        return False
    return is_archive(filename)


# ── 文件名匹配 ─────────────────────────

def _normalize(name: str) -> str:
    """归一化文件名用于匹配。"""
    n = str(name or "").strip().lower()
    n = re.sub(r"\s+", " ", n)
    n = re.sub(r"[/｜|\\]+", " ", n)
    # 去掉扩展名
    base, _ = os.path.splitext(n)
    return base.strip()


def match_files(
    target_name: str,
    files: List[Dict[str, Any]],
    platform: str = "",
) -> List[Dict[str, Any]]:
    """从文件列表中匹配目标文件。返回匹配结果列表。

    策略：
    1. 精确匹配 file_name
    2. 归一化后模糊匹配（去掉扩展名、空格归一化）
    3. 多文件分享 → 全部下载
    4. 单文件 → 直接返回
    """
    target = str(target_name or "").strip()
    if not target or not files:
        return []

    # 排除目录
    non_dirs = [f for f in files if not _fget(f, "is_directory")]
    if not non_dirs:
        non_dirs = files  # 全部是目录

    # 单文件 → 直接返回
    if len(non_dirs) == 1:
        return non_dirs

    target_norm = _normalize(target)
    target_base = _normalize(os.path.basename(target))

    # 策略 1：精确匹配
    for f in non_dirs:
        if _fget(f, "name", "") == target or _fget(f, "file_name", "") == target:
            return [f]

    # 策略 2：归一化匹配
    best_score = 0
    best_match = None

    for f in non_dirs:
        fname = _fget(f, "name", _fget(f, "file_name", ""))
        fnorm = _normalize(fname)
        fbase = _normalize(os.path.basename(fname))

        # 完全匹配
        if target_norm == fnorm or target_base == fbase:
            return [f]

        # 包含匹配
        if target_norm in fnorm or fnorm in target_norm:
            score = min(len(target_norm), len(fnorm)) / max(len(target_norm), len(fnorm))
            if score > best_score:
                best_score = score
                best_match = f

        # 关键词匹配（取文件名中的字母/数字部分）
        target_words = set(re.findall(r"[a-z0-9]{3,}", target_norm))
        file_words = set(re.findall(r"[a-z0-9]{3,}", fnorm))
        common = target_words & file_words
        if common:
            score = len(common) / max(len(target_words | file_words), 1)
            if score > best_score:
                best_score = score
                best_match = f

    if best_match and best_score > 0.3:
        return [best_match]

    # 策略 3：多文件 → 尝试匹配分卷
    base = _split_archive_base(target)
    if base:
        parts = [f for f in non_dirs if str(_fget(f, "name", "")).startswith(base)]
        if parts:
            return parts

    # 策略 4：回退，返回空让调用方决定
    return []


def _split_archive_base(filename: str) -> str:
    """检测分卷压缩包，返回基础名（不含分卷后缀）。"""
    name = str(filename or "")
    m = re.match(r"^(.+\.(?:7z|zip|rar|iso|gz|bz2|xz))\.(\d{2,4})$", name, re.I)
    if m:
        return m.group(1)
    m = re.match(r"^(.+)\.part(\d+)\.(rar|7z|zip)$", name, re.I)
    if m:
        return m.group(1)
    m = re.match(r"^(.+)\.r(\d{2,4})$", name, re.I)
    if m:
        return m.group(1)
    m = re.match(r"^(.+)\.(\d{3,4})$", name)
    if m and not re.search(r"\.v\d+$", m.group(1), re.I):
        return m.group(1)
    return ""


# ── 文件夹匹配（PS3 等）─────────────────

def match_folder(
    target_name: str,
    files: List[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    """从文件列表中匹配目标文件夹。"""
    target = str(target_name or "").strip()
    if not target:
        return None

    dirs = [f for f in files if _fget(f, "is_directory")]
    if not dirs:
        return None

    target_norm = _normalize(target)

    # 精确匹配
    for d in dirs:
        if _normalize(_fget(d, "name", "")) == target_norm:
            return d

    # 包含匹配
    for d in dirs:
        dnorm = _normalize(_fget(d, "name", ""))
        if target_norm in dnorm or dnorm in target_norm:
            return d

    return None


# ── 游戏分类 ───────────────────────────

def classify_game(game_id: str, platform: str, file_name: str) -> str:
    """分类游戏安装类型：pc | emulator_tool | emulator_rom"""
    gid = str(game_id or "").strip().lower()
    plat = normalize_platform(platform)

    if gid.endswith("_emulator"):
        return "emulator_tool"

    if plat in ROM_EXTENSIONS:
        return "emulator_rom"

    return "pc"


# ── ROM 导入快捷方式构建 ────────────────

def build_rom_launch_args(platform: str, rom_path: str) -> str:
    """根据平台构建模拟器启动参数。"""
    from emulator_adapters import get_adapter
    platform = normalize_platform(platform)
    adapter = get_adapter(platform)
    if adapter and adapter.launch_args:
        return adapter.launch_args.replace("{rom}", rom_path)
    return f'"{rom_path}"'


def find_emulator_appimage(platform: str) -> Optional[str]:
    """查找已安装的模拟器 AppImage。"""
    from emulator_adapters import get_adapter
    from emulator_registry import EMULATOR_PLATFORMS

    platform = normalize_platform(platform)

    home = os.path.expanduser("~")
    install_base = os.path.join(home, "FreedeckGames", "Emulators")

    plat_info = EMULATOR_PLATFORMS.get(platform, {})
    plat_id = plat_info.get("id", platform)
    emu_dir = os.path.join(install_base, plat_id)

    adapter = get_adapter(platform)
    if adapter:
        appimage = adapter.find_appimage(emu_dir)
        if appimage:
            return appimage

    # 回退：搜索整个 Emulators 目录
    if os.path.isdir(install_base):
        for root, _, files in os.walk(install_base):
            for f in files:
                if f.lower().endswith(".appimage"):
                    return os.path.join(root, f)

    return None
