# game_catalog.py - 游戏目录加载与检索
#
# 该模块负责读取 CSV 并提供搜索能力。

from __future__ import annotations

import csv
import os
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Dict, List, Optional

import decky

import config


@dataclass
class GameCatalogEntry:
    """单条游戏目录记录。"""

    game_id: str
    title: str
    category_parent: str
    categories: str
    down_url: str
    pwd: str
    openpath: str
    size_bytes: int
    size_text: str

    def to_dict(self) -> Dict[str, object]:
        """转为前端可用字典。"""
        return asdict(self)


def _safe_int(value: str) -> int:
    """安全解析整数字段。"""
    try:
        return int(str(value).strip())
    except Exception:
        return 0


def _normalize_title(raw: str) -> str:
    """标准化标题，去除多余空白。"""
    return " ".join((raw or "").replace("\u3000", " ").strip().split())


def _is_valid_tianyi_url(url: str) -> bool:
    """判断是否是支持的天翼分享链接。"""
    value = (url or "").strip()
    return value.startswith("https://cloud.189.cn/t/") or value.startswith("http://cloud.189.cn/t/")


def resolve_default_catalog_path() -> str:
    """解析默认目录文件路径。"""
    candidates: List[Path] = []

    env_path = (os.getenv("FRIENDECK_GAME_CATALOG_CSV") or "").strip()
    if env_path:
        candidates.append(Path(env_path))

    plugin_dir = getattr(decky, "DECKY_PLUGIN_DIR", None)
    if plugin_dir:
        root = Path(plugin_dir)
        candidates.append(root / "defaults" / "tianyi_catalog" / "gamebox_all_links_20260221_234730.csv")
        candidates.append(root / "defaults" / "tianyi_catalog.csv")

    cwd = Path.cwd()
    candidates.append(cwd / "exports" / "gamebox_all_links_20260221_234730.csv")
    candidates.append(cwd / "gamebox_all_links_20260221_234730.csv")

    for path in candidates:
        try:
            resolved = path.expanduser().resolve()
        except Exception:
            continue
        if resolved.is_file():
            return str(resolved)

    return ""


class GameCatalog:
    """游戏目录仓库。"""

    def __init__(self, csv_path: str):
        self.csv_path = csv_path
        self.entries: List[GameCatalogEntry] = []
        self.invalid_rows = 0

    def load(self) -> None:
        """加载 CSV 到内存。"""
        self.entries = []
        self.invalid_rows = 0
        if not self.csv_path:
            config.logger.warning("未找到游戏目录 CSV 路径")
            return
        if not os.path.isfile(self.csv_path):
            config.logger.warning("游戏目录 CSV 不存在: %s", self.csv_path)
            return

        with open(self.csv_path, "r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if not isinstance(row, dict):
                    self.invalid_rows += 1
                    continue

                title = _normalize_title(row.get("title", ""))
                down_url = (row.get("down_url") or "").strip()
                if not title or not _is_valid_tianyi_url(down_url):
                    self.invalid_rows += 1
                    continue

                game_id = str(row.get("game_id", "")).strip()
                size_bytes = _safe_int(row.get("filesize_z", "0"))
                size_text = (row.get("list_filesize") or "").strip()
                if not size_text and size_bytes > 0:
                    size_text = _format_size(size_bytes)

                entry = GameCatalogEntry(
                    game_id=game_id or title,
                    title=title,
                    category_parent=str(row.get("category_parent", "")).strip(),
                    categories=str(row.get("categories", "")).strip(),
                    down_url=down_url,
                    pwd=str(row.get("pwd", "")).strip(),
                    openpath=str(row.get("openpath", "")).strip(),
                    size_bytes=size_bytes,
                    size_text=size_text,
                )
                self.entries.append(entry)

        config.logger.info(
            "已加载游戏目录: total=%s invalid=%s file=%s",
            len(self.entries),
            self.invalid_rows,
            self.csv_path,
        )

    def summary(self) -> Dict[str, object]:
        """返回目录摘要。"""
        return {
            "path": self.csv_path,
            "total": len(self.entries),
            "invalid": self.invalid_rows,
            "preview": [e.to_dict() for e in self.entries[:8]],
        }

    def list(
        self,
        query: str = "",
        page: int = 1,
        page_size: int = 50,
    ) -> Dict[str, object]:
        """按关键词分页检索。"""
        q = (query or "").strip().lower()
        normalized_page = max(1, int(page))
        normalized_size = max(1, min(200, int(page_size)))

        if not q:
            matched = self.entries
        else:
            matched = [
                item
                for item in self.entries
                if q in item.title.lower()
                or q in item.categories.lower()
                or q in item.game_id.lower()
            ]

        start = (normalized_page - 1) * normalized_size
        end = start + normalized_size
        items = matched[start:end]

        return {
            "total": len(matched),
            "page": normalized_page,
            "page_size": normalized_size,
            "items": [e.to_dict() for e in items],
        }

    def get_by_game_id(self, game_id: str) -> Optional[GameCatalogEntry]:
        """按 game_id 查找条目。"""
        target = (game_id or "").strip()
        if not target:
            return None
        for item in self.entries:
            if item.game_id == target:
                return item
        return None


def _format_size(size_bytes: int) -> str:
    """格式化字节大小显示。"""
    value = float(size_bytes)
    units = ["B", "KB", "MB", "GB", "TB"]
    unit = units[0]
    for unit in units:
        if value < 1024.0 or unit == units[-1]:
            break
        value /= 1024.0
    if unit == "B":
        return f"{int(value)} {unit}"
    return f"{value:.2f} {unit}"
