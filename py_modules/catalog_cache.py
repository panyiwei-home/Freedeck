"""本地 Catalog JSON 缓存。

解决每次进入模拟器 tab 都远程拉取的痛点：
  - 首次拉取后写入本地 cache/catalog/{platform}.json
  - 后续请求直接从本地读（<5ms），后台异步刷新
"""

from __future__ import annotations

import asyncio
import json as _json
import logging
import os
import time
from pathlib import Path
from typing import Optional

import aiohttp

_LOGGER = logging.getLogger("NewFreedeck")
_CATALOG_BASE = "http://panyiwei.xyz/freedeck-catalog"
_STALE_SEC = 1800  # 30 分钟过期


class CatalogCache:
    """每个平台的游戏目录本地缓存。"""

    def __init__(self, plugin_dir: str):
        self._cache_dir = Path(plugin_dir) / "cache" / "catalog"
        self._cache_dir.mkdir(parents=True, exist_ok=True)
        self._session: Optional[aiohttp.ClientSession] = None
        self._lock = asyncio.Lock()

    async def _ensure_session(self):
        if self._session is None or self._session.closed:
            conn = aiohttp.TCPConnector(limit=4, limit_per_host=4, ttl_dns_cache=300)
            self._session = aiohttp.ClientSession(
                connector=conn,
                timeout=aiohttp.ClientTimeout(total=10),
            )

    def _platform_path(self, platform: str) -> Path:
        return self._cache_dir / f"{platform}.json"

    def _is_fresh(self, path: Path) -> bool:
        if not path.exists():
            return False
        return (time.time() - path.stat().st_mtime) < _STALE_SEC

    async def get_platform(self, platform: str) -> list[dict]:
        """获取平台游戏列表（优先本地缓存）。"""
        path = self._platform_path(platform)

        # 本地缓存命中 → 直接返回，后台异步刷新
        if self._is_fresh(path):
            try:
                return _json.loads(path.read_text())
            except Exception:
                pass

        # 本地缓存过期 → 返回旧数据 + 触发后台刷新
        if path.exists():
            try:
                old = _json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                old = []
            asyncio.create_task(self._refresh(platform))
            return old

        # 无缓存 → 同步拉取
        return await self._fetch_remote(platform)

    async def _refresh(self, platform: str):
        """后台刷新单个平台的缓存。"""
        async with self._lock:
            try:
                items = await self._fetch_remote(platform)
                if items:
                    path = self._platform_path(platform)
                    path.write_text(_json.dumps(items, ensure_ascii=False), encoding="utf-8")
                    _LOGGER.info("CatalogCache refreshed %s (%d items)", platform, len(items))
            except Exception as e:
                _LOGGER.warning("CatalogCache refresh %s failed: %s", platform, e)

    async def _fetch_remote(self, platform: str) -> list[dict]:
        """直接从远程拉取。"""
        await self._ensure_session()
        url = f"{_CATALOG_BASE}/platform/{platform}.json"
        async with self._session.get(url) as resp:
            data = _json.loads(await resp.text())
        items = data if isinstance(data, list) else data.get("games", data.get("items", []))
        if items:
            path = self._platform_path(platform)
            path.write_text(_json.dumps(items, ensure_ascii=False), encoding="utf-8")
            _LOGGER.info("CatalogCache fetched %s (%d items)", platform, len(items))
        return items

    async def warm_all(self, platforms: list[str]):
        """后台预热全部平台缓存。"""
        tasks = [asyncio.create_task(self._refresh(p)) for p in platforms]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for p, r in zip(platforms, results):
            if isinstance(r, Exception):
                _LOGGER.warning("CatalogCache warm %s failed: %s", p, r)
