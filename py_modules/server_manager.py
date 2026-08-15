"""本地 HTTP 服务管理器。

基于 aiohttp 的小型 Web 服务，用于提供登录桥接页面和 API。
"""

from __future__ import annotations

import logging
from typing import Optional

from aiohttp import web

_LOGGER = logging.getLogger("NewFreedeck")
_DEFAULT_HOST = "127.0.0.1"
_DEFAULT_PORT = 20065


class LocalServer:
    """管理 aiohttp AppRunner 的生命周期。"""

    def __init__(
        self,
        *,
        host: str = _DEFAULT_HOST,
        port: int = _DEFAULT_PORT,
    ):
        self._host = host
        self._port = port
        self._app: Optional[web.Application] = None
        self._runner: Optional[web.AppRunner] = None
        self._site: Optional[web.TCPSite] = None

    # ── 公开 API ──────────────────────────

    @property
    def is_running(self) -> bool:
        return self._runner is not None

    @property
    def base_url(self) -> str:
        return f"http://{self._host}:{self._port}"

    def app(self) -> web.Application:
        """获取或创建 Application 实例。"""
        if self._app is None:
            self._app = web.Application()
        return self._app

    async def start(self) -> str:
        """启动 HTTP 服务。返回 base_url。"""
        if self._runner is not None:
            return self.base_url

        self._runner = web.AppRunner(self.app())
        await self._runner.setup()
        self._site = web.TCPSite(self._runner, self._host, self._port)
        await self._site.start()
        _LOGGER.info("LocalServer started at %s", self.base_url)
        return self.base_url

    async def stop(self) -> None:
        """停止 HTTP 服务。"""
        if self._runner is None:
            return
        try:
            if self._site is not None:
                await self._site.stop()
                self._site = None
            await self._runner.cleanup()
        except Exception as exc:
            _LOGGER.warning("LocalServer stop error: %s", exc)
        finally:
            self._runner = None
        _LOGGER.info("LocalServer stopped")
