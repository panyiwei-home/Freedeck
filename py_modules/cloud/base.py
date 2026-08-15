"""云盘子系统 —— 统一接口协议。"""

from __future__ import annotations

from typing import Protocol

from .models import (
    CloudProviderId,
    CloudCredential,
    ShareInfo,
    DirectLink,
    LoginState,
)


class CloudProvider(Protocol):
    """所有云盘后端的统一接口。"""

    # ── 身份 ──────────────────────────────

    @property
    def provider_id(self) -> CloudProviderId:
        """返回该实现的云盘标识。"""
        ...

    # ── 登录 ──────────────────────────────

    async def check_login(self, credential: CloudCredential) -> LoginState:
        """校验凭证是否仍然有效。"""
        ...

    async def refresh_credential(self, credential: CloudCredential) -> CloudCredential:
        """刷新短期 token（如 accessToken）。"""
        ...

    # ── 分享解析 ──────────────────────────

    def parse_share_url(self, url: str) -> tuple[str, str]:
        """从 URL 提取 (share_code, access_code)。"""
        ...

    async def resolve_share(
        self,
        url: str,
        credential: CloudCredential,
        *,
        access_code: str = "",
        root_dir_id: str = "",
    ) -> ShareInfo:
        """解析分享链接，返回文件列表。"""
        ...

    # ── 直链 ──────────────────────────────

    async def fetch_direct_link(
        self,
        file_id: str,
        share_id: str,
        credential: CloudCredential,
    ) -> DirectLink:
        """获取文件下载直链。"""
        ...

    async def refresh_direct_link(
        self,
        file_id: str,
        share_id: str,
        credential: CloudCredential,
    ) -> DirectLink:
        """刷新已过期的直链。"""
        ...
