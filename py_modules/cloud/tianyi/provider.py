"""天翼云盘 —— CloudProvider 实现。"""

from __future__ import annotations

from ..models import (
    CloudProviderId, CloudCredential, ShareInfo, DirectLink, LoginState,
)
from ..errors import LoginExpiredError
from . import auth as tianyi_auth
from . import resolver as tianyi_resolver
from . import link_fetcher as tianyi_link


class TianyiCloudProvider:
    """天翼云盘的完整 CloudProvider 实现。"""

    @property
    def provider_id(self) -> CloudProviderId:
        return CloudProviderId.TIANYI

    # ── 登录 ──────────────────────────────

    async def check_login(self, credential: CloudCredential) -> LoginState:
        """校验 cookie 是否有效。"""
        try:
            fresh = await tianyi_auth.validate_cookie(credential.cookie)
            return LoginState(
                logged_in=True,
                account_name=fresh.account_name,
                message="登录有效",
                credential=fresh,
            )
        except LoginExpiredError as exc:
            return LoginState(
                logged_in=False,
                account_name="",
                message=str(exc),
            )

    async def refresh_credential(self, credential: CloudCredential) -> CloudCredential:
        """刷新短期 token。"""
        access_token = await tianyi_auth.fetch_access_token(credential.cookie)
        credential.access_token = access_token
        return credential

    # ── 分享 ──────────────────────────────

    def parse_share_url(self, url: str) -> tuple[str, str]:
        return tianyi_resolver.parse_share_url(url)

    async def resolve_share(
        self,
        url: str,
        credential: CloudCredential,
        *,
        access_code: str = "",
        root_dir_id: str = "",
    ) -> ShareInfo:
        return await tianyi_resolver.resolve_share(
            url, credential,
            access_code=access_code,
            root_dir_id=root_dir_id,
        )

    # ── 直链 ──────────────────────────────

    async def fetch_direct_link(
        self,
        file_id: str,
        share_id: str,
        credential: CloudCredential,
    ) -> DirectLink:
        return await tianyi_link.fetch_direct_link(file_id, share_id, credential)

    async def refresh_direct_link(
        self,
        file_id: str,
        share_id: str,
        credential: CloudCredential,
    ) -> DirectLink:
        return await tianyi_link.refresh_direct_link(file_id, share_id, credential)
