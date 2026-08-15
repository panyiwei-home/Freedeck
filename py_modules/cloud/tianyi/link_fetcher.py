"""天翼云盘 —— 直链获取器。

核心接口:
  - fetch_direct_link(file_id, share_id, credential) -> DirectLink
  - refresh_direct_link(...) -> DirectLink
"""

from __future__ import annotations

import hashlib
from urllib.parse import urlencode

import aiohttp

from ..models import CloudCredential, DirectLink
from ..errors import (
    DirectLinkError, DirectLinkRateLimited, LoginExpiredError,
)
from .api import (
    create_session, build_headers, sign_download_request,
    retry_with_backoff, now_ms,
)
from . import auth as tianyi_auth

# 天翼 API 域名列表（自动故障转移）
_API_URLS = [
    "https://api.cloud.189.cn/open/file/getFileDownloadUrl.action",
    "https://cloud.189.cn/api/open/file/getFileDownloadUrl.action",
]


async def fetch_direct_link(
    file_id: str,
    share_id: str,
    credential: CloudCredential,
) -> DirectLink:
    """获取文件下载直链。

    Raises:
        DirectLinkRateLimited: 被限流
        DirectLinkError:       获取失败
        LoginExpiredError:     登录态失效
    """
    token = str(credential.access_token or "").strip()
    if not token:
        # 自动获取
        token = await tianyi_auth.fetch_access_token(credential.cookie)
        credential.access_token = token

    signature, timestamp = sign_download_request(
        access_token=token,
        file_id=file_id,
        share_id=share_id,
    )

    query = urlencode({
        "fileId": file_id,
        "shareId": share_id,
        "dt": "1",
    })

    async with create_session() as session:
        headers = build_headers(credential.cookie)
        headers.update({
            "AccessToken": token,
            "Signature": signature,
            "Timestamp": timestamp,
            "Sign-Type": "1",
        })

        last_error: Exception | None = None
        for api_url in _API_URLS:
            full_url = f"{api_url}?{query}"
            try:
                async def _req():
                    async with session.get(
                        full_url, headers=headers, allow_redirects=False,
                    ) as resp:
                        text = await resp.text()
                        if resp.status == 401:
                            raise LoginExpiredError("accessToken 已过期")
                        if resp.status == 429:
                            raise DirectLinkRateLimited("请求过于频繁")
                        if resp.status >= 400:
                            raise aiohttp.ClientResponseError(
                                request_info=resp.request_info,
                                history=resp.history,
                                status=resp.status,
                                message=f"HTTP {resp.status}: {text[:200]}",
                            )
                        import json
                        return json.loads(text)

                payload = await retry_with_backoff(_req, max_retries=3)
                last_error = None
                break
            except DirectLinkRateLimited as exc:
                last_error = exc
                break  # 不限流重试，直接退出
            except LoginExpiredError:
                raise
            except Exception as exc:
                last_error = exc
                continue

        if last_error is not None:
            if isinstance(last_error, DirectLinkRateLimited):
                raise last_error
            raise DirectLinkError(str(last_error))

    if not isinstance(payload, dict):
        raise DirectLinkError("直链响应格式异常")

    direct_url = str(
        payload.get("fileDownloadUrl", payload.get("downloadUrl", "")) or ""
    ).strip()

    if not direct_url:
        data = payload.get("data")
        if isinstance(data, dict):
            direct_url = str(
                data.get("fileDownloadUrl", data.get("downloadUrl", "")) or ""
            ).strip()

    if not direct_url:
        raise DirectLinkError("未获取到可用直链")

    return DirectLink(url=direct_url)


async def refresh_direct_link(
    file_id: str,
    share_id: str,
    credential: CloudCredential,
) -> DirectLink:
    """刷新已过期的直链。

    与 fetch_direct_link 相同，但会先尝试刷新 access_token。
    """
    if credential.access_token:
        try:
            credential.access_token = await tianyi_auth.fetch_access_token(
                credential.cookie
            )
        except Exception:
            pass

    return await fetch_direct_link(file_id, share_id, credential)
