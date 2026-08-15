"""天翼云盘 —— 登录与鉴权。

核心接口:
  - validate_cookie(cookie) -> CloudCredential
  - fetch_access_token(cookie) -> str
"""

from __future__ import annotations

import aiohttp
from urllib.parse import parse_qs, urljoin, urlparse

from ..models import CloudCredential, CloudProviderId
from ..errors import LoginExpiredError, LoginFailedError
from .._utils import pick_first
from .api import (
    build_headers, create_session, json_get, now_ms,
)


async def validate_cookie(cookie: str) -> CloudCredential:
    """校验 cookie 有效性，成功返回完整凭证。

    流程:
      1. GET getUserBriefInfo → 确认 cookie 有效 + 提取 account / sessionKey
      2. 调用 fetch_access_token() 获取 accessToken
      3. 组装 CloudCredential 返回

    Raises:
        LoginExpiredError: cookie 无效
    """
    if not cookie:
        raise LoginExpiredError("Cookie 为空")

    async with create_session() as session:
        url = f"https://cloud.189.cn/api/portal/v2/getUserBriefInfo.action?noCache={now_ms()}"
        try:
            payload = await json_get(session, url, cookie)
        except aiohttp.ClientResponseError as exc:
            if exc.status in (401, 403):
                raise LoginExpiredError("Cookie 已过期，请重新登录") from exc
            raise LoginFailedError(f"登录校验失败: {exc}") from exc

    if not isinstance(payload, dict):
        raise LoginExpiredError("登录校验返回格式异常")

    # 检查业务层错误
    res_code = str(pick_first(payload, "res_code", "code", "result") or "").strip()
    if res_code and res_code != "0" and res_code != "200":
        msg = str(pick_first(payload, "res_message", "msg", "message") or "未知错误")
        raise LoginExpiredError(f"Cookie 已过期: {msg}")

    account_name = str(
        pick_first(payload, "userAccount", "name", "nickName") or ""
    ).strip()

    if not account_name:
        raise LoginExpiredError("登录校验未返回用户信息，Cookie 可能已失效")

    session_key = str(pick_first(payload, "sessionKey") or "").strip()

    # 获取 access token
    access_token = ""
    try:
        access_token = await fetch_access_token(cookie)
    except Exception:
        pass

    return CloudCredential(
        provider=CloudProviderId.TIANYI,
        cookie=cookie,
        access_token=access_token,
        account_name=account_name,
        raw_extra={"session_key": session_key},
    )


async def fetch_access_token(cookie: str) -> str:
    """获取短期 accessToken。

    天翼 accessToken 有效期约 2 小时。
    """
    current = "https://api.cloud.189.cn/open/oauth2/ssoH5.action"
    headers = build_headers(cookie, extra={"Referer": "https://cloud.189.cn/"})

    async with create_session() as session:
        for _ in range(12):
            try:
                async with session.get(
                    current,
                    headers=headers,
                    allow_redirects=False,
                ) as resp:
                    final_url = str(resp.url)
                    token = _extract_access_token(final_url)
                    if token:
                        return token
                    if resp.status in {301, 302, 303, 307, 308}:
                        location = str(resp.headers.get("Location", "") or "").strip()
                        if not location:
                            raise LoginFailedError("ssoH5 跳转缺少 Location")
                        current = urljoin(final_url, location)
                        token = _extract_access_token(current)
                        if token:
                            return token
                        continue
                    break
            except aiohttp.ClientResponseError as exc:
                raise LoginFailedError(f"获取 accessToken 失败: {exc}") from exc
            except aiohttp.ClientError as exc:
                raise LoginFailedError(f"获取 accessToken 失败: {exc}") from exc

    raise LoginFailedError("未获取到 accessToken")


def _extract_access_token(url: str) -> str:
    parsed = urlparse(str(url or ""))
    return str(parse_qs(parsed.query or "").get("accessToken", [""])[0] or "").strip()
