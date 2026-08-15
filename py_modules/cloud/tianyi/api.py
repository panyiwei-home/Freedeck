"""天翼云盘 —— HTTP 底层工具。

提供: session 工厂、请求头构造、签名算法、退避重试。
"""

from __future__ import annotations

import asyncio
import hashlib
import time
from typing import Any, Dict, Optional

import aiohttp

from .._utils import steamos_ssl_context, parse_json_like

# ── 常量 ──────────────────────────────────

_DEFAULT_UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.6478.183 Safari/537.36"
)


# ── 公开 API ──────────────────────────────

def now_ms() -> int:
    """当前毫秒时间戳。"""
    return int(time.time() * 1000)


def default_ua() -> str:
    return _DEFAULT_UA


def create_session(*, timeout_seconds: float = 20.0) -> aiohttp.ClientSession:
    """创建带默认超时和 SSL 上下文的 session。"""
    connector = aiohttp.TCPConnector(ssl=steamos_ssl_context())
    timeout = aiohttp.ClientTimeout(total=timeout_seconds)
    return aiohttp.ClientSession(connector=connector, timeout=timeout)


def build_headers(cookie: str, *, extra: Optional[Dict[str, str]] = None) -> Dict[str, str]:
    """构造请求头。"""
    h: Dict[str, str] = {
        "User-Agent": _DEFAULT_UA,
        "Accept": "application/json;charset=UTF-8",
    }
    if cookie:
        h["Cookie"] = cookie
    if extra:
        h.update(extra)
    return h


def sign_download_request(*, access_token: str, file_id: str, share_id: str) -> tuple[str, str]:
    """生成直链请求的签名和时间戳。

    返回 (signature, timestamp)。
    """
    ts = str(now_ms())
    source = (
        f"AccessToken={access_token}"
        f"&Timestamp={ts}"
        f"&dt=1"
        f"&fileId={file_id}"
        f"&shareId={share_id}"
    )
    sig = hashlib.md5(source.encode("utf-8")).hexdigest()
    return sig, ts


async def json_get(
    session: aiohttp.ClientSession,
    url: str,
    cookie: str,
    *,
    extra_headers: Optional[Dict[str, str]] = None,
    allow_redirects: bool = True,
) -> Any:
    """GET 并解析 JSON 响应体。"""
    headers = build_headers(cookie, extra=extra_headers)
    async with session.get(url, headers=headers, allow_redirects=allow_redirects) as resp:
        text = await resp.text()
        if resp.status >= 400:
            raise aiohttp.ClientResponseError(
                request_info=resp.request_info,
                history=resp.history,
                status=resp.status,
                message=f"HTTP {resp.status}: {text[:200]}",
            )
        return parse_json_like(text)


async def json_post(
    session: aiohttp.ClientSession,
    url: str,
    cookie: str,
    *,
    data: Optional[Dict[str, Any]] = None,
    extra_headers: Optional[Dict[str, str]] = None,
    as_form: bool = False,
) -> Any:
    """POST 并解析 JSON 响应体。"""
    headers = build_headers(cookie, extra=extra_headers)
    if as_form:
        headers.setdefault("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8")
        req_kwargs: Dict[str, Any] = {"data": data or {}}
    else:
        headers.setdefault("Content-Type", "application/json;charset=UTF-8")
        req_kwargs = {"json": data or {}}
    async with session.post(url, headers=headers, **req_kwargs) as resp:
        text = await resp.text()
        if resp.status >= 400:
            raise aiohttp.ClientResponseError(
                request_info=resp.request_info,
                history=resp.history,
                status=resp.status,
                message=f"HTTP {resp.status}: {text[:200]}",
            )
        return parse_json_like(text)


async def retry_with_backoff(
    fn,
    *,
    max_retries: int = 3,
    base_delay: float = 0.5,
    transient_statuses: set[int] = frozenset({429, 500, 502, 503, 504}),
) -> Any:
    """指数退避重试。

    `fn` 为无参 async callable，应就单次请求成功返回数据，
    失败时抛出 aiohttp.ClientResponseError。
    """
    last_error: Optional[BaseException] = None
    for attempt in range(1, max_retries + 1):
        try:
            return await fn()
        except aiohttp.ClientResponseError as exc:
            if exc.status in transient_statuses and attempt < max_retries:
                delay = base_delay * (2 ** (attempt - 1))
                await asyncio.sleep(delay)
                last_error = exc
                continue
            raise
        except (aiohttp.ClientConnectorError, aiohttp.ClientSSLError, asyncio.TimeoutError) as exc:
            if attempt < max_retries:
                delay = base_delay * (2 ** (attempt - 1))
                await asyncio.sleep(delay)
                last_error = exc
                continue
            raise
    raise last_error  # type: ignore[misc]
