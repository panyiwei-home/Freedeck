"""天翼云盘 —— 二维码登录后端。

实现天翼官方 QR 登录 API 的完整流程:
  1. bootstrap:  获取 loginEntry → appConf → getUUID → 返回 QR 图片 URL + state payload
  2. poll:       轮询 qrcodeLoginState.do → 根据状态码处理
  3. finalize:   成功后跟随 redirect URL → 提取 Cookie → 校验 → 保存

状态码:
  0        成功
  -106     等待扫码
  -11002   已扫码等待确认
  -11001, -20099  过期
  -134     需要二次验证
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid as _uuid
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Tuple
from urllib.parse import parse_qs, quote, urlparse

import aiohttp

from .api import create_session, default_ua
from .auth import validate_cookie
from .._utils import pick_first, parse_json_like, steamos_ssl_context
from ..models import CloudCredential, CloudProviderId, LoginState
from ..errors import LoginFailedError, LoginExpiredError

_LOGGER = logging.getLogger("NewFreedeck")

# ── 常量 ──────────────────────────────────

SESSION_TIMEOUT = 300       # 二维码会话总超时 (秒)
HTTP_TIMEOUT = 20           # 单次 HTTP 请求超时
POLL_INTERVAL = 1.6         # 默认轮询间隔 (秒)

# QR 状态码
QR_SUCCESS = 0
QR_WAITING = {-106}
QR_SCANNED = {-11002}
QR_EXPIRED = {-11001, -20099}
QR_EXTRA_VERIFY = {-134}

_TIANYI_BOOTSTRAP_URL = "https://cloud.189.cn/api/portal/loginUrl.action?redirectURL={redirect}&pageId=1"
_TIANYI_APPCONF_URL = "https://open.e.189.cn/api/logbox/oauth2/appConf.do"
_TIANYI_GETUUID_URL = "https://open.e.189.cn/api/logbox/oauth2/getUUID.do"
_TIANYI_POLL_URL = "https://open.e.189.cn/api/logbox/oauth2/qrcodeLoginState.do"
_TIANYI_REDIRECT_TARGET = "https://cloud.189.cn/web/main/"


# ── 公共接口 ──────────────────────────────

@dataclass
class QrSessionState:
    """QR 登录会话的当前状态。"""
    session_id: str = ""
    stage: str = "idle"        # idle | running | completed | failed
    message: str = ""
    reason: str = ""
    next_action: str = ""
    user_account: str = ""
    image_url: str = ""
    expires_at: float = 0.0
    diagnostics: dict = field(default_factory=dict)


class QrLoginManager:
    """天翼云盘 QR 登录管理器。

    使用方式::

        mgr = QrLoginManager(session_store)
        state = await mgr.start()
        # ... 前端轮询 ...
        state = await mgr.poll(session_id)
        if state.stage == "completed":
            credential = mgr.last_credential
    """

    def __init__(self, session_store):
        self._store = session_store
        self._lock = asyncio.Lock()
        self._state = QrSessionState()
        self._context: Optional[Dict[str, Any]] = None
        self._last_credential: Optional[CloudCredential] = None

    @property
    def last_credential(self) -> Optional[CloudCredential]:
        return self._last_credential

    # ── 公开 API ──────────────────────────

    async def check_login_state(self) -> dict:
        """快速检查本地已存登录态。"""
        saved = self._store.load(CloudProviderId.TIANYI)
        if saved is None:
            return {
                "logged_in": False,
                "user_account": "",
                "message": "未找到已保存的登录凭证",
            }
        try:
            cred = await validate_cookie(saved.cookie)
            self._store.save(cred)
            return {
                "logged_in": True,
                "user_account": cred.account_name,
                "message": "登录态有效",
            }
        except Exception as exc:
            return {
                "logged_in": False,
                "user_account": "",
                "message": f"登录态已过期: {exc}",
            }

    async def start_qr_login(self) -> dict:
        """启动 QR 登录会话。"""
        async with self._lock:
            await self._close_context()

            # 先快速检查已有登录态
            check = await self.check_login_state()
            if check["logged_in"]:
                self._set_state(
                    session_id="", stage="completed",
                    message=f"检测到有效登录态：{check['user_account']}",
                    user_account=check["user_account"],
                    diagnostics={"source": "stored_cookie"},
                )
                return self._state_to_dict()

            session_id = _uuid.uuid4().hex
            created_at = time.time()
            expires_at = created_at + SESSION_TIMEOUT

            connector = aiohttp.TCPConnector(ssl=steamos_ssl_context(), force_close=True)
            client = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=HTTP_TIMEOUT),
                cookie_jar=aiohttp.CookieJar(unsafe=True),
                connector=connector,
            )
            self._context = {
                "session_id": session_id,
                "client": client,
                "created_at": created_at,
                "expires_at": expires_at,
                "poll_count": 0,
            }

            try:
                bootstrap = await self._bootstrap(client)
                self._context.update(bootstrap)
                self._context["image_url"] = (
                    f"/api/tianyi/login/qr/image?session_id={session_id}&_ts={int(time.time())}"
                )
                self._set_state(
                    session_id=session_id, stage="running",
                    message="请使用天翼云盘 App 扫码登录",
                    reason="waiting_scan", next_action="scan_qr",
                    image_url=self._context["image_url"],
                    expires_at=expires_at,
                    diagnostics={"source": "qr_api", "created_at": created_at},
                )
                return self._state_to_dict()
            except Exception as exc:
                await self._safe_close(client)
                self._context = None
                self._set_state(
                    stage="failed",
                    message=f"二维码会话启动失败：{exc}",
                    reason="qr_start_failed", next_action="retry",
                    diagnostics={"exception": str(exc)},
                )
                return self._state_to_dict()

    async def poll_qr_login(self, session_id: str = "") -> dict:
        """轮询 QR 登录状态。"""
        async with self._lock:
            ctx = self._context
            if ctx is None:
                return self._state_to_dict()

            if session_id and session_id != str(ctx.get("session_id", "")):
                return self._state_to_dict()

            expires_at = float(ctx.get("expires_at") or 0)
            if expires_at > 0 and time.time() >= expires_at:
                self._set_state(
                    session_id=str(ctx.get("session_id", "")),
                    stage="failed", message="二维码已过期，请刷新后重试",
                    reason="qr_expired", next_action="retry",
                )
                await self._close_context()
                return self._state_to_dict()

            client = ctx.get("client")
            if not isinstance(client, aiohttp.ClientSession):
                self._set_state(
                    stage="failed", message="会话异常，请刷新",
                    reason="qr_context_invalid", next_action="retry",
                )
                await self._close_context()
                return self._state_to_dict()

            state_payload = dict(ctx.get("state_payload") or {})
            now_ms = str(int(time.time() * 1000))
            state_payload["date"] = now_ms
            state_payload["timeStamp"] = now_ms

            headers = self._qr_headers(ctx)

            try:
                async with client.post(_TIANYI_POLL_URL, data=state_payload, headers=headers) as resp:
                    raw = await resp.text()
                    if resp.status >= 400:
                        raise LoginFailedError(f"状态接口 HTTP {resp.status}")
            except Exception as exc:
                self._set_state(
                    stage="running", message="状态轮询失败，正在重试...",
                    reason="poll_exception", next_action="wait",
                    diagnostics={"exception": str(exc)},
                )
                return self._state_to_dict()

            try:
                payload = parse_json_like(raw)
            except Exception as exc:
                self._set_state(
                    stage="running", message="状态解析失败，正在重试...",
                    reason="poll_parse_failed", next_action="wait",
                    diagnostics={"exception": str(exc), "raw": raw[:320]},
                )
                return self._state_to_dict()

            ctx["poll_count"] = int(ctx.get("poll_count") or 0) + 1
            code_raw = pick_first(payload, "status", "res_result", "result", "resultCode")
            if code_raw is not None:
                try:
                    status_code = int(code_raw or 0)
                except (TypeError, ValueError):
                    status_code = -999
            else:
                status_code = -999
            poll_diag = {"poll_count": ctx["poll_count"], "status_code": status_code}
            if status_code == -999:
                poll_diag["raw_preview"] = raw[:500]
                poll_diag["payload_keys"] = sorted(payload.keys()) if isinstance(payload, dict) else str(type(payload))

            if status_code == QR_SUCCESS:
                redirect_url = str(payload.get("redirectUrl", payload.get("redirect_url", "")) or "").strip()
                account, cookie, verify_reason = await self._finalize(ctx, redirect_url)
                poll_diag["redirect_url"] = redirect_url
                if verify_reason:
                    poll_diag["verify_reason"] = verify_reason

                if account and cookie:
                    # 二次校验以获取完整凭证（含 accessToken）
                    try:
                        credential = await validate_cookie(cookie)
                    except Exception as exc:
                        _LOGGER.warning("QR poll: second validate failed: %s", exc)
                        credential = CloudCredential(
                            provider=CloudProviderId.TIANYI,
                            cookie=cookie, account_name=account,
                        )
                    self._store.save(credential)
                    self._last_credential = credential
                    self._set_state(
                        session_id=str(ctx.get("session_id", "")),
                        stage="completed", message=f"登录成功：{account}",
                        user_account=account,
                        diagnostics=poll_diag,
                    )
                    await self._close_context()
                    return self._state_to_dict()

                verify_reason = poll_diag.get("verify_reason", "unknown")
                _LOGGER.warning("QR poll: no valid login state account=%r cookie_len=%d reason=%s",
                                account, len(cookie or ""), verify_reason)
                self._set_state(
                    stage="failed", message="扫码已确认，但未拿到有效登录态",
                    reason="qr_cookie_verify_failed", next_action="retry",
                    diagnostics=poll_diag,
                )
                await self._close_context()
                return self._state_to_dict()

            if status_code in QR_EXPIRED:
                self._set_state(
                    stage="failed", message="二维码已失效，请刷新后重试",
                    reason="qr_expired", next_action="retry",
                    diagnostics=poll_diag,
                )
                await self._close_context()
                return self._state_to_dict()

            if status_code in QR_SCANNED:
                self._set_state(
                    stage="running", message="已扫码，请在手机上确认登录",
                    reason="await_confirm", next_action="confirm_on_phone",
                    diagnostics=poll_diag,
                )
                return self._state_to_dict()

            if status_code in QR_EXTRA_VERIFY:
                self._set_state(
                    stage="failed", message="账号触发二次验证，请在天翼云官方页面完成验证后重试",
                    reason="need_extra_verify", next_action="open_official_login",
                    diagnostics=poll_diag,
                )
                return self._state_to_dict()

            self._set_state(
                stage="running", message="等待扫码登录",
                reason="waiting_scan", next_action="scan_qr",
                diagnostics=poll_diag,
            )
            return self._state_to_dict()

    async def stop_qr_login(self, session_id: str = "") -> dict:
        """停止 QR 登录会话。"""
        async with self._lock:
            ctx = self._context
            if ctx is None:
                return self._state_to_dict()
            if session_id and session_id != str(ctx.get("session_id", "")):
                return self._state_to_dict()
            self._set_state(
                session_id=str(ctx.get("session_id", "")),
                stage="failed", message="已停止登录",
                reason="stopped", next_action="retry",
            )
            await self._close_context()
            return self._state_to_dict()

    async def get_qr_image(self, session_id: str = "") -> Tuple[bytes, str]:
        """获取二维码图片二进制。"""
        async with self._lock:
            ctx = self._context
            if ctx is None:
                raise LoginFailedError("二维码会话不存在，请先刷新二维码")
            if session_id and session_id != str(ctx.get("session_id", "")):
                raise LoginFailedError("二维码会话已更新，请刷新页面")

            client = ctx.get("client")
            if not isinstance(client, aiohttp.ClientSession):
                raise LoginFailedError("二维码会话异常，请刷新二维码")

            image_url = str(ctx.get("image_remote_url", ""))
            if not image_url:
                raise LoginFailedError("二维码地址缺失，请刷新二维码")

            headers = self._qr_headers(ctx)
            async with client.get(image_url, headers=headers) as resp:
                if resp.status >= 400:
                    raise LoginFailedError(f"二维码图片获取失败 HTTP {resp.status}")
                content_type = str(resp.headers.get("Content-Type", "image/jpeg"))
                body = await resp.read()
                if not body:
                    raise LoginFailedError("二维码图片为空")
                return body, content_type

    # ── 内部 ──────────────────────────────

    async def _bootstrap(self, client: aiohttp.ClientSession) -> dict:
        """初始化 QR 登录上下文。"""
        ua = default_ua()
        base_headers = {"User-Agent": ua, "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"}

        # Step 1: 获取 loginEntry
        redirect = quote(_TIANYI_REDIRECT_TARGET, safe="")
        bootstrap_url = _TIANYI_BOOTSTRAP_URL.format(redirect=redirect)
        step1_status = 0
        async with client.get(bootstrap_url, headers=base_headers, allow_redirects=False) as resp:
            step1_status = resp.status
            if resp.status in (301, 302, 303, 307, 308):
                login_entry = str(resp.headers.get("Location", "")).strip()
            else:
                login_entry = str(resp.url)
        if not login_entry:
            _LOGGER.warning("QR bootstrap step1 failed: status=%s url=%s", step1_status, bootstrap_url)
            raise LoginFailedError("未获取到天翼登录入口")

        # Step 2: 跟随重定向获取登录页面参数
        async with client.get(login_entry, headers=base_headers, allow_redirects=True) as resp:
            login_page_url = str(resp.url)
            await resp.text()

        query = parse_qs(urlparse(login_page_url).query or "", keep_blank_values=True)
        app_id = str((query.get("appId") or [""])[0]).strip()
        lt = str((query.get("lt") or [""])[0]).strip()
        req_id = str((query.get("reqId") or [""])[0]).strip()
        if not app_id or not lt or not req_id:
            _LOGGER.warning("QR bootstrap step2: missing params appId=%s lt=%s reqId=%s url=%s",
                            app_id, lt, req_id, login_page_url)
            raise LoginFailedError("登录页面参数缺失 (appId/lt/reqId)")

        # Step 3: appConf
        api_headers = self._qr_headers({"req_id": req_id, "lt": lt, "login_page_url": login_page_url})
        async with client.post(_TIANYI_APPCONF_URL, data={"version": "2.0", "appKey": app_id}, headers=api_headers) as resp:
            conf_text = await resp.text()
            if resp.status >= 400:
                raise LoginFailedError(f"appConf HTTP {resp.status}")
        conf = parse_json_like(conf_text)
        conf_data = conf.get("data")
        if not isinstance(conf_data, dict):
            raise LoginFailedError("appConf 返回异常")

        # Step 4: getUUID
        async with client.post(_TIANYI_GETUUID_URL, data={"appId": app_id}, headers=api_headers) as resp:
            uuid_text = await resp.text()
            if resp.status >= 400:
                raise LoginFailedError(f"getUUID HTTP {resp.status}")
        uuid_data = parse_json_like(uuid_text)
        code = pick_first(uuid_data, "res_result", "result", "resultCode")
        if code is not None:
            try:
                code = int(code or 0)
            except (TypeError, ValueError):
                code = None
        if code != QR_SUCCESS:
            raise LoginFailedError(str(uuid_data.get("msg", "二维码生成失败")))

        uuid_val = str(uuid_data.get("uuid", "")).strip()
        encryuuid = str(uuid_data.get("encryuuid", "")).strip()
        encodeuuid = str(uuid_data.get("encodeuuid", "")).strip()
        if not uuid_val or not encryuuid or not encodeuuid:
            raise LoginFailedError("二维码参数缺失")

        image_remote_url = f"https://open.e.189.cn/api/logbox/oauth2/image.do?uuid={encodeuuid}&REQID={req_id}"

        state_payload = {
            "appId": app_id, "encryuuid": encryuuid,
            "date": str(int(time.time() * 1000)), "uuid": uuid_val,
            "returnUrl": str(conf_data.get("returnUrl") or ""),
            "clientType": str(conf_data.get("clientType") or "1"),
            "timeStamp": str(int(time.time() * 1000)),
            "cb_SaveName": str(conf_data.get("defaultSaveName") or ""),
            "isOauth2": "false" if str(conf_data.get("isOauth2", "")).lower() == "false" else "true",
            "state": str(conf_data.get("state") or ""),
            "paramId": str(conf_data.get("paramId") or ""),
        }

        _LOGGER.info("QR bootstrap ok: app_id=%s image_url=%s", app_id, image_remote_url[:80])
        return {
            "app_id": app_id, "lt": lt, "req_id": req_id,
            "login_page_url": login_page_url,
            "image_remote_url": image_remote_url,
            "state_payload": state_payload,
        }

    async def _finalize(self, ctx: dict, redirect_url: str) -> Tuple[str, str, str]:
        """扫码成功后跟随重定向 → 提取 Cookie → 校验。"""
        client = ctx.get("client")
        if not isinstance(client, aiohttp.ClientSession):
            return "", "", "client_invalid"

        # 使用 QR 专用 headers（含 Referer），维持会话连续性
        headers = self._qr_headers(ctx)
        headers["Accept"] = "text/html,application/xhtml+xml,*/*;q=0.8"

        urls = [redirect_url] if redirect_url else []
        urls.append(_TIANYI_REDIRECT_TARGET)

        final_urls: list[str] = []
        for url in urls:
            if not url:
                continue
            try:
                async with client.get(url, headers=headers, allow_redirects=True, max_redirects=10) as resp:
                    await resp.read()
                final_urls.append(str(resp.url))
                _LOGGER.info("QR finalize: %s → %s", url[:80], str(resp.url)[:80])
            except Exception as exc:
                _LOGGER.warning("QR finalize: redirect %s failed: %s", url[:80], exc)

        # 提取 cookie — 优先用 filter_cookies（最可靠）
        cookie_parts: dict[str, str] = {}
        for domain in ("cloud.189.cn", ".cloud.189.cn", "open.e.189.cn", ".e.189.cn"):
            try:
                cookies = client.cookie_jar.filter_cookies(aiohttp.URL(f"https://{domain}/"))
                for k, v in cookies.items():
                    if k:
                        cookie_parts[k] = v.value
            except Exception as exc:
                _LOGGER.debug("QR finalize: filter %s failed: %s", domain, exc)

        # 兜底：遍历整个 jar
        if not cookie_parts:
            try:
                for morsel in client.cookie_jar:
                    try:
                        k = getattr(morsel, "key", None)
                        v = getattr(morsel, "value", None)
                        if k and v and isinstance(k, str):
                            cookie_parts[k] = v
                    except Exception:
                        continue
            except Exception as exc:
                _LOGGER.warning("QR finalize: iter jar failed: %s", exc)

        cookie = "; ".join(f"{k}={v}" for k, v in cookie_parts.items())
        _LOGGER.info("QR finalize: keys=%s count=%d final_urls=%s",
                      sorted(cookie_parts.keys()), len(cookie_parts), final_urls)

        if not cookie:
            return "", "", "cookie_missing"

        try:
            cred = await validate_cookie(cookie)
            _LOGGER.info("QR finalize: OK account=%s", cred.account_name)
            return cred.account_name, cookie, ""
        except LoginExpiredError as exc:
            _LOGGER.warning("QR finalize: invalid: %s", exc)
            return "", cookie, str(exc)
        except Exception as exc:
            _LOGGER.warning("QR finalize: unexpected: %s", exc)
            return "", cookie, str(exc)

    def _qr_headers(self, ctx: dict) -> dict:
        return {
            "User-Agent": "Mozilla/5.0 (Freedeck/1.0)",
            "Accept": "application/json, text/plain, */*",
            "Referer": str(ctx.get("login_page_url", _TIANYI_REDIRECT_TARGET)),
            "REQID": str(ctx.get("req_id", "")),
            "lt": str(ctx.get("lt", "")),
        }

    def _set_state(self, **kwargs):
        for k, v in kwargs.items():
            if hasattr(self._state, k):
                setattr(self._state, k, v)

    def _state_to_dict(self) -> dict:
        return {
            "session_id": self._state.session_id,
            "stage": self._state.stage,
            "message": self._state.message,
            "reason": self._state.reason,
            "next_action": self._state.next_action,
            "user_account": self._state.user_account,
            "image_url": self._state.image_url,
            "expires_at": self._state.expires_at,
            "diagnostics": self._state.diagnostics,
        }

    async def _close_context(self):
        ctx = self._context
        if ctx is None:
            return
        await self._safe_close(ctx.get("client"))
        self._context = None

    async def _safe_close(self, client):
        if not isinstance(client, aiohttp.ClientSession):
            return
        try:
            await client.close()
        except Exception:
            pass
