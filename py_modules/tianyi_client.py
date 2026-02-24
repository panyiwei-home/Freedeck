# tianyi_client.py - 天翼官方接口客户端
#
# 该模块封装登录校验、分享解析、直链获取。

from __future__ import annotations

import asyncio
import hashlib
import html
import json
import logging
import os
import re
import ssl
import subprocess
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple
from urllib.parse import parse_qs, urlencode, urljoin, urlparse

try:
    # 某些 Decky 运行环境可能缺少 xml 标准库子模块，做兼容降级。
    from xml.etree import ElementTree as ET
except Exception:
    ET = None  # type: ignore[assignment]

import aiohttp

TLS_CA_CANDIDATE_FILES = (
    "/etc/ssl/certs/ca-certificates.crt",
    "/etc/pki/tls/certs/ca-bundle.crt",
    "/etc/ssl/cert.pem",
    "/etc/openssl/certs/ca-certificates.crt",
)
_XML_TOKEN_RE = re.compile(
    r"<(?P<tag>[A-Za-z_][\w:\-\.]*)[^>]*>(?P<body>.*?)</\1>",
    re.DOTALL,
)
_JSONP_RE = re.compile(r"^\s*[\w\.\$]+\((?P<body>[\s\S]+)\)\s*;?\s*$")
_SHARE_ID_PATTERNS = (
    re.compile(r'"share[Ii][Dd]"\s*:\s*"(?P<id>[A-Za-z0-9_-]{4,})"'),
    re.compile(r'"share[Ii][Dd]"\s*:\s*(?P<id>[A-Za-z0-9_-]{4,})'),
    re.compile(r"\bshare[Ii][Dd]\s*[:=]\s*['\"]?(?P<id>[A-Za-z0-9_-]{4,})['\"]?"),
    re.compile(r"[?&]shareId=(?P<id>[A-Za-z0-9_-]{4,})", re.IGNORECASE),
)
_LOGGER = logging.getLogger("freedeck")
_DEFAULT_BROWSER_UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.6478.183 Safari/537.36"
)
_JS_SHARE_RESOLVER_RELATIVE_PATH = ("backend", "tianyi_share_resolver.js")
_JS_CLOUD_UPLOAD_RELATIVE_PATHS: Tuple[Tuple[str, str], ...] = (
    ("backend", "tianyi_cloud_upload.js"),
    ("backend", "tianyi_cloud_upload.cjs"),
)

@dataclass
class ResolvedFile:
    """分享解析出的文件项。"""

    file_id: str
    name: str
    size: int
    is_folder: bool

    def to_dict(self) -> Dict[str, object]:
        """转字典给前端使用。"""
        return asdict(self)


@dataclass
class ResolvedShare:
    """分享解析结果。"""

    share_code: str
    share_id: str
    pwd: str
    files: List[ResolvedFile]

    def to_dict(self) -> Dict[str, object]:
        """转字典给前端使用。"""
        return {
            "share_code": self.share_code,
            "share_id": self.share_id,
            "pwd": self.pwd,
            "files": [f.to_dict() for f in self.files],
        }


@dataclass(frozen=True)
class ShareRequestProfile:
    """分享接口请求画像。"""

    name: str
    host: str
    endpoint: str
    method: str = "GET"
    use_form: bool = False


_SHARE_INFO_PROFILES: Tuple[ShareRequestProfile, ...] = (
    ShareRequestProfile(
        name="getShareInfoByCodeV2_get_cloud_query",
        host="cloud.189.cn",
        endpoint="/api/open/share/getShareInfoByCodeV2.action",
        method="GET",
        use_form=False,
    ),
    ShareRequestProfile(
        name="getShareInfoByCodeV2_post_cloud_form",
        host="cloud.189.cn",
        endpoint="/api/open/share/getShareInfoByCodeV2.action",
        method="POST",
        use_form=True,
    ),
    ShareRequestProfile(
        name="getShareInfoByCodeV2_post_api_form",
        host="api.cloud.189.cn",
        endpoint="/open/share/getShareInfoByCodeV2.action",
        method="POST",
        use_form=True,
    ),
)

_SHARE_CHECK_PROFILES: Tuple[ShareRequestProfile, ...] = (
    ShareRequestProfile(
        name="checkAccessCode_get_cloud_query",
        host="cloud.189.cn",
        endpoint="/api/open/share/checkAccessCode.action",
        method="GET",
        use_form=False,
    ),
    ShareRequestProfile(
        name="checkAccessCode_post_cloud_form",
        host="cloud.189.cn",
        endpoint="/api/open/share/checkAccessCode.action",
        method="POST",
        use_form=True,
    ),
    ShareRequestProfile(
        name="checkAccessCode_post_api_form",
        host="api.cloud.189.cn",
        endpoint="/open/share/checkAccessCode.action",
        method="POST",
        use_form=True,
    ),
)

_SHARE_LIST_PROFILES: Tuple[ShareRequestProfile, ...] = (
    ShareRequestProfile(
        name="listShareDir_get_cloud_query",
        host="cloud.189.cn",
        endpoint="/api/open/share/listShareDir.action",
        method="GET",
        use_form=False,
    ),
    ShareRequestProfile(
        name="listShareDir_get_api_query",
        host="api.cloud.189.cn",
        endpoint="/open/share/listShareDir.action",
        method="GET",
        use_form=False,
    ),
    ShareRequestProfile(
        name="listShareDir_post_cloud_form",
        host="cloud.189.cn",
        endpoint="/api/open/share/listShareDir.action",
        method="POST",
        use_form=True,
    ),
)


class TianyiApiError(RuntimeError):
    """天翼接口异常。"""

    def __init__(self, message: str, *, diagnostics: Optional[Dict[str, object]] = None):
        super().__init__(str(message))
        self.diagnostics: Dict[str, object] = diagnostics or {}


def parse_share_url(share_url: str) -> Tuple[str, str]:
    """解析分享链接，返回 share_code 与 pwd。"""
    raw = (share_url or "").strip()
    if not raw:
        raise TianyiApiError("分享链接为空")
    parsed = urlparse(raw)
    host = (parsed.netloc or "").lower()
    if host not in {"cloud.189.cn", "www.cloud.189.cn"}:
        raise TianyiApiError("链接格式无效，仅支持 cloud.189.cn 分享链接")
    path_parts = [part for part in (parsed.path or "").split("/") if part]
    if len(path_parts) < 2 or path_parts[0] != "t":
        raise TianyiApiError("链接格式无效，缺少 shareCode")
    share_code = path_parts[1].strip()
    if not share_code:
        raise TianyiApiError("链接格式无效，shareCode 为空")
    qs = parse_qs(parsed.query or "", keep_blank_values=True)
    pwd = (qs.get("pwd", [""])[0] or "").strip()
    return share_code, pwd


def _now_ms() -> int:
    """返回毫秒时间戳。"""
    return int(time.time() * 1000)


def _get_json_value(payload: Dict[str, object], *keys: str) -> str:
    """按候选键提取字符串值。"""
    lower_map: Dict[str, object] = {}
    for raw_key, raw_value in payload.items():
        lower_map[str(raw_key).strip().lower()] = raw_value
    for key in keys:
        value = payload.get(key)
        if value is None:
            value = lower_map.get(str(key).strip().lower())
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    target_keys = {str(key).strip().lower() for key in keys if str(key).strip()}
    if not target_keys:
        return ""
    stack: List[object] = [payload]
    visited_ids = set()
    while stack:
        current = stack.pop()
        obj_id = id(current)
        if obj_id in visited_ids:
            continue
        visited_ids.add(obj_id)
        if isinstance(current, dict):
            for raw_key, raw_value in current.items():
                key_name = str(raw_key).strip().lower()
                if key_name in target_keys:
                    text = str(raw_value or "").strip()
                    if text:
                        return text
                if isinstance(raw_value, (dict, list)):
                    stack.append(raw_value)
        elif isinstance(current, list):
            for item in current:
                if isinstance(item, (dict, list)):
                    stack.append(item)
    return ""


def _parse_int(value: object, default: int = 0) -> int:
    """解析整数。"""
    try:
        return int(str(value))
    except Exception:
        return default


def _as_optional_int(value: object) -> Optional[int]:
    """解析可选整数。"""
    try:
        if value is None:
            return None
        text = str(value).strip()
        if not text:
            return None
        return int(text)
    except Exception:
        return None


def _extract_api_error(payload: Dict[str, object]) -> str:
    """提取接口错误码与错误文案。"""
    if not isinstance(payload, dict):
        return ""
    code = _get_json_value(payload, "res_code", "resCode", "code", "errorCode")
    message = _get_json_value(
        payload,
        "res_message",
        "resMessage",
        "msg",
        "message",
        "errorMsg",
        "errorMessage",
        "desc",
        "description",
    )
    if code and message:
        return f"code={code}, msg={message}"
    if message:
        return message
    if code:
        return f"code={code}"
    return ""


def _short_text(text: str, limit: int = 280) -> str:
    """裁剪长文本并转单行，避免日志污染。"""
    raw = str(text or "").replace("\r", "\\r").replace("\n", "\\n")
    if len(raw) <= limit:
        return raw
    return raw[:limit] + "..."


def _normalize_json_payload(data: object) -> Dict[str, object]:
    """把接口返回统一规整为字典结构。"""
    if isinstance(data, dict):
        return data

    if isinstance(data, list):
        if len(data) == 1 and isinstance(data[0], dict):
            return data[0]
        return {"_raw_list": data}

    if isinstance(data, str):
        raw = str(data or "").strip()
        if not raw:
            return {"_raw_text": ""}

        if raw.startswith("<"):
            xml_payload = _try_parse_xml_payload(raw)
            if isinstance(xml_payload, dict):
                return xml_payload

        jsonp_match = _JSONP_RE.match(raw)
        if jsonp_match:
            inner = str(jsonp_match.group("body") or "").strip()
            try:
                return _normalize_json_payload(json.loads(inner))
            except Exception:
                pass

        if raw.startswith("{") or raw.startswith("["):
            try:
                return _normalize_json_payload(json.loads(raw))
            except Exception:
                pass

        return {"message": raw, "_raw_text": raw}

    return {"value": data}


def _find_nested_value(payload: Dict[str, object], *keys: str) -> object:
    """在任意层级查找目标键对应的值。"""
    target_keys = {str(key).strip().lower() for key in keys if str(key).strip()}
    if not target_keys:
        return None

    stack: List[object] = [payload]
    visited_ids = set()
    while stack:
        current = stack.pop()
        obj_id = id(current)
        if obj_id in visited_ids:
            continue
        visited_ids.add(obj_id)

        if isinstance(current, dict):
            for raw_key, raw_value in current.items():
                key_name = str(raw_key).strip().lower()
                if key_name in target_keys:
                    return raw_value
                if isinstance(raw_value, (dict, list)):
                    stack.append(raw_value)
        elif isinstance(current, list):
            for item in current:
                if isinstance(item, (dict, list)):
                    stack.append(item)
    return None


def _extract_share_id_from_text(raw_text: str) -> str:
    """从文本（HTML/URL）中提取 shareId。"""
    text = str(raw_text or "")
    if not text:
        return ""
    for pattern in _SHARE_ID_PATTERNS:
        match = pattern.search(text)
        if not match:
            continue
        share_id = str(match.group("id") or "").strip()
        if share_id:
            return share_id
    return ""


def _append_attempt(
    attempts: List[Dict[str, object]],
    *,
    step: str,
    endpoint: str,
    ok: bool,
    message: str = "",
    share_id: str = "",
    host: str = "",
    method: str = "",
    profile: str = "",
    status: Optional[int] = None,
    body_type: str = "",
    body_preview: str = "",
) -> None:
    """记录 shareId 解析尝试，便于最终诊断。"""
    item: Dict[str, object] = {
        "step": str(step or ""),
        "endpoint": str(endpoint or ""),
        "ok": bool(ok),
    }
    if message:
        item["message"] = _short_text(message, 320)
    if share_id:
        item["share_id"] = str(share_id)
    if host:
        item["host"] = str(host)
    if method:
        item["method"] = str(method).upper()
    if profile:
        item["profile"] = str(profile)
    if status is not None:
        item["status"] = int(status)
    if body_type:
        item["body_type"] = str(body_type)
    if body_preview:
        item["body_preview"] = _short_text(body_preview, 320)
    attempts.append(item)


def _detect_body_type(raw_text: str) -> str:
    """识别响应体类型。"""
    stripped = str(raw_text or "").strip()
    if not stripped:
        return "empty"
    lower = stripped[:128].lower()
    if stripped.startswith("{") or stripped.startswith("["):
        return "json"
    if _JSONP_RE.match(stripped):
        return "jsonp"
    if stripped.startswith("<"):
        if "<!doctype html" in lower or "<html" in lower:
            return "html"
        return "xml"
    return "text"


def _share_browser_headers(
    cookie: str,
    *,
    referer_url: str,
    use_form: bool,
    include_sign_type: bool,
) -> Dict[str, str]:
    """构建分享接口浏览器画像请求头。"""
    headers: Dict[str, str] = {
        "User-Agent": _DEFAULT_BROWSER_UA,
        "Cookie": str(cookie or ""),
        # 与 Gamebox 保持一致，降低服务端返回空体概率。
        "Accept": "application/json;charset=UTF-8",
        "Referer": str(referer_url or "https://cloud.189.cn/"),
    }
    if include_sign_type:
        headers["Sign-Type"] = "1"
    if use_form:
        headers["Content-Type"] = "application/x-www-form-urlencoded; charset=UTF-8"
    return headers


def _build_profile_url(profile: ShareRequestProfile, query_params: Dict[str, str]) -> str:
    """按画像与 query 参数构建 URL。"""
    url = f"https://{profile.host}{profile.endpoint}"
    if query_params:
        url = url + "?" + urlencode(query_params)
    return url


async def _request_share_profile(
    session: aiohttp.ClientSession,
    *,
    cookie: str,
    profile: ShareRequestProfile,
    query_params: Optional[Dict[str, str]] = None,
    form_params: Optional[Dict[str, str]] = None,
    referer_url: str,
    allow_redirects: bool = True,
) -> Tuple[Dict[str, object], Dict[str, object]]:
    """按请求画像请求分享接口并返回 payload + 诊断元信息。"""
    method = str(profile.method or "GET").strip().upper() or "GET"
    query_data = {str(k): str(v) for k, v in (query_params or {}).items() if v is not None}
    form_data = {str(k): str(v) for k, v in (form_params or {}).items() if v is not None}
    url = _build_profile_url(profile, query_data if method == "GET" else {})

    include_sign_type = "listShareDir.action" in str(profile.endpoint or "")
    req_kwargs: Dict[str, object] = {
        "headers": _share_browser_headers(
            cookie,
            referer_url=referer_url,
            use_form=bool(profile.use_form),
            include_sign_type=include_sign_type,
        ),
        "allow_redirects": bool(allow_redirects),
    }
    if method != "GET":
        if profile.use_form:
            req_kwargs["data"] = form_data or query_data
        elif query_data:
            url = _build_profile_url(profile, query_data)
            if form_data:
                req_kwargs["data"] = form_data
        elif form_data:
            req_kwargs["data"] = form_data

    meta: Dict[str, object] = {
        "host": profile.host,
        "method": method,
        "profile": profile.name,
        "endpoint": profile.endpoint,
        "status": 0,
        "body_type": "empty",
        "body_preview": "",
    }

    try:
        async with session.request(method, url, **req_kwargs) as resp:
            raw_text = await resp.text()
            body_type = _detect_body_type(raw_text)
            body_preview = _short_text(raw_text, 320)
            meta.update(
                {
                    "status": int(resp.status),
                    "body_type": body_type,
                    "body_preview": body_preview,
                }
            )

            if resp.status >= 400:
                raise TianyiApiError(
                    f"请求失败 status={resp.status} endpoint={profile.endpoint}",
                    diagnostics=dict(meta),
                )

            if not str(raw_text or "").strip():
                raise TianyiApiError(
                    f"响应体为空 endpoint={profile.endpoint} status={resp.status}",
                    diagnostics=dict(meta),
                )

            payload = _normalize_json_payload(raw_text)
            if not isinstance(payload, dict):
                raise TianyiApiError(
                    f"接口返回格式异常 endpoint={profile.endpoint} type={type(payload).__name__}",
                    diagnostics=dict(meta),
                )
            return payload, meta
    except TianyiApiError:
        raise
    except aiohttp.ClientConnectorCertificateError as exc:
        raise TianyiApiError(f"TLS证书校验失败: {exc}", diagnostics=dict(meta)) from exc
    except aiohttp.ClientSSLError as exc:
        raise TianyiApiError(f"TLS连接失败: {exc}", diagnostics=dict(meta)) from exc
    except aiohttp.ClientError as exc:
        raise TianyiApiError(f"网络请求失败: {exc}", diagnostics=dict(meta)) from exc


async def _fetch_share_id_from_share_page(
    session: aiohttp.ClientSession,
    *,
    share_code: str,
    pwd: str,
    cookie: str,
) -> str:
    """访问分享落地页，尝试从 HTML 中提取 shareId。"""
    params: Dict[str, str] = {}
    if pwd:
        params["pwd"] = pwd
    page_url = f"https://cloud.189.cn/t/{share_code}"
    if params:
        page_url = page_url + "?" + urlencode(params)

    try:
        async with session.get(
            page_url,
            headers=_share_browser_headers(
                cookie,
                referer_url="https://cloud.189.cn/",
                use_form=False,
                include_sign_type=False,
            ),
            allow_redirects=True,
        ) as resp:
            text = await resp.text()
            if resp.status >= 400:
                raise TianyiApiError(
                    f"分享页请求失败 status={resp.status} endpoint={urlparse(page_url).path or page_url}"
                )
            share_id = _extract_share_id_from_text(text)
            if not share_id:
                share_id = _extract_share_id_from_text(str(resp.url))
            if share_id:
                return share_id
            raise TianyiApiError(
                "分享页解析失败：未找到shareId"
                + f" endpoint={urlparse(page_url).path or page_url} body={_short_text(text)}"
            )
    except aiohttp.ClientConnectorCertificateError as exc:
        raise TianyiApiError(f"TLS证书校验失败: {exc}") from exc
    except aiohttp.ClientSSLError as exc:
        raise TianyiApiError(f"TLS连接失败: {exc}") from exc
    except aiohttp.ClientError as exc:
        raise TianyiApiError(f"网络请求失败: {exc}") from exc


def _get_js_share_resolver_path() -> str:
    """定位 JS 分享解析器路径。"""
    candidates: List[Path] = []

    plugin_dir = str(os.environ.get("DECKY_PLUGIN_DIR", "") or "").strip()
    if plugin_dir:
        candidates.append(Path(plugin_dir, *_JS_SHARE_RESOLVER_RELATIVE_PATH))

    # py_modules/tianyi_client.py -> plugin_root/backend/tianyi_share_resolver.js
    candidates.append(Path(__file__).resolve().parents[1].joinpath(*_JS_SHARE_RESOLVER_RELATIVE_PATH))

    for candidate in candidates:
        try:
            if candidate.is_file():
                return str(candidate)
        except Exception:
            continue
    return ""


def _build_resolved_share_from_payload(data: Dict[str, object]) -> ResolvedShare:
    """将 JS 解析器返回值转换为 ResolvedShare。"""
    if not isinstance(data, dict):
        raise TianyiApiError("JS 解析器返回格式异常")

    share_code = str(data.get("share_code", "") or "").strip()
    share_id = str(data.get("share_id", "") or "").strip()
    pwd = str(data.get("pwd", "") or "").strip()
    if not share_code or not share_id:
        raise TianyiApiError("JS 解析器返回缺少 share_code/share_id")

    files_raw = data.get("files")
    files: List[ResolvedFile] = []
    if isinstance(files_raw, list):
        for item in files_raw:
            if not isinstance(item, dict):
                continue
            file_id = str(item.get("file_id", "") or "").strip()
            if not file_id:
                continue
            name = str(item.get("name", "") or "").strip() or f"file-{file_id}"
            size = _parse_int(item.get("size", 0), 0)
            is_folder = bool(item.get("is_folder", False))
            files.append(
                ResolvedFile(
                    file_id=file_id,
                    name=name,
                    size=max(0, size),
                    is_folder=is_folder,
                )
            )

    return ResolvedShare(
        share_code=share_code,
        share_id=share_id,
        pwd=pwd,
        files=files,
    )


async def _resolve_share_via_js(share_url: str, cookie: str) -> ResolvedShare:
    """使用 JS 版链路解析分享（复刻 GameBox 顺序）。"""
    script_path = _get_js_share_resolver_path()
    if not script_path:
        raise TianyiApiError("JS 解析器不存在")

    payload = json.dumps(
        {
            "share_url": str(share_url or "").strip(),
            "cookie": str(cookie or "").strip(),
        },
        ensure_ascii=False,
    ).encode("utf-8")

    def _build_node_env() -> Dict[str, str]:
        env: Dict[str, str] = {}
        for key, value in os.environ.items():
            k = str(key or "").strip()
            if not k:
                continue
            env[k] = str(value or "")

        # Decky/打包环境可能注入 _MEI 临时库路径，导致 node 链接到错误 libcrypto。
        ld_orig = str(env.get("LD_LIBRARY_PATH_ORIG", "") or "").strip()
        if ld_orig:
            env["LD_LIBRARY_PATH"] = ld_orig
        else:
            env.pop("LD_LIBRARY_PATH", None)

        env.pop("PYTHONHOME", None)
        env.pop("PYTHONPATH", None)
        env.pop("_MEIPASS2", None)

        if not str(env.get("PATH", "")).strip():
            env["PATH"] = "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
        return env

    node_env = _build_node_env()

    def _run_node() -> subprocess.CompletedProcess[bytes]:
        return subprocess.run(
            ["node", script_path],
            input=payload,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=node_env,
            check=False,
            timeout=28.0,
        )

    try:
        completed = await asyncio.to_thread(_run_node)
    except FileNotFoundError as exc:
        raise TianyiApiError("系统缺少 node 运行时，无法启用 JS 解析器") from exc
    except subprocess.TimeoutExpired as exc:
        raise TianyiApiError("JS 解析器执行超时") from exc
    except Exception as exc:
        raise TianyiApiError(f"启动 JS 解析器失败: {exc}") from exc

    raw_out = bytes(completed.stdout or b"").decode("utf-8", errors="ignore").strip()
    raw_err = bytes(completed.stderr or b"").decode("utf-8", errors="ignore").strip()

    if int(completed.returncode or 0) != 0:
        raise TianyiApiError(f"JS 解析器执行失败: {raw_err or f'code={completed.returncode}'}")
    if not raw_out:
        raise TianyiApiError(f"JS 解析器无输出: {raw_err or 'empty_stdout'}")

    try:
        result = json.loads(raw_out)
    except Exception as exc:
        raise TianyiApiError(f"JS 解析器输出非 JSON: {exc}; out={_short_text(raw_out)}") from exc

    if not isinstance(result, dict):
        raise TianyiApiError("JS 解析器输出结构异常")

    ok = bool(result.get("ok", False))
    if not ok:
        error_message = str(result.get("error", "") or "JS 解析失败").strip()
        diagnostics = result.get("diagnostics")
        if isinstance(diagnostics, dict) and diagnostics:
            raise TianyiApiError(error_message, diagnostics=diagnostics)
        raise TianyiApiError(error_message)

    data = result.get("data")
    if not isinstance(data, dict):
        raise TianyiApiError("JS 解析器缺少 data")
    return _build_resolved_share_from_payload(data)


def _headers(cookie: str) -> Dict[str, str]:
    """构建基础请求头。"""
    return {
        "User-Agent": _DEFAULT_BROWSER_UA,
        "Cookie": cookie,
        "Accept": "application/json, text/plain, */*",
    }


def _build_tls_context() -> ssl.SSLContext:
    """构建统一 TLS 上下文，兼容 SteamOS 证书链差异。"""
    candidates: List[str] = []
    env_cert_file = str(os.environ.get("SSL_CERT_FILE", "") or "").strip()
    if env_cert_file:
        candidates.append(env_cert_file)
    candidates.extend(list(TLS_CA_CANDIDATE_FILES))

    try:
        import certifi  # type: ignore

        certifi_path = str(certifi.where() or "").strip()
        if certifi_path:
            candidates.append(certifi_path)
    except Exception:
        pass

    dedup: List[str] = []
    seen = set()
    for raw in candidates:
        path = os.path.realpath(os.path.expanduser(str(raw).strip()))
        if not path or path in seen:
            continue
        seen.add(path)
        dedup.append(path)

    for path in dedup:
        if not os.path.isfile(path):
            continue
        try:
            return ssl.create_default_context(cafile=path)
        except Exception:
            continue

    context = ssl.create_default_context()

    # 仅用于紧急排障，默认保持证书校验开启。
    insecure_flag = str(os.environ.get("FREEDECK_QR_INSECURE_TLS", "") or "").strip().lower()
    if insecure_flag in {"1", "true", "yes"}:
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
    return context


def _create_session(*, timeout: aiohttp.ClientTimeout) -> aiohttp.ClientSession:
    """创建带 TLS 修复的会话。"""
    connector = aiohttp.TCPConnector(ssl=_build_tls_context())
    return aiohttp.ClientSession(timeout=timeout, connector=connector)


def _strip_xml_tag(tag: str) -> str:
    """去掉 XML 标签命名空间前缀。"""
    text = str(tag or "")
    if "}" in text:
        return text.split("}", 1)[1]
    return text


def _xml_node_to_value(node: ET.Element) -> object:
    """递归把 XML 节点转换为字典/文本。"""
    children = list(node)
    if not children:
        return (node.text or "").strip()

    payload: Dict[str, object] = {}
    for child in children:
        key = _strip_xml_tag(child.tag)
        value = _xml_node_to_value(child)
        if key in payload:
            current = payload[key]
            if isinstance(current, list):
                current.append(value)
            else:
                payload[key] = [current, value]
        else:
            payload[key] = value
    return payload


def _merge_xml_value(payload: Dict[str, object], key: str, value: object) -> None:
    """合并重复 XML 节点。"""
    if key in payload:
        current = payload[key]
        if isinstance(current, list):
            current.append(value)
        else:
            payload[key] = [current, value]
        return
    payload[key] = value


def _try_parse_xml_payload_fallback(text: str) -> Optional[Dict[str, object]]:
    """在缺少 xml.etree 时，使用轻量规则解析 XML。"""
    raw = str(text or "").strip()
    if not raw.startswith("<"):
        return None

    def _parse_fragment(fragment: str) -> Optional[Dict[str, object]]:
        matches = list(_XML_TOKEN_RE.finditer(fragment))
        if not matches:
            return None

        payload: Dict[str, object] = {}
        for match in matches:
            tag = _strip_xml_tag(match.group("tag"))
            body = str(match.group("body") or "").strip()
            nested = _parse_fragment(body)
            value: object = nested if isinstance(nested, dict) and nested else html.unescape(body)
            _merge_xml_value(payload, tag, value)
        return payload

    parsed = _parse_fragment(raw)
    if not parsed:
        return None
    if len(parsed) == 1:
        only_key = next(iter(parsed.keys()))
        only_value = parsed[only_key]
        if isinstance(only_value, dict):
            return only_value
    return parsed


def _try_parse_xml_payload(text: str) -> Optional[Dict[str, object]]:
    """尝试解析 XML 文本为字典。"""
    raw = str(text or "").strip()
    if not raw.startswith("<"):
        return None
    if ET is None:
        return _try_parse_xml_payload_fallback(raw)
    try:
        root = ET.fromstring(raw)
    except Exception:
        return _try_parse_xml_payload_fallback(raw)

    value = _xml_node_to_value(root)
    if isinstance(value, dict):
        return value
    return {_strip_xml_tag(root.tag): value}


async def _json_get(
    session: aiohttp.ClientSession,
    url: str,
    cookie: str,
    *,
    allow_redirects: bool = False,
) -> Dict[str, object]:
    """执行 GET 并返回 JSON。"""
    try:
        async with session.get(
            url,
            headers=_headers(cookie),
            allow_redirects=allow_redirects,
        ) as resp:
            raw_text = await resp.text()
            if resp.status >= 400:
                raise TianyiApiError(
                    f"请求失败 status={resp.status} endpoint={urlparse(url).path or url} body={_short_text(raw_text)}"
                )
            if not str(raw_text or "").strip():
                raise TianyiApiError(
                    f"响应体为空 endpoint={urlparse(url).path or url} status={resp.status}"
                )
            try:
                data = json.loads(raw_text)
            except Exception as exc:
                xml_payload = _try_parse_xml_payload(raw_text)
                if isinstance(xml_payload, dict):
                    return xml_payload
                raise TianyiApiError(
                    "响应解析失败（JSON/XML均不可用）"
                    + f" endpoint={urlparse(url).path or url} status={resp.status}: {exc}; body={_short_text(raw_text)}"
                ) from exc

            if not isinstance(data, dict):
                _LOGGER.warning(
                    "天翼接口返回非对象JSON: endpoint=%s type=%s body=%s",
                    urlparse(url).path or url,
                    type(data).__name__,
                    _short_text(raw_text),
                )
            payload = _normalize_json_payload(data)
            if not isinstance(payload, dict):
                raise TianyiApiError(
                    f"接口返回格式异常 endpoint={urlparse(url).path or url} type={type(data).__name__}"
                )
            return payload
    except aiohttp.ClientConnectorCertificateError as exc:
        raise TianyiApiError(f"TLS证书校验失败: {exc}") from exc
    except aiohttp.ClientSSLError as exc:
        raise TianyiApiError(f"TLS连接失败: {exc}") from exc
    except aiohttp.ClientError as exc:
        raise TianyiApiError(f"网络请求失败: {exc}") from exc


def _is_success(payload: Dict[str, object]) -> bool:
    """兼容多种成功标识。"""
    status = _get_json_value(payload, "res_code", "resCode", "status", "result")
    if status in {"0", "SUCCESS", "success", "200"}:
        return True
    if payload.get("success") is True:
        return True
    success_text = _get_json_value(payload, "success")
    if success_text.lower() == "true":
        return True
    code = _get_json_value(payload, "code")
    if code in {"0", "200"}:
        return True
    # 部分接口成功时仅返回数据，不含 code。
    if _get_json_value(payload, "shareId", "shareID", "shareid", "userAccount", "name", "nickName"):
        return True
    stack: List[object] = [payload]
    visited_ids = set()
    while stack:
        current = stack.pop()
        obj_id = id(current)
        if obj_id in visited_ids:
            continue
        visited_ids.add(obj_id)
        if isinstance(current, dict):
            for raw_key, raw_value in current.items():
                if str(raw_key).strip().lower() == "filelistao":
                    return True
                if isinstance(raw_value, (dict, list)):
                    stack.append(raw_value)
        elif isinstance(current, list):
            for item in current:
                if isinstance(item, (dict, list)):
                    stack.append(item)
    return False


async def get_user_account(cookie: str) -> Optional[str]:
    """校验登录态并返回账号标识。"""
    cookie = (cookie or "").strip()
    if not cookie:
        return None

    url = f"https://cloud.189.cn/api/portal/v2/getUserBriefInfo.action?noCache={_now_ms()}"
    timeout = aiohttp.ClientTimeout(total=12)
    async with _create_session(timeout=timeout) as session:
        payload = await _json_get(session, url, cookie, allow_redirects=False)
    if not _is_success(payload):
        return None
    account = _get_json_value(payload, "userAccount", "name", "nickName")
    return account or None


async def resolve_share(share_url: str, cookie: str) -> ResolvedShare:
    """解析分享链接，返回分享文件清单。"""
    share_code, pwd = parse_share_url(share_url)
    timeout = aiohttp.ClientTimeout(total=20)
    no_cache = str(_now_ms())
    attempts: List[Dict[str, object]] = []
    referer_url = f"https://cloud.189.cn/t/{share_code}"
    if pwd:
        referer_url = referer_url + "?" + urlencode({"pwd": pwd})

    async with _create_session(timeout=timeout) as session:
        share_id = ""
        root_file_id = ""
        is_folder = False
        info_payload: Dict[str, object] = {}
        check_payload: Dict[str, object] = {}
        last_share_error = ""

        def _apply_info_payload(payload: Dict[str, object]) -> str:
            """把分享信息 payload 提取到上下文。"""
            nonlocal share_id, root_file_id, is_folder, info_payload
            if not isinstance(payload, dict):
                return ""

            if not info_payload:
                info_payload = payload

            payload_share_id = _get_json_value(payload, "shareId", "shareID", "shareid")
            if payload_share_id:
                share_id = payload_share_id

            payload_file_id = _get_json_value(payload, "fileId", "fileID", "fileid")
            if payload_file_id and not root_file_id:
                root_file_id = payload_file_id

            folder_value = _get_json_value(payload, "isFolder")
            if folder_value:
                is_folder = str(folder_value).lower() in {"1", "true"}

            return payload_share_id

        # 0) 先对齐 Gamebox：优先 checkAccessCode，再走 getShareInfoByCodeV2。
        if pwd and not share_id:
            check_params_primary = {"noCache": no_cache, "shareCode": share_code, "accessCode": pwd}
            for profile in _SHARE_CHECK_PROFILES:
                try:
                    payload, meta = await _request_share_profile(
                        session,
                        cookie=cookie,
                        profile=profile,
                        query_params=check_params_primary,
                        form_params=check_params_primary if profile.use_form else None,
                        referer_url=referer_url,
                        allow_redirects=True,
                    )
                    check_payload = payload
                    checked_share_id = _get_json_value(payload, "shareId", "shareID", "shareid")
                    if checked_share_id:
                        share_id = checked_share_id
                        if not root_file_id:
                            root_file_id = checked_share_id
                    detail = _extract_api_error(payload)
                    ok = bool(checked_share_id)
                    _append_attempt(
                        attempts,
                        step="check_access_code_primary",
                        endpoint=profile.endpoint,
                        ok=ok,
                        message=detail or ("未返回shareId" if not ok else ""),
                        share_id=checked_share_id,
                        host=str(meta.get("host", "")),
                        method=str(meta.get("method", "")),
                        profile=str(meta.get("profile", "")),
                        status=_as_optional_int(meta.get("status")),
                        body_type=str(meta.get("body_type", "")),
                        body_preview=str(meta.get("body_preview", "")),
                    )
                    if ok:
                        break
                    last_share_error = detail or "未返回shareId"
                except TianyiApiError as exc:
                    diag = exc.diagnostics if isinstance(exc.diagnostics, dict) else {}
                    _append_attempt(
                        attempts,
                        step="check_access_code_primary",
                        endpoint=str(diag.get("endpoint", profile.endpoint)),
                        ok=False,
                        message=str(exc),
                        host=str(diag.get("host", profile.host)),
                        method=str(diag.get("method", profile.method)),
                        profile=str(diag.get("profile", profile.name)),
                        status=_as_optional_int(diag.get("status")),
                        body_type=str(diag.get("body_type", "")),
                        body_preview=str(diag.get("body_preview", "")),
                    )
                    last_share_error = str(exc)

                if share_id:
                    break

        # 1) 主链路：按请求画像获取 getShareInfoByCodeV2。
        info_param_sets: List[Tuple[str, Dict[str, str]]] = []
        if pwd:
            info_param_sets.append(
                (
                    "info_with_access_code",
                    {"noCache": no_cache, "shareCode": share_code, "accessCode": pwd},
                )
            )
        info_param_sets.append(
            (
                "info_without_access_code",
                {"noCache": no_cache, "shareCode": share_code},
            )
        )

        for step_name, req_params in info_param_sets:
            if share_id:
                break
            for profile in _SHARE_INFO_PROFILES:
                try:
                    payload, meta = await _request_share_profile(
                        session,
                        cookie=cookie,
                        profile=profile,
                        query_params=req_params,
                        form_params=req_params if profile.use_form else None,
                        referer_url=referer_url,
                        allow_redirects=True,
                    )
                    payload_share_id = _apply_info_payload(payload)
                    detail = _extract_api_error(payload)
                    ok = bool(payload_share_id)
                    _append_attempt(
                        attempts,
                        step=step_name,
                        endpoint=profile.endpoint,
                        ok=ok,
                        message=detail or ("未返回shareId" if not ok else ""),
                        share_id=payload_share_id,
                        host=str(meta.get("host", "")),
                        method=str(meta.get("method", "")),
                        profile=str(meta.get("profile", "")),
                        status=_as_optional_int(meta.get("status")),
                        body_type=str(meta.get("body_type", "")),
                        body_preview=str(meta.get("body_preview", "")),
                    )
                    if ok:
                        break
                    last_share_error = detail or "未返回shareId"
                except TianyiApiError as exc:
                    diag = exc.diagnostics if isinstance(exc.diagnostics, dict) else {}
                    _append_attempt(
                        attempts,
                        step=step_name,
                        endpoint=str(diag.get("endpoint", profile.endpoint)),
                        ok=False,
                        message=str(exc),
                        host=str(diag.get("host", profile.host)),
                        method=str(diag.get("method", profile.method)),
                        profile=str(diag.get("profile", profile.name)),
                        status=_as_optional_int(diag.get("status")),
                        body_type=str(diag.get("body_type", "")),
                        body_preview=str(diag.get("body_preview", "")),
                    )
                    last_share_error = str(exc)

                if share_id:
                    break

        # 2) HTML 兜底：从分享落地页提取 shareId。
        if not share_id:
            try:
                html_share_id = await _fetch_share_id_from_share_page(
                    session,
                    share_code=share_code,
                    pwd=pwd,
                    cookie=cookie,
                )
                share_id = html_share_id
                if not root_file_id:
                    root_file_id = html_share_id
                if not info_payload:
                    is_folder = True
                _append_attempt(
                    attempts,
                    step="share_page_html",
                    endpoint=f"/t/{share_code}",
                    ok=True,
                    share_id=html_share_id,
                    host="cloud.189.cn",
                    method="GET",
                    profile="share_page_html",
                )
            except TianyiApiError as exc:
                _append_attempt(
                    attempts,
                    step="share_page_html",
                    endpoint=f"/t/{share_code}",
                    ok=False,
                    message=str(exc),
                    host="cloud.189.cn",
                    method="GET",
                    profile="share_page_html",
                )
                last_share_error = str(exc)

        # 3) checkAccessCode 辅助链路：按请求画像重试。
        if not share_id:
            check_params = {"noCache": no_cache, "shareCode": share_code, "accessCode": pwd}
            for profile in _SHARE_CHECK_PROFILES:
                try:
                    payload, meta = await _request_share_profile(
                        session,
                        cookie=cookie,
                        profile=profile,
                        query_params=check_params,
                        form_params=check_params if profile.use_form else None,
                        referer_url=referer_url,
                        allow_redirects=True,
                    )
                    check_payload = payload
                    checked_share_id = _get_json_value(payload, "shareId", "shareID", "shareid")
                    if checked_share_id:
                        share_id = checked_share_id
                        if not root_file_id:
                            root_file_id = checked_share_id
                    detail = _extract_api_error(payload)
                    ok = bool(checked_share_id)
                    _append_attempt(
                        attempts,
                        step="check_access_code_aux",
                        endpoint=profile.endpoint,
                        ok=ok,
                        message=detail or ("未返回shareId" if not ok else ""),
                        share_id=checked_share_id,
                        host=str(meta.get("host", "")),
                        method=str(meta.get("method", "")),
                        profile=str(meta.get("profile", "")),
                        status=_as_optional_int(meta.get("status")),
                        body_type=str(meta.get("body_type", "")),
                        body_preview=str(meta.get("body_preview", "")),
                    )
                    if ok:
                        break
                    last_share_error = detail or "未返回shareId"
                except TianyiApiError as exc:
                    diag = exc.diagnostics if isinstance(exc.diagnostics, dict) else {}
                    _append_attempt(
                        attempts,
                        step="check_access_code_aux",
                        endpoint=str(diag.get("endpoint", profile.endpoint)),
                        ok=False,
                        message=str(exc),
                        host=str(diag.get("host", profile.host)),
                        method=str(diag.get("method", profile.method)),
                        profile=str(diag.get("profile", profile.name)),
                        status=_as_optional_int(diag.get("status")),
                        body_type=str(diag.get("body_type", "")),
                        body_preview=str(diag.get("body_preview", "")),
                    )
                    last_share_error = str(exc)

        if not share_id:
            js_error = ""
            try:
                js_resolved = await _resolve_share_via_js(share_url, cookie)
                _append_attempt(
                    attempts,
                    step="js_fallback",
                    endpoint="/backend/tianyi_share_resolver.js",
                    ok=True,
                    share_id=js_resolved.share_id,
                    host="local_js_resolver",
                    method="NODE",
                    profile="gamebox_like_js",
                )
                return js_resolved
            except TianyiApiError as exc:
                js_error = str(exc)
                _append_attempt(
                    attempts,
                    step="js_fallback",
                    endpoint="/backend/tianyi_share_resolver.js",
                    ok=False,
                    message=js_error,
                    host="local_js_resolver",
                    method="NODE",
                    profile="gamebox_like_js",
                )

            detail = _extract_api_error(check_payload) or _extract_api_error(info_payload)
            message = "分享解析失败：未获取shareId"
            if detail:
                message = f"{message}（{detail}）"
            elif last_share_error:
                message = f"{message}（{last_share_error}）"
            elif js_error:
                message = f"{message}（JS兜底失败: {js_error}）"
            raise TianyiApiError(
                message,
                diagnostics={
                    "share_code": share_code,
                    "share_url": share_url,
                    "attempts": attempts,
                },
            )

        if not root_file_id:
            root_file_id = share_id
        if not info_payload and share_id:
            is_folder = True

        params = {
            "noCache": str(no_cache),
            "shareId": share_id,
            "shareMode": "1",
            "iconOption": "5",
            "pageNum": "1",
            "pageSize": "60",
        }
        if is_folder:
            params.update(
                {
                    "fileId": share_id or root_file_id,
                    "shareDirFileId": share_id or root_file_id,
                    "isFolder": "true",
                    "orderBy": "lastOpTime",
                    "descending": "true",
                }
            )
        else:
            params.update({"fileId": root_file_id, "isFolder": "false"})

        # 无提取码分享也保留 accessCode 参数，兼容部分接口行为。
        params["accessCode"] = pwd

        async def _request_list_payload(list_params: Dict[str, str], *, step_name: str) -> Dict[str, object]:
            last_error: Optional[TianyiApiError] = None
            last_message = "listShareDir 请求失败"
            for profile in _SHARE_LIST_PROFILES:
                try:
                    payload, meta = await _request_share_profile(
                        session,
                        cookie=cookie,
                        profile=profile,
                        query_params=list_params,
                        form_params=list_params if profile.use_form else None,
                        referer_url=referer_url,
                        allow_redirects=True,
                    )
                    detail = _extract_api_error(payload)
                    ok = _is_success(payload)
                    _append_attempt(
                        attempts,
                        step=step_name,
                        endpoint=profile.endpoint,
                        ok=ok,
                        message=detail or ("响应未标记成功" if not ok else ""),
                        share_id=share_id,
                        host=str(meta.get("host", "")),
                        method=str(meta.get("method", "")),
                        profile=str(meta.get("profile", "")),
                        status=_as_optional_int(meta.get("status")),
                        body_type=str(meta.get("body_type", "")),
                        body_preview=str(meta.get("body_preview", "")),
                    )
                    if ok:
                        return payload
                    last_message = detail or "响应未标记成功"
                except TianyiApiError as exc:
                    diag = exc.diagnostics if isinstance(exc.diagnostics, dict) else {}
                    _append_attempt(
                        attempts,
                        step=step_name,
                        endpoint=str(diag.get("endpoint", profile.endpoint)),
                        ok=False,
                        message=str(exc),
                        share_id=share_id,
                        host=str(diag.get("host", profile.host)),
                        method=str(diag.get("method", profile.method)),
                        profile=str(diag.get("profile", profile.name)),
                        status=_as_optional_int(diag.get("status")),
                        body_type=str(diag.get("body_type", "")),
                        body_preview=str(diag.get("body_preview", "")),
                    )
                    last_error = exc
                    last_message = str(exc)

            if last_error is not None:
                raise TianyiApiError(last_message, diagnostics={"attempts": attempts}) from last_error
            raise TianyiApiError(last_message, diagnostics={"attempts": attempts})

        js_list_error = ""
        try:
            list_payload = await _request_list_payload(params, step_name="list_share_dir")
        except TianyiApiError as first_error:
            # 文件夹场景下，部分分享需要使用 root_file_id 作为目录参数重试。
            if is_folder and root_file_id and root_file_id != params.get("fileId"):
                retry_params = dict(params)
                retry_params["fileId"] = root_file_id
                retry_params["shareDirFileId"] = root_file_id
                try:
                    list_payload = await _request_list_payload(retry_params, step_name="list_share_dir_retry_root")
                except TianyiApiError as second_error:
                    try:
                        js_resolved = await _resolve_share_via_js(share_url, cookie)
                        _append_attempt(
                            attempts,
                            step="js_fallback_on_list_error",
                            endpoint="/backend/tianyi_share_resolver.js",
                            ok=True,
                            share_id=js_resolved.share_id,
                            host="local_js_resolver",
                            method="NODE",
                            profile="gamebox_like_js",
                            message=f"python_list_failed: {second_error}",
                        )
                        return js_resolved
                    except TianyiApiError as js_exc:
                        js_list_error = str(js_exc)
                        _append_attempt(
                            attempts,
                            step="js_fallback_on_list_error",
                            endpoint="/backend/tianyi_share_resolver.js",
                            ok=False,
                            host="local_js_resolver",
                            method="NODE",
                            profile="gamebox_like_js",
                            message=js_list_error,
                        )

                    if not pwd:
                        raise TianyiApiError(
                            "解析失败：该分享可能需要提取码，请补充 ?pwd= 后重试"
                            + f"（{first_error}; {second_error}"
                            + (f"; JS兜底失败: {js_list_error}" if js_list_error else "")
                            + "）",
                            diagnostics={"share_code": share_code, "share_url": share_url, "attempts": attempts},
                        ) from second_error
                    raise TianyiApiError(
                        str(second_error) + (f"（JS兜底失败: {js_list_error}）" if js_list_error else ""),
                        diagnostics={"share_code": share_code, "share_url": share_url, "attempts": attempts},
                    ) from second_error
            else:
                try:
                    js_resolved = await _resolve_share_via_js(share_url, cookie)
                    _append_attempt(
                        attempts,
                        step="js_fallback_on_list_error",
                        endpoint="/backend/tianyi_share_resolver.js",
                        ok=True,
                        share_id=js_resolved.share_id,
                        host="local_js_resolver",
                        method="NODE",
                        profile="gamebox_like_js",
                        message=f"python_list_failed: {first_error}",
                    )
                    return js_resolved
                except TianyiApiError as js_exc:
                    js_list_error = str(js_exc)
                    _append_attempt(
                        attempts,
                        step="js_fallback_on_list_error",
                        endpoint="/backend/tianyi_share_resolver.js",
                        ok=False,
                        host="local_js_resolver",
                        method="NODE",
                        profile="gamebox_like_js",
                        message=js_list_error,
                    )

                if not pwd:
                    raise TianyiApiError(
                        "解析失败：该分享可能需要提取码，请补充 ?pwd= 后重试"
                        + f"（{first_error}"
                        + (f"; JS兜底失败: {js_list_error}" if js_list_error else "")
                        + "）",
                        diagnostics={"share_code": share_code, "share_url": share_url, "attempts": attempts},
                    ) from first_error
                raise TianyiApiError(
                    str(first_error) + (f"（JS兜底失败: {js_list_error}）" if js_list_error else ""),
                    diagnostics={"share_code": share_code, "share_url": share_url, "attempts": attempts},
                ) from first_error

        file_list_ao = _find_nested_value(list_payload, "fileListAO")
        rows: object = file_list_ao.get("fileList") if isinstance(file_list_ao, dict) else None
        if not isinstance(rows, list):
            rows = _find_nested_value(list_payload, "fileList", "files", "rows", "list")
        files: List[ResolvedFile] = []
        if isinstance(rows, list):
            for row in rows:
                if not isinstance(row, dict):
                    continue
                file_id = _get_json_value(row, "id", "fileId")
                if not file_id:
                    continue
                name = _get_json_value(row, "name", "fileName") or f"file-{file_id}"
                size = _parse_int(row.get("size", row.get("fileSize", 0)), 0)
                folder = str(row.get("isFolder", "")).lower() in {"1", "true"}
                files.append(ResolvedFile(file_id=file_id, name=name, size=size, is_folder=folder))

        if not files and root_file_id:
            files.append(
                ResolvedFile(
                    file_id=root_file_id,
                    name=_get_json_value(info_payload, "name", "fileName") or "single-file",
                    size=_parse_int(info_payload.get("size", info_payload.get("fileSize", 0)), 0),
                    is_folder=False,
                )
            )

    return ResolvedShare(share_code=share_code, share_id=share_id, pwd=pwd, files=files)


async def fetch_access_token(cookie: str) -> str:
    """通过 ssoH5 跳转链获取 accessToken。"""
    timeout = aiohttp.ClientTimeout(total=20)
    current = "https://api.cloud.189.cn/open/oauth2/ssoH5.action"

    async with _create_session(timeout=timeout) as session:
        for _ in range(12):
            try:
                async with session.get(
                    current,
                    headers=_headers(cookie),
                    allow_redirects=False,
                ) as resp:
                    final_url = str(resp.url)
                    token = _extract_access_token(final_url)
                    if token:
                        return token
                    if resp.status in {301, 302, 303, 307, 308}:
                        location = resp.headers.get("Location", "").strip()
                        if not location:
                            raise TianyiApiError("ssoH5 跳转缺少 Location")
                        current = urljoin(final_url, location)
                        token = _extract_access_token(current)
                        if token:
                            return token
                        continue
                    break
            except aiohttp.ClientConnectorCertificateError as exc:
                raise TianyiApiError(f"TLS证书校验失败: {exc}") from exc
            except aiohttp.ClientSSLError as exc:
                raise TianyiApiError(f"TLS连接失败: {exc}") from exc
            except aiohttp.ClientError as exc:
                raise TianyiApiError(f"网络请求失败: {exc}") from exc

    raise TianyiApiError("获取 accessToken 失败，请重新登录后重试")


def _extract_access_token(url: str) -> str:
    """从 URL 查询参数提取 accessToken。"""
    parsed = urlparse(url)
    return (parse_qs(parsed.query or "").get("accessToken", [""])[0] or "").strip()


async def fetch_download_url(
    cookie: str,
    access_token: str,
    share_id: str,
    file_id: str,
) -> str:
    """获取官方文件下载直链。"""
    timestamp = str(_now_ms())
    sign_source = (
        f"AccessToken={access_token}&Timestamp={timestamp}&dt=1&fileId={file_id}&shareId={share_id}"
    )
    signature = hashlib.md5(sign_source.encode("utf-8")).hexdigest()
    url = (
        "https://api.cloud.189.cn/open/file/getFileDownloadUrl.action?"
        + urlencode({"fileId": file_id, "dt": "1", "shareId": share_id})
    )

    timeout = aiohttp.ClientTimeout(total=20)
    headers = _headers(cookie)
    headers.update(
        {
            "Accesstoken": access_token,
            "Signature": signature,
            "Timestamp": timestamp,
            "Sign-Type": "1",
            "Accept": "application/json;charset=UTF-8",
        }
    )

    async with _create_session(timeout=timeout) as session:
        try:
            async with session.get(url, headers=headers, allow_redirects=False) as resp:
                if resp.status >= 400:
                    text = (await resp.text())[:280]
                    raise TianyiApiError(f"直链请求失败 status={resp.status} body={text}")
                try:
                    payload = await resp.json(content_type=None)
                except Exception as exc:
                    text = (await resp.text())[:280]
                    raise TianyiApiError(f"直链响应解析失败: {exc}; body={text}") from exc
        except aiohttp.ClientConnectorCertificateError as exc:
            raise TianyiApiError(f"TLS证书校验失败: {exc}") from exc
        except aiohttp.ClientSSLError as exc:
            raise TianyiApiError(f"TLS连接失败: {exc}") from exc
        except aiohttp.ClientError as exc:
            raise TianyiApiError(f"网络请求失败: {exc}") from exc

    if not isinstance(payload, dict):
        raise TianyiApiError("直链响应格式异常")

    direct_url = _get_json_value(payload, "fileDownloadUrl", "downloadUrl", "url")
    if not direct_url:
        # 一些场景字段嵌套在 data 节点。
        data = payload.get("data")
        if isinstance(data, dict):
            direct_url = _get_json_value(data, "fileDownloadUrl", "downloadUrl", "url")

    if not direct_url:
        raise TianyiApiError("未获取到可用直链，请检查登录态或分享权限")
    return direct_url


def _get_js_cloud_upload_path() -> str:
    """定位 JS 云上传脚本路径。"""
    candidates: List[Path] = []

    plugin_dir = str(os.environ.get("DECKY_PLUGIN_DIR", "") or "").strip()
    if plugin_dir:
        for relative_path in _JS_CLOUD_UPLOAD_RELATIVE_PATHS:
            candidates.append(Path(plugin_dir, *relative_path))

    base_dir = Path(__file__).resolve().parents[1]
    for relative_path in _JS_CLOUD_UPLOAD_RELATIVE_PATHS:
        candidates.append(base_dir.joinpath(*relative_path))

    for candidate in candidates:
        try:
            if candidate.is_file():
                return str(candidate)
        except Exception:
            continue
    return ""


async def fetch_session_key(cookie: str, access_token: str = "") -> str:
    """获取 sessionKey。

    优先走网页端同款接口：
    - GET /api/portal/v2/getUserBriefInfo.action
    若该链路未返回 sessionKey，再回退到 getSessionForPC。
    """

    def _extract_session_key(payload: Dict[str, object]) -> str:
        session_key = _get_json_value(payload, "sessionKey", "session_key")
        if not session_key:
            nested = _find_nested_value(payload, "sessionKey", "session_key")
            if nested is not None:
                session_key = str(nested or "").strip()
        return session_key

    attempts: List[Dict[str, object]] = []
    user_brief_url = f"https://cloud.189.cn/api/portal/v2/getUserBriefInfo.action?noCache={_now_ms()}"

    # 1) 官方网页同款：直接从 getUserBriefInfo 返回体取 sessionKey。
    try:
        timeout = aiohttp.ClientTimeout(total=15)
        async with _create_session(timeout=timeout) as session:
            payload = await _json_get(session, user_brief_url, cookie, allow_redirects=False)
        if not _is_success(payload):
            message = _extract_api_error(payload) or _short_text(json.dumps(payload, ensure_ascii=False), 320)
            raise TianyiApiError(f"getUserBriefInfo 校验失败: {message}")
        session_key = _extract_session_key(payload)
        if session_key:
            return session_key
        message = _extract_api_error(payload) or _short_text(json.dumps(payload, ensure_ascii=False), 320)
        raise TianyiApiError(f"getUserBriefInfo 未返回 sessionKey: {message}")
    except TianyiApiError as exc:
        attempts.append(
            {
                "source": "portal_v2_getUserBriefInfo",
                "ok": False,
                "message": str(exc),
            }
        )

    # 2) 回退链路：兼容旧流程与参数组合差异。
    token = str(access_token or "").strip()
    if not token:
        try:
            token = await fetch_access_token(cookie)
        except Exception as exc:
            attempts.append(
                {
                    "source": "fetch_access_token",
                    "ok": False,
                    "message": str(exc),
                }
            )
            raise TianyiApiError("获取 sessionKey 失败：accessToken 获取失败", diagnostics={"attempts": attempts}) from exc

    request_profiles: Tuple[Dict[str, str], ...] = (
        {
            "name": "getSessionForPC_app600100422",
            "appId": "600100422",
            "clientType": "TELEPC",
            "version": "6.2",
            "channelId": "web_cloud.189.cn",
        },
        {
            "name": "getSessionForPC_app8025431004",
            "appId": "8025431004",
            "clientType": "TELEPC",
            "version": "6.2",
            "channelId": "web_cloud.189.cn",
        },
    )
    methods: Tuple[str, ...] = ("GET", "POST")
    url = "https://api.cloud.189.cn/getSessionForPC.action"

    timeout = aiohttp.ClientTimeout(total=20)
    try:
        async with _create_session(timeout=timeout) as session:
            for profile in request_profiles:
                for method in methods:
                    params = {
                        "appId": profile["appId"],
                        "clientType": profile["clientType"],
                        "version": profile["version"],
                        "channelId": profile["channelId"],
                        "rand": str(_now_ms()),
                        "accessToken": token,
                    }
                    try:
                        if method == "GET":
                            async with session.get(
                                url,
                                params=params,
                                headers=_headers(cookie),
                                allow_redirects=True,
                            ) as resp:
                                text = await resp.text()
                        else:
                            async with session.post(
                                url,
                                params=params,
                                headers=_headers(cookie),
                                allow_redirects=True,
                            ) as resp:
                                text = await resp.text()
                        status = int(resp.status)
                    except aiohttp.ClientConnectorCertificateError as exc:
                        attempts.append(
                            {
                                "source": profile["name"],
                                "method": method,
                                "ok": False,
                                "message": f"TLS证书校验失败: {exc}",
                            }
                        )
                        continue
                    except aiohttp.ClientSSLError as exc:
                        attempts.append(
                            {
                                "source": profile["name"],
                                "method": method,
                                "ok": False,
                                "message": f"TLS连接失败: {exc}",
                            }
                        )
                        continue
                    except aiohttp.ClientError as exc:
                        attempts.append(
                            {
                                "source": profile["name"],
                                "method": method,
                                "ok": False,
                                "message": f"网络请求失败: {exc}",
                            }
                        )
                        continue

                    if status >= 400:
                        attempts.append(
                            {
                                "source": profile["name"],
                                "method": method,
                                "ok": False,
                                "status": status,
                                "message": _short_text(text, 320),
                            }
                        )
                        continue

                    payload = _normalize_json_payload(text)
                    if not isinstance(payload, dict):
                        attempts.append(
                            {
                                "source": profile["name"],
                                "method": method,
                                "ok": False,
                                "status": status,
                                "message": "响应格式异常",
                            }
                        )
                        continue

                    session_key = _extract_session_key(payload)
                    if session_key:
                        return session_key

                    message = _extract_api_error(payload) or _short_text(json.dumps(payload, ensure_ascii=False), 320)
                    attempts.append(
                        {
                            "source": profile["name"],
                            "method": method,
                            "ok": False,
                            "status": status,
                            "message": message,
                        }
                    )
    except aiohttp.ClientConnectorCertificateError as exc:
        raise TianyiApiError(f"TLS证书校验失败: {exc}", diagnostics={"attempts": attempts}) from exc
    except aiohttp.ClientSSLError as exc:
        raise TianyiApiError(f"TLS连接失败: {exc}", diagnostics={"attempts": attempts}) from exc
    except aiohttp.ClientError as exc:
        raise TianyiApiError(f"网络请求失败: {exc}", diagnostics={"attempts": attempts}) from exc

    raise TianyiApiError("未获取到 sessionKey，请重新登录后重试", diagnostics={"attempts": attempts})


async def _invoke_cloud_helper_via_js(
    *,
    action: str,
    payload: Dict[str, object],
    timeout_seconds: float = 360.0,
) -> Dict[str, object]:
    """调用 JS 云盘脚本执行指定动作。"""
    script_path = _get_js_cloud_upload_path()
    if not script_path:
        raise TianyiApiError("JS 云上传脚本不存在")

    normalized_payload: Dict[str, object] = {
        "action": str(action or "").strip().lower() or "upload",
    }
    for key, value in dict(payload or {}).items():
        normalized_payload[str(key)] = value

    raw_payload = json.dumps(normalized_payload, ensure_ascii=False).encode("utf-8")

    def _build_node_env() -> Dict[str, str]:
        env: Dict[str, str] = {}
        for key, value in os.environ.items():
            k = str(key or "").strip()
            if not k:
                continue
            env[k] = str(value or "")

        ld_orig = str(env.get("LD_LIBRARY_PATH_ORIG", "") or "").strip()
        if ld_orig:
            env["LD_LIBRARY_PATH"] = ld_orig
        else:
            env.pop("LD_LIBRARY_PATH", None)

        env.pop("PYTHONHOME", None)
        env.pop("PYTHONPATH", None)
        env.pop("_MEIPASS2", None)

        if not str(env.get("PATH", "")).strip():
            env["PATH"] = "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
        return env

    node_env = _build_node_env()

    def _run_node() -> subprocess.CompletedProcess[bytes]:
        return subprocess.run(
            ["node", script_path],
            input=raw_payload,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=node_env,
            check=False,
            timeout=max(30.0, float(timeout_seconds or 360.0)),
        )

    try:
        completed = await asyncio.to_thread(_run_node)
    except FileNotFoundError as exc:
        raise TianyiApiError("系统缺少 node 运行时，无法执行云上传") from exc
    except subprocess.TimeoutExpired as exc:
        raise TianyiApiError("云上传执行超时") from exc
    except Exception as exc:
        raise TianyiApiError(f"启动云上传脚本失败: {exc}") from exc

    raw_out = bytes(completed.stdout or b"").decode("utf-8", errors="ignore").strip()
    raw_err = bytes(completed.stderr or b"").decode("utf-8", errors="ignore").strip()

    if int(completed.returncode or 0) != 0 and not raw_out:
        raise TianyiApiError(
            f"云上传脚本执行失败: script={script_path} err={raw_err or f'code={completed.returncode}'}"
        )

    if not raw_out:
        raise TianyiApiError(f"云上传脚本无输出: script={script_path} err={raw_err or 'empty_stdout'}")

    try:
        result = json.loads(raw_out)
    except Exception as exc:
        raise TianyiApiError(
            f"云上传脚本输出非 JSON: script={script_path} {exc}; out={_short_text(raw_out)}"
        ) from exc

    if not isinstance(result, dict):
        raise TianyiApiError("云上传脚本输出结构异常")

    if not bool(result.get("ok", False)):
        error_message = str(result.get("error", "") or "云上传失败").strip()
        diagnostics = result.get("diagnostics")
        if isinstance(diagnostics, dict) and diagnostics:
            raise TianyiApiError(error_message, diagnostics=diagnostics)
        raise TianyiApiError(error_message)

    data = result.get("data")
    if not isinstance(data, dict):
        raise TianyiApiError("云上传脚本缺少 data")
    return data


async def _upload_archive_via_js(
    *,
    cookie: str,
    access_token: str,
    session_key: str,
    local_file_path: str,
    remote_folder_parts: Sequence[str],
    remote_name: str,
) -> Dict[str, object]:
    """调用 JS 上传脚本执行目录确保与分片上传。"""
    return await _invoke_cloud_helper_via_js(
        action="upload",
        payload={
            "cookie": str(cookie or "").strip(),
            "access_token": str(access_token or "").strip(),
            "session_key": str(session_key or "").strip(),
            "local_file_path": str(local_file_path or "").strip(),
            "remote_folder_parts": [
                str(item or "").strip()
                for item in list(remote_folder_parts or [])
                if str(item or "").strip()
            ],
            "remote_name": str(remote_name or "").strip(),
        },
        timeout_seconds=360.0,
    )


async def upload_archive_to_cloud(
    *,
    cookie: str,
    local_file_path: str,
    remote_folder_parts: Sequence[str],
    remote_name: str,
) -> Dict[str, object]:
    """上传本地压缩包到天翼云盘指定目录。"""
    normalized_cookie = str(cookie or "").strip()
    if not normalized_cookie:
        raise TianyiApiError("未登录，缺少 cookie")

    local_path = os.path.realpath(os.path.expanduser(str(local_file_path or "").strip()))
    if not local_path or not os.path.isfile(local_path):
        raise TianyiApiError("待上传压缩包不存在")

    remote_file_name = str(remote_name or "").strip() or os.path.basename(local_path)
    remote_parts = [str(item or "").strip() for item in list(remote_folder_parts or []) if str(item or "").strip()]

    access_token = await fetch_access_token(normalized_cookie)
    session_key = await fetch_session_key(normalized_cookie, access_token)

    data = await _upload_archive_via_js(
        cookie=normalized_cookie,
        access_token=access_token,
        session_key=session_key,
        local_file_path=local_path,
        remote_folder_parts=remote_parts,
        remote_name=remote_file_name,
    )

    result: Dict[str, object] = dict(data)
    result.setdefault("remote_name", remote_file_name)
    result.setdefault("remote_folder_parts", remote_parts)
    return result


async def list_cloud_archives(
    *,
    cookie: str,
    remote_folder_parts: Sequence[str],
) -> Dict[str, object]:
    """列出云端目录下的存档版本文件。"""
    normalized_cookie = str(cookie or "").strip()
    if not normalized_cookie:
        raise TianyiApiError("未登录，缺少 cookie")

    remote_parts = [
        str(item or "").strip()
        for item in list(remote_folder_parts or [])
        if str(item or "").strip()
    ]

    access_token = await fetch_access_token(normalized_cookie)
    session_key = await fetch_session_key(normalized_cookie, access_token)

    data = await _invoke_cloud_helper_via_js(
        action="list_versions",
        payload={
            "cookie": normalized_cookie,
            "access_token": access_token,
            "session_key": session_key,
            "remote_folder_parts": remote_parts,
        },
        timeout_seconds=180.0,
    )

    result: Dict[str, object] = dict(data)
    files_raw = result.get("files")
    if not isinstance(files_raw, list):
        files_raw = []
    files: List[Dict[str, object]] = []
    for item in files_raw:
        if not isinstance(item, dict):
            continue
        files.append(
            {
                "file_id": str(item.get("file_id", "") or ""),
                "name": str(item.get("name", "") or ""),
                "size": max(0, int(item.get("size", 0) or 0)),
                "last_op_time": str(item.get("last_op_time", "") or ""),
            }
        )
    result["files"] = files
    result["remote_folder_parts"] = remote_parts
    return result


async def download_cloud_archive(
    *,
    cookie: str,
    file_id: str,
    local_file_path: str,
) -> Dict[str, object]:
    """按 file_id 下载云端文件到本地路径。"""
    normalized_cookie = str(cookie or "").strip()
    normalized_file_id = str(file_id or "").strip()
    target_path = os.path.realpath(os.path.expanduser(str(local_file_path or "").strip()))

    if not normalized_cookie:
        raise TianyiApiError("未登录，缺少 cookie")
    if not normalized_file_id:
        raise TianyiApiError("缺少 file_id")
    if not target_path:
        raise TianyiApiError("下载路径无效")

    access_token = await fetch_access_token(normalized_cookie)
    session_key = await fetch_session_key(normalized_cookie, access_token)

    data = await _invoke_cloud_helper_via_js(
        action="download_file",
        payload={
            "cookie": normalized_cookie,
            "access_token": access_token,
            "session_key": session_key,
            "file_id": normalized_file_id,
            "local_file_path": target_path,
        },
        timeout_seconds=600.0,
    )

    result: Dict[str, object] = dict(data)
    result["file_id"] = normalized_file_id
    result["local_file_path"] = str(result.get("local_file_path", "") or target_path)
    result["file_size"] = max(0, int(result.get("file_size", 0) or 0))
    return result
