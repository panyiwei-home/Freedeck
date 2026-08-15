"""天翼云盘 API 客户端 — 分享链接解析与直链获取。

API 端点（参考 freedeck-plugin，去屎山精简版）：

  分享信息:  api.cloud.189.cn /open/share/getShareInfoByCodeV2.action
  密码校验:  api.cloud.189.cn /open/share/checkAccessCode.action
  目录列表:  api.cloud.189.cn /open/share/listShareDir.action
  直链获取:  api.cloud.189.cn /open/file/getFileDownloadUrl.action
"""

from __future__ import annotations

import re
import time
import json as _json
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode, urlparse

import aiohttp

# ── 常量 ──────────────────────────────────

_SHARE_CODE_RE = re.compile(r"^[A-Za-z0-9_-]{4,}$")
_HOST = "api.cloud.189.cn"
_UA = (
    "Mozilla/5.0 (X11; Linux x86_64; Valve Steam Gamepad) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.6478.183 Safari/537.36"
)

# ── 数据模型 ──────────────────────────────

@dataclass
class ShareFile:
    file_id: str = ""
    name: str = ""
    size_bytes: int = 0
    is_folder: bool = False


@dataclass
class ResolvedShare:
    share_id: str = ""
    files: List[ShareFile] = field(default_factory=list)
    raw: Dict[str, Any] = field(default_factory=dict)


class TianyiError(RuntimeError):
    """天翼云 API 错误。"""


# ── 工具函数 ──────────────────────────────

def _now_ms() -> str:
    return str(int(time.time() * 1000))


def _get_str(obj: dict, *keys: str) -> str:
    for k in keys:
        v = obj.get(k)
        if v:
            return str(v).strip()
    return ""


def _get_int(obj: dict, *keys: str) -> int:
    for k in keys:
        v = obj.get(k)
        if v is not None:
            try:
                return int(v)
            except (TypeError, ValueError):
                pass
    return 0


# ── API 请求 ──────────────────────────────

async def _api_post(endpoint: str, params: dict, cookie: str = "",
                    referer: str = "") -> dict:
    """调用天翼云 API，返回 JSON 对象。"""
    url = f"https://{_HOST}{endpoint}"
    headers = {
        "User-Agent": _UA,
        "Accept": "application/json, text/plain, */*",
        "Referer": referer or "https://cloud.189.cn/",
    }
    if cookie:
        headers["Cookie"] = cookie

    timeout = aiohttp.ClientTimeout(total=20)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.post(url, data=params, headers=headers) as resp:
            text = await resp.text()
            if resp.status >= 400:
                raise TianyiError(f"HTTP {resp.status}: {text[:200]}")
            try:
                data = _json.loads(text)
            except Exception as e:
                raise TianyiError(f"JSON 解析失败: {e}") from e
            if not isinstance(data, dict):
                raise TianyiError(f"API 返回非对象")

            # 检查业务错误
            msg = _get_str(data, "msg", "message", "errorMsg")
            code = _get_str(data, "code", "errorCode", "resultCode")
            if code and code != "0" and code != "200":
                raise TianyiError(f"API 错误 code={code}: {msg}")
            return data


# ── 公开 API ──────────────────────────────

def parse_share_url(share_url: str) -> Tuple[str, str]:
    """解析分享链接，返回 (share_code, pwd)。"""
    raw = (share_url or "").strip()
    if not raw:
        raise TianyiError("分享链接为空")

    # 纯分享码
    if _SHARE_CODE_RE.fullmatch(raw):
        return raw, ""

    # 补 scheme
    if raw.startswith("cloud.189.cn/") or raw.startswith("m.cloud.189.cn/"):
        raw = "https://" + raw

    parsed = urlparse(raw)
    host = (parsed.netloc or "").lower()
    if host and "cloud.189.cn" not in host:
        raise TianyiError("仅支持 cloud.189.cn 分享链接")

    # 从路径提取 share_code: /t/XXXXX 或 /share/XXXXX 或 /web/share?code=XXXXX
    m = re.search(r"/(?:t|share)/([A-Za-z0-9_-]{4,})", parsed.path)
    # 兼容 /web/share?code=XXXXX 格式 — code 在 query 而非 path
    if not m and "/web/share" in parsed.path:
        pass  # code 在 query 中，下面统一处理
    if not m:
        m = re.search(r"(?:shareCode|sharecode|share_code|code)=([A-Za-z0-9_-]{4,})", parsed.query or "")
    if not m:
        raise TianyiError("无法从链接中提取分享码")

    share_code = m.group(1) if m else ""

    # 提取密码
    from urllib.parse import parse_qs
    qs = parse_qs(parsed.query or "")
    pwd = qs.get("pwd", qs.get("accessCode", [""]))[0] or ""

    if not share_code:
        # /web/share 路径 + code 在 query
        share_code = qs.get("code", qs.get("shareCode", qs.get("sharecode", [""])))[0] or ""
        if not share_code:
            raise TianyiError("无法从链接中提取分享码")

    return share_code, pwd


async def resolve_share(share_url: str, cookie: str = "") -> ResolvedShare:
    """解析分享，获取文件列表。"""
    share_code, pwd = parse_share_url(share_url)
    # referer 格式与原始链接保持一致
    if "/web/share" in share_url:
        referer = f"https://cloud.189.cn/web/share?code={share_code}"
        if pwd:
            referer += "&" + urlencode({"pwd": pwd})
    else:
        referer = f"https://cloud.189.cn/t/{share_code}"
        if pwd:
            referer += "?" + urlencode({"pwd": pwd})

    params: Dict[str, str] = {
        "shareCode": share_code,
        "noCache": _now_ms(),
    }
    if pwd:
        params["accessCode"] = pwd

    # 先尝试带 accessCode 的 checkAccessCode
    if pwd:
        try:
            data = await _api_post(
                "/open/share/checkAccessCode.action",
                params, cookie=cookie, referer=referer,
            )
            if _get_str(data, "shareId", "shareID"):
                return _parse_share_response(data, share_code)
        except TianyiError:
            pass

    # getShareInfoByCodeV2
    info_params = {"shareCode": share_code, "noCache": _now_ms()}
    try:
        data = await _api_post(
            "/open/share/getShareInfoByCodeV2.action",
            info_params, cookie=cookie, referer=referer,
        )
    except TianyiError:
        # 回退不带 V2
        data = await _api_post(
            "/open/share/getShareInfoByCode.action",
            info_params, cookie=cookie, referer=referer,
        )

    return _parse_share_response(data, share_code)


async def fetch_download_url(share_code: str, share_id: str, file_id: str,
                             cookie: str = "") -> str:
    """获取文件直链。"""
    referer = f"https://cloud.189.cn/t/{share_code}"
    params = {
        "shareId": share_id,
        "fileId": file_id,
        "noCache": _now_ms(),
    }
    data = await _api_post(
        "/open/file/getFileDownloadUrl.action",
        params, cookie=cookie, referer=referer,
    )
    url = _get_str(data, "fileDownloadUrl", "downloadUrl", "url")
    if not url and isinstance(data.get("data"), dict):
        url = _get_str(data["data"], "fileDownloadUrl", "downloadUrl", "url")
    if not url:
        raise TianyiError("未获取到下载直链")
    return url


def _parse_share_response(data: dict, share_code: str) -> ResolvedShare:
    """从 API 响应中提取文件列表。"""
    share_id = _get_str(data, "shareId", "shareID")
    if not share_id:
        raise TianyiError("未获取到 shareId，分享可能已过期")

    files: List[ShareFile] = []

    # 尝试从 fileList/files 中提取
    file_list = data.get("fileList") or data.get("files") or data.get("data", {}).get("fileList") or []
    if isinstance(file_list, list):
        for f in file_list:
            if not isinstance(f, dict):
                continue
            fid = _get_str(f, "fileId", "fileID", "id")
            if not fid:
                continue
            files.append(ShareFile(
                file_id=fid,
                name=_get_str(f, "fileName", "name", "title") or fid,
                size_bytes=_get_int(f, "fileSize", "size", "fileSizeByte"),
                is_folder=_get_str(f, "isFolder").lower() in ("1", "true"),
            ))

    # 如果 fileList 为空但 data 里只有单个文件
    if not files and "fileId" in data:
        fid = _get_str(data, "fileId", "fileID")
        if fid:
            files.append(ShareFile(
                file_id=fid,
                name=_get_str(data, "fileName", "name", "title") or fid,
                size_bytes=_get_int(data, "fileSize", "size", "fileSizeByte"),
            ))

    # 尝试 listShareDir 递归获取子目录文件
    if files and any(f.is_folder for f in files):
        # 对于文件夹分享，用 listShareDir 展开
        pass  # TODO: 递归展开文件夹

    return ResolvedShare(share_id=share_id, files=files, raw=data)
