"""天翼云盘 —— 分享解析器。

核心接口:
  - parse_share_url(url) -> (share_code, access_code)
  - resolve_share(url, credential) -> ShareInfo
"""

from __future__ import annotations

import re
from urllib.parse import urlencode

import aiohttp

from ..models import CloudCredential, CloudProviderId, ShareFile, ShareInfo
from ..errors import ShareNotFoundError, ShareAccessDeniedError
from .api import create_session, json_post, json_get, retry_with_backoff, now_ms

# ── URL 解析 ──────────────────────────────

_SHARE_URL_PATTERNS = [
    # https://cloud.189.cn/t/abc123 (提取码在 query)
    re.compile(r"cloud\.189\.cn/t/(?P<code>[A-Za-z0-9_-]{4,})", re.I),
    # https://cloud.189.cn/web/share?code=abc123
    re.compile(r"[?&]code=(?P<code>[A-Za-z0-9_-]{4,})", re.I),
]

_PWD_PARAM_RE = re.compile(r"[?&](?:pwd|password|accessCode)=([^&]+)", re.I)


def parse_share_url(url: str) -> tuple[str, str]:
    """从分享 URL 提取 (share_code, access_code)。

    支持格式:
      - https://cloud.189.cn/t/xxx?pwd=yyy
      - https://cloud.189.cn/web/share?code=xxx
      - https://cloud.189.cn/t/xxx (提取码 unknown，需用户输入)
    """
    url = str(url or "").strip()
    if not url:
        return "", ""

    share_code = ""
    for pattern in _SHARE_URL_PATTERNS:
        m = pattern.search(url)
        if m:
            share_code = str(m.group("code") or "").strip()
            break

    access_code = ""
    m = _PWD_PARAM_RE.search(url)
    if m:
        access_code = m.group(1)

    return share_code, access_code


# ── 分享解析 ──────────────────────────────

async def resolve_share(
    url: str,
    credential: CloudCredential,
    *,
    access_code: str = "",
    root_dir_id: str = "",
) -> ShareInfo:
    """解析分享链接，返回文件列表。

    流程:
      1. parse_share_url() 提取 shareCode
      2. 表单请求 getShareInfoByCodeV2.action → shareId + 根目录 fileId
      3. 请求 listShareDir.action → 文件列表
      4. 递归展开文件夹 (最多 2 层)
      5. 组装 ShareInfo 返回

    Raises:
        ShareNotFoundError:   分享不存在
        ShareAccessDeniedError: 提取码错误
    """
    share_code, url_pwd = parse_share_url(url)
    if not share_code:
        raise ShareNotFoundError("无法从 URL 提取分享码")

    pwd = access_code or url_pwd

    async with create_session() as session:

        # Step 1: 获取分享信息
        share_id, root_file_id, title, direct_file = await _get_share_info(
            session, credential.cookie, share_code, pwd
        )

        # Step 2: 列出文件（优先用 data 中的直接文件信息）
        if direct_file:
            files = [
                ShareFile(
                    file_id=direct_file["fileId"],
                    name=direct_file["fileName"],
                    size_bytes=direct_file["fileSize"],
                    is_directory=direct_file.get("isFolder", False),
                )
            ]
        else:
            files = await _list_files(
                session,
                credential.cookie,
                share_id,
                share_code=share_code,
                access_code=pwd,
                folder_id=root_dir_id or root_file_id,
            )

    return ShareInfo(
        provider=CloudProviderId.TIANYI,
        share_code=share_code,
        share_id=share_id,
        access_code=pwd,
        title=title,
        files=files,
    )


async def list_share_files(
    share_id: str,
    credential: CloudCredential,
    *,
    folder_id: str = "",
) -> list[ShareFile]:
    """列出分享内的直接文件（供文件夹递归使用）。"""
    async with create_session() as session:
        return await _list_files(session, credential.cookie, share_id, folder_id=folder_id)


# ── 内部 ──────────────────────────────────

async def _get_share_info(
    session: aiohttp.ClientSession,
    cookie: str,
    share_code: str,
    pwd: str,
) -> tuple[str, str, str]:
    """获取 shareId、根目录 fileId 和标题。"""
    check_url = "https://api.cloud.189.cn/open/share/checkAccessCode.action"
    url = "https://api.cloud.189.cn/open/share/getShareInfoByCodeV2.action"

    share_id = ""
    if pwd:
        check_body = {
            "shareCode": share_code,
            "accessCode": pwd,
            "noCache": str(now_ms()),
        }

        async def _check_req():
            return await json_post(session, check_url, cookie, data=check_body, as_form=True)

        try:
            check_payload = await retry_with_backoff(_check_req, max_retries=2)
            if isinstance(check_payload, dict):
                check_data = check_payload.get("data", check_payload)
                if isinstance(check_data, dict):
                    share_id = str(check_data.get("shareId", "") or "").strip()
        except aiohttp.ClientResponseError as exc:
            if exc.status in (401, 403):
                raise ShareAccessDeniedError(f"提取码错误或无权访问 (HTTP {exc.status})")
            raise ShareNotFoundError(f"提取码校验失败: {exc}")

    body: dict = {"shareCode": share_code}
    if pwd:
        body["accessCode"] = pwd

    async def _req():
        return await json_post(session, url, cookie, data=body, as_form=True)

    try:
        payload = await retry_with_backoff(_req, max_retries=2)
    except aiohttp.ClientResponseError as exc:
        if exc.status in (401, 403):
            raise ShareAccessDeniedError(f"提取码错误或无权访问 (HTTP {exc.status})")
        raise ShareNotFoundError(f"分享信息请求失败: {exc}")

    if not isinstance(payload, dict):
        raise ShareNotFoundError("分享信息响应格式异常")

    # 检查错误
    code = str(payload.get("res_code", payload.get("code", "")) or "").strip()
    if code and code not in ("0", "200"):
        msg = str(payload.get("res_message", payload.get("msg", "")) or "").strip()
        if "密码" in msg or "pwd" in msg.lower() or "提取码" in msg:
            raise ShareAccessDeniedError(f"提取码错误: {msg}")
        raise ShareNotFoundError(f"解析分享失败: {msg}")

    data = payload.get("data", payload)
    if not isinstance(data, dict):
        raise ShareNotFoundError("分享信息 data 字段缺失")

    share_id = share_id or str(data.get("shareId", "") or "").strip()
    if not share_id:
        raise ShareNotFoundError("未获取到 shareId")

    root_file_id = str(data.get("fileId", "") or "").strip() or share_id

    title = str(
        data.get("shareName", data.get("fileName", data.get("title", data.get("name", "")))) or ""
    ).strip()

    # 单文件分享：data 中直接包含文件信息，不需要再调 listShareDir
    direct_file: dict | None = None
    try:
        file_size = int(data.get("fileSize", data.get("size", 0)) or 0)
        if file_size > 0:
            direct_file = {
                "fileId": root_file_id,
                "fileName": title,
                "fileSize": file_size,
                "isFolder": False,
            }
    except (TypeError, ValueError):
        pass

    return share_id, root_file_id, title, direct_file


async def _list_files(
    session: aiohttp.ClientSession,
    cookie: str,
    share_id: str,
    *,
    share_code: str,
    access_code: str = "",
    folder_id: str = "",
    _depth: int = 0,
) -> list[ShareFile]:
    """列出分享内的文件。"""

    url = "https://api.cloud.189.cn/open/share/listShareDir.action"
    result: list[ShareFile] = []
    seen_ids: set[str] = set()
    page_num = 1
    total_count = 0

    while page_num <= 50:
        body = {
            "shareId": share_id,
            "shareMode": "1",
            "iconOption": "5",
            "pageNum": str(page_num),
            "pageSize": "200",
            "fileId": folder_id or share_id,
            "shareDirFileId": folder_id or share_id,
            "isFolder": "true",
            "orderBy": "lastOpTime",
            "descending": "true",
        }
        if access_code:
            body["accessCode"] = access_code

        payload = await json_post(
            session,
            url,
            cookie,
            data=body,
            as_form=True,
            extra_headers={
                "Referer": f"https://cloud.189.cn/t/{share_code}" + (f"?{urlencode({'pwd': access_code})}" if access_code else ""),
                "Sign-Type": "1",
            },
        )

        if not isinstance(payload, dict):
            break

        file_list_ao = payload.get("fileListAO")
        if not isinstance(file_list_ao, dict):
            break

        if total_count <= 0:
            try:
                total_count = int(file_list_ao.get("count", 0) or 0)
            except Exception:
                total_count = 0

        raw_files = file_list_ao.get("fileList", [])
        if not isinstance(raw_files, list) or not raw_files:
            break

        added = 0
        for entry in raw_files:
            if not isinstance(entry, dict):
                continue
            file_id = str(entry.get("fileId", entry.get("id", "")) or "").strip()
            if not file_id or file_id in seen_ids:
                continue
            seen_ids.add(file_id)
            sf = ShareFile(
                file_id=file_id,
                name=str(entry.get("fileName", entry.get("name", "")) or "").strip(),
                size_bytes=int(entry.get("fileSize", entry.get("size", 0)) or 0),
                is_directory=bool(entry.get("isFolder", entry.get("isDir", False))),
                path=str(entry.get("filePath", entry.get("path", "")) or "").strip(),
            )
            result.append(sf)
            added += 1

        if added <= 0:
            break
        if total_count > 0 and len(seen_ids) >= total_count:
            break
        if len(raw_files) < 200:
            break
        page_num += 1

    # 递归展开文件夹 (最多 2 层)
    if _depth < 2:
        for sf in list(result):
            if sf.is_directory:
                try:
                    children = await _list_files(
                        session,
                        cookie,
                        share_id,
                        share_code=share_code,
                        access_code=access_code,
                        folder_id=sf.file_id,
                        _depth=_depth + 1,
                    )
                    # 文件夹本身不保留，直接展开子文件
                    result.remove(sf)
                    result.extend(children)
                except Exception:
                    pass

    return result
