"""云盘子系统 —— 通用数据模型。"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional


class CloudProviderId(str, Enum):
    """云盘供应商枚举。"""
    TIANYI = "tianyi"
    BAIDU  = "baidu"
    QUARK  = "quark"
    CTFILE = "ctfile"


# ═══════════════════════════════════════════════
# 会话 / 凭证
# ═══════════════════════════════════════════════

@dataclass
class CloudCredential:
    """单个登录凭证。"""
    provider: CloudProviderId
    cookie: str
    access_token: str = ""
    refresh_token: str = ""
    account_name: str = ""
    account_id: str = ""
    raw_extra: dict = field(default_factory=dict)

    @property
    def is_expired(self) -> bool:
        """子类可覆写。"""
        return False


# ═══════════════════════════════════════════════
# 分享
# ═══════════════════════════════════════════════

@dataclass
class ShareFile:
    """分享内的一个文件或文件夹。"""
    file_id: str
    name: str
    size_bytes: int
    is_directory: bool
    path: str = ""


@dataclass
class ShareInfo:
    """分享解析结果。"""
    provider: CloudProviderId
    share_code: str
    share_id: str
    access_code: str
    title: str
    files: List[ShareFile]


# ═══════════════════════════════════════════════
# 下载直链
# ═══════════════════════════════════════════════

@dataclass
class DirectLink:
    """文件下载直链。"""
    url: str
    expires_at: float = 0.0
    headers: dict = field(default_factory=dict)
    cookie: str = ""


# ═══════════════════════════════════════════════
# 登录状态
# ═══════════════════════════════════════════════

@dataclass
class LoginState:
    """登录校验结果。"""
    logged_in: bool
    account_name: str = ""
    message: str = ""
    credential: Optional[CloudCredential] = None
