"""云盘子系统 —— 工厂函数与公开导出。"""

from .models import (
    CloudProviderId,
    CloudCredential,
    ShareFile,
    ShareInfo,
    DirectLink,
    LoginState,
)
from .errors import (
    CloudError,
    LoginExpiredError,
    LoginFailedError,
    ShareNotFoundError,
    ShareAccessDeniedError,
    DirectLinkExpiredError,
    DirectLinkRateLimited,
    DirectLinkError,
)
from .session import SessionStore, SessionManager
from .tianyi import TianyiCloudProvider


def get_provider(provider_id: CloudProviderId):
    """根据标识返回对应的 CloudProvider 实例。

    Raises:
        ValueError: 不支持的云盘类型
    """
    if provider_id == CloudProviderId.TIANYI:
        return TianyiCloudProvider()
    raise ValueError(f"Unsupported cloud provider: {provider_id}")


__all__ = [
    # 工厂
    "get_provider",
    # 模型
    "CloudProviderId",
    "CloudCredential",
    "ShareFile",
    "ShareInfo",
    "DirectLink",
    "LoginState",
    # 错误
    "CloudError",
    "LoginExpiredError",
    "LoginFailedError",
    "ShareNotFoundError",
    "ShareAccessDeniedError",
    "DirectLinkExpiredError",
    "DirectLinkRateLimited",
    "DirectLinkError",
    # 会话
    "SessionStore",
    "SessionManager",
    # 提供者
    "TianyiCloudProvider",
]
