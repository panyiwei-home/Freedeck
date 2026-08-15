"""云盘子系统 —— 异常体系。"""


class CloudError(RuntimeError):
    """所有云盘异常的基类。"""

    def __init__(self, message: str, *, diagnostics: dict | None = None):
        super().__init__(message)
        self.diagnostics = diagnostics or {}


class LoginExpiredError(CloudError):
    """登录态过期。"""


class LoginFailedError(CloudError):
    """登录失败。"""


class ShareNotFoundError(CloudError):
    """分享不存在或已失效。"""


class ShareAccessDeniedError(CloudError):
    """提取码错误或无权访问。"""


class DirectLinkExpiredError(CloudError):
    """直链过期。"""


class DirectLinkRateLimited(CloudError):
    """直链请求被限流 (429)。"""


class DirectLinkError(CloudError):
    """通用直链获取失败。"""
