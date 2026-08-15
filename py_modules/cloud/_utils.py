"""云盘子系统 —— 共享工具。

提供:
  - pick_first: 从 dict 按优先级取第一个存在的 key
  - parse_json_like: 解析 JSON 或 JSONP 响应
  - steamos_ssl_context: 统一 SSL 上下文
"""

from __future__ import annotations

import ssl
from typing import Any


def pick_first(d: dict, *keys: str) -> Any:
    """从 dict 中按优先级取第一个存在且非空的 key 值。

    同时检查 `d["data"]` 子节点（云盘 API 常见模式）。
    """
    for k in keys:
        v = d.get(k)
        if v is not None and v != "":
            return v
    data = d.get("data")
    if isinstance(data, dict):
        for k in keys:
            v = data.get(k)
            if v is not None and v != "":
                return v
    return None


def parse_json_like(text: str) -> dict:
    """解析 JSON 或 JSONP 响应体，始终返回 dict。

    支持:
      - 标准 JSON: {"key":"val"}
      - JSONP:    callback({"key":"val"});
    """
    import json as _json
    import re

    try:
        data = _json.loads(text)
        return data if isinstance(data, dict) else {}
    except _json.JSONDecodeError:
        m = re.search(r"^\s*[\w.$]+\(([\s\S]+)\)\s*;?\s*$", text)
        if m:
            data = _json.loads(m.group(1))
            return data if isinstance(data, dict) else {}
        return {}


def steamos_ssl_context() -> ssl.SSLContext:
    """创建兼容 SteamOS 的 SSL 上下文。

    FIXME: SteamOS 系统证书链有时不完整，这里禁用了主机名验证。
           长期方案应在插件安装时补全 CA 证书，或仅在连接失败时回退到不安全模式。
    """
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx
