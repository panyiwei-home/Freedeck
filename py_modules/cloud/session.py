"""云盘子系统 —— 会话管理与凭证持久化。"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Optional

from .models import CloudCredential, CloudProviderId
from .base import CloudProvider
from .errors import LoginExpiredError


class SessionStore:
    """凭证本地持久化 (JSON 文件)。

    存储结构:
      {
        "tianyi": { "cookie": "...", "access_token": "...", ... },
        "baidu":  { ... },
      }
    """

    def __init__(self, path: str | Path):
        self._path = Path(path)
        self._dir = self._path.parent

    # ── 公开 API ──────────────────────────

    def load(self, provider: CloudProviderId) -> CloudCredential | None:
        """加载指定云盘的凭证。"""
        all_data = self._read()
        entry = all_data.get(provider.value)
        if not isinstance(entry, dict):
            return None
        return _dict_to_credential(provider, entry)

    def save(self, credential: CloudCredential) -> None:
        """保存凭证。"""
        all_data = self._read()
        all_data[credential.provider.value] = _credential_to_dict(credential)
        self._write(all_data)

    def delete(self, provider: CloudProviderId) -> None:
        """删除凭证。"""
        all_data = self._read()
        all_data.pop(provider.value, None)
        self._write(all_data)

    def load_all(self) -> dict[CloudProviderId, CloudCredential]:
        """加载全部凭证。"""
        all_data = self._read()
        result: dict[CloudProviderId, CloudCredential] = {}
        for key, entry in all_data.items():
            try:
                pid = CloudProviderId(key)
            except ValueError:
                continue
            if isinstance(entry, dict):
                cred = _dict_to_credential(pid, entry)
                if cred is not None:
                    result[pid] = cred
        return result

    # ── 内部 ──────────────────────────────

    def _read(self) -> dict:
        if not self._path.exists():
            return {}
        try:
            text = self._path.read_text(encoding="utf-8")
            data = json.loads(text)
            return data if isinstance(data, dict) else {}
        except Exception:
            return {}

    def _write(self, data: dict) -> None:
        self._dir.mkdir(parents=True, exist_ok=True)
        tmp = self._path.with_suffix(".tmp")
        tmp.write_text(
            json.dumps(data, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        tmp.replace(self._path)


class SessionManager:
    """会话管理器 —— 串联 Store 与 Provider。

    使用示例::

        store = SessionStore(Path("~/.freedeck/credentials.json"))
        mgr  = SessionManager(store)
        provider = TianyiCloudProvider()
        cred = await mgr.ensure_valid(provider)   # 自动加载/校验/刷新
    """

    def __init__(self, store: SessionStore):
        self._store = store

    async def ensure_valid(
        self,
        provider: CloudProvider,
        credential: CloudCredential | None = None,
    ) -> CloudCredential:
        """确保有一个有效的凭证。

        1. 若未传入 credential → 从 store 加载
        2. 调用 provider.check_login() 校验
        3. 若过期 → 尝试 provider.refresh_credential()
        4. 成功后保存到 store

        Raises:
            LoginExpiredError: 凭证完全失效，需重新登录
        """
        if credential is None:
            credential = self._store.load(provider.provider_id)

        if credential is None:
            raise LoginExpiredError(f"{provider.provider_id.value} 无已保存的凭证")

        state = await provider.check_login(credential)

        if state.logged_in:
            if state.credential is not None:
                self._store.save(state.credential)
                return state.credential
            self._store.save(credential)
            return credential

        # 尝试刷新
        try:
            fresh = await provider.refresh_credential(credential)
            state2 = await provider.check_login(fresh)
            if state2.logged_in:
                if state2.credential is not None:
                    self._store.save(state2.credential)
                    return state2.credential
                self._store.save(fresh)
                return fresh
        except Exception:
            pass

        raise LoginExpiredError(
            f"{provider.provider_id.value} 登录已过期，请重新登录",
        )

    def save(self, credential: CloudCredential) -> None:
        """直接保存凭证。"""
        self._store.save(credential)

    def delete(self, provider: CloudProviderId) -> None:
        """删除凭证。"""
        self._store.delete(provider)

    def load(self, provider: CloudProviderId) -> CloudCredential | None:
        """加载凭证（不校验）。"""
        return self._store.load(provider)


# ── 辅助 ──────────────────────────────────

def _credential_to_dict(cred: CloudCredential) -> dict:
    return {
        "provider": cred.provider.value,
        "cookie": cred.cookie,
        "access_token": cred.access_token,
        "refresh_token": cred.refresh_token,
        "account_name": cred.account_name,
        "account_id": cred.account_id,
        "raw_extra": cred.raw_extra,
        "_saved_at": time.time(),
    }


def _dict_to_credential(provider: CloudProviderId, data: dict) -> CloudCredential | None:
    cookie = str(data.get("cookie", "") or "").strip()
    if not cookie:
        return None
    return CloudCredential(
        provider=provider,
        cookie=cookie,
        access_token=str(data.get("access_token", "") or ""),
        refresh_token=str(data.get("refresh_token", "") or ""),
        account_name=str(data.get("account_name", "") or ""),
        account_id=str(data.get("account_id", "") or ""),
        raw_extra=data.get("raw_extra", {}) if isinstance(data.get("raw_extra"), dict) else {},
    )
