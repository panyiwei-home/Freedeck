# aria2_manager.py - aria2 进程与 RPC 管理
#
# 该模块负责启动 aria2、执行 RPC、同步状态。

from __future__ import annotations

import os
import platform
import shutil
import socket
import subprocess
import threading
import time
import uuid
import asyncio
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

import aiohttp

import config


@dataclass
class Aria2Runtime:
    """运行中的 aria2 信息。"""

    process: subprocess.Popen
    rpc_url: str
    rpc_secret: str
    binary_path: str
    started_at: int


class Aria2Error(RuntimeError):
    """aria2 相关异常。"""


def _pick_free_port() -> int:
    """申请本机空闲端口。"""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _is_alive(proc: Optional[subprocess.Popen]) -> bool:
    """判断进程是否仍在运行。"""
    return bool(proc and proc.poll() is None)


def _now_ts() -> int:
    """返回当前秒级时间戳。"""
    return int(time.time())


class Aria2Manager:
    """aria2 进程管理器。"""

    def __init__(self, plugin_dir: str, work_dir: str):
        self._plugin_dir = plugin_dir
        self._work_dir = work_dir
        self._lock = threading.RLock()
        self._runtime: Optional[Aria2Runtime] = None

    def _resolve_binary_path(self) -> str:
        """优先解析内置 aria2，可回退系统命令。"""
        env_path = (os.getenv("FRIENDECK_ARIA2_BIN") or "").strip()
        if env_path and os.path.isfile(env_path):
            return env_path

        root = Path(self._plugin_dir)
        machine = platform.machine().lower()
        candidates: List[Path] = []

        if machine in {"x86_64", "amd64"}:
            candidates.append(root / "defaults" / "aria2" / "linux-x64" / "aria2c")
        if machine in {"aarch64", "arm64"}:
            candidates.append(root / "defaults" / "aria2" / "linux-arm64" / "aria2c")

        candidates.extend(
            [
                root / "defaults" / "aria2" / "aria2c",
                root / "defaults" / "aria2c",
            ]
        )

        for candidate in candidates:
            if candidate.is_file():
                return str(candidate)

        system_path = shutil.which("aria2c")
        if system_path:
            return system_path
        raise Aria2Error("下载组件不可用，未找到内置 aria2 或系统 aria2c")

    def _start_process(self) -> Aria2Runtime:
        """启动 aria2 进程。"""
        binary_path = self._resolve_binary_path()
        os.makedirs(self._work_dir, exist_ok=True)
        try:
            mode = os.stat(binary_path).st_mode
            if mode & 0o111 == 0:
                os.chmod(binary_path, mode | 0o755)
        except Exception:
            # 权限设置失败不立即中断，让后续启动给出真实报错。
            pass

        port = _pick_free_port()
        rpc_secret = uuid.uuid4().hex
        session_file = os.path.join(self._work_dir, "aria2.session")
        input_file = session_file if os.path.exists(session_file) else "/dev/null"

        args = [
            binary_path,
            "--enable-rpc=true",
            "--rpc-listen-all=false",
            "--rpc-allow-origin-all=true",
            "--rpc-listen-port",
            str(port),
            "--rpc-secret",
            rpc_secret,
            "--dir",
            self._work_dir,
            "--continue=true",
            "--max-concurrent-downloads=3",
            "--input-file",
            input_file,
            "--save-session",
            session_file,
            "--save-session-interval=15",
            "--auto-file-renaming=false",
            "--allow-overwrite=true",
            "--daemon=false",
            "--summary-interval=0",
        ]

        proc = subprocess.Popen(
            args,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            cwd=self._work_dir,
        )

        runtime = Aria2Runtime(
            process=proc,
            rpc_url=f"http://127.0.0.1:{port}/jsonrpc",
            rpc_secret=rpc_secret,
            binary_path=binary_path,
            started_at=_now_ts(),
        )
        return runtime

    async def _rpc_call(self, method: str, params: Optional[List[Any]] = None) -> Any:
        """调用 aria2 JSON-RPC。"""
        with self._lock:
            runtime = self._runtime
        if runtime is None or not _is_alive(runtime.process):
            raise Aria2Error("aria2 未运行")

        payload: Dict[str, Any] = {
            "jsonrpc": "2.0",
            "id": str(_now_ts()),
            "method": method,
            "params": [f"token:{runtime.rpc_secret}"] + (params or []),
        }
        timeout = aiohttp.ClientTimeout(total=12)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(runtime.rpc_url, json=payload) as resp:
                if resp.status >= 400:
                    text = (await resp.text())[:240]
                    raise Aria2Error(f"aria2 rpc 请求失败 status={resp.status} body={text}")
                try:
                    data = await resp.json(content_type=None)
                except Exception as exc:
                    text = (await resp.text())[:240]
                    raise Aria2Error(f"aria2 rpc 解析失败: {exc}; body={text}") from exc

        if not isinstance(data, dict):
            raise Aria2Error("aria2 rpc 响应格式异常")
        if data.get("error"):
            err = data.get("error")
            if isinstance(err, dict):
                message = str(err.get("message", "unknown"))
                code = err.get("code")
                raise Aria2Error(f"aria2 rpc 错误 code={code} message={message}")
            raise Aria2Error("aria2 rpc 返回错误")
        return data.get("result")

    async def ensure_running(self) -> Dict[str, str]:
        """确保 aria2 可用，必要时自动启动。"""
        with self._lock:
            runtime = self._runtime

        if runtime and _is_alive(runtime.process):
            try:
                await self._rpc_call("aria2.getVersion", [])
                return {
                    "rpc_url": runtime.rpc_url,
                    "binary_path": runtime.binary_path,
                }
            except Exception:
                # 健康检查失败时重启进程。
                pass

        with self._lock:
            if self._runtime and _is_alive(self._runtime.process):
                runtime = self._runtime
            else:
                runtime = self._start_process()
                self._runtime = runtime

        # 启动后最多等待约 5 秒。
        last_error = ""
        for _ in range(25):
            try:
                await self._rpc_call("aria2.getVersion", [])
                return {
                    "rpc_url": runtime.rpc_url,
                    "binary_path": runtime.binary_path,
                }
            except Exception as exc:
                last_error = str(exc)
                await _sleep_short()

        raise Aria2Error(f"下载组件不可用，aria2 启动失败: {last_error}")

    async def add_uri(
        self,
        direct_url: str,
        cookie: str,
        download_dir: str,
        out_name: str,
        split: int,
    ) -> str:
        """创建下载任务并返回 gid。"""
        split = max(1, min(64, int(split)))
        await self.ensure_running()
        options = {
            "dir": download_dir,
            "out": out_name,
            "header": [f"Cookie: {cookie}"],
            "split": str(split),
            "max-connection-per-server": str(split),
            "min-split-size": "1M",
            "continue": "true",
            "allow-overwrite": "true",
            "auto-file-renaming": "false",
        }
        result = await self._rpc_call("aria2.addUri", [[direct_url], options])
        gid = str(result or "").strip()
        if not gid:
            raise Aria2Error("aria2.addUri 返回空 gid")
        return gid

    async def tell_status(self, gid: str) -> Dict[str, Any]:
        """查询任务状态。"""
        keys = ["status", "totalLength", "completedLength", "downloadSpeed", "errorMessage"]
        result = await self._rpc_call("aria2.tellStatus", [gid, keys])
        if not isinstance(result, dict):
            raise Aria2Error("aria2.tellStatus 返回格式异常")
        return result

    async def pause(self, gid: str) -> None:
        """暂停任务。"""
        await self._rpc_call("aria2.forcePause", [gid])

    async def resume(self, gid: str) -> None:
        """恢复任务。"""
        await self._rpc_call("aria2.unpause", [gid])

    async def remove(self, gid: str) -> None:
        """移除任务。"""
        try:
            await self._rpc_call("aria2.remove", [gid])
        except Exception:
            pass
        try:
            await self._rpc_call("aria2.removeDownloadResult", [gid])
        except Exception:
            pass

    def stop(self) -> None:
        """停止 aria2 进程。"""
        with self._lock:
            runtime = self._runtime
            self._runtime = None
        if runtime and _is_alive(runtime.process):
            try:
                runtime.process.terminate()
                runtime.process.wait(timeout=2.0)
            except Exception:
                try:
                    runtime.process.kill()
                except Exception:
                    pass


async def _sleep_short() -> None:
    """短暂等待，避免启动探测忙等。"""
    await asyncio.sleep(0.2)
