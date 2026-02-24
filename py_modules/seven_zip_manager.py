# seven_zip_manager.py - 7z 解压运行时管理
#
# 该模块负责定位 7z 可执行文件并执行解压，支持进度回调。

from __future__ import annotations

import os
import re
import shutil
import subprocess
from pathlib import Path
from typing import Callable, List, Optional, Sequence


class SevenZipError(RuntimeError):
    """7z 相关异常。"""


class SevenZipManager:
    """7z 解压管理器。"""

    def __init__(self, plugin_dir: str):
        self._plugin_dir = str(plugin_dir or "")

    def _resolve_binary_path(self) -> str:
        """优先解析插件内置 7z，可回退系统命令。"""
        env_path = (os.getenv("FREEDECK_7Z_BIN") or "").strip()
        if env_path and os.path.isfile(env_path):
            return env_path

        root = Path(self._plugin_dir)
        candidates: List[Path] = [
            root / "defaults" / "7z" / "linux-x86_64" / "7zz",
            root / "defaults" / "7z" / "linux-x64" / "7zz",
            root / "defaults" / "7z" / "7zz",
            root / "defaults" / "7z" / "7z",
            root / "defaults" / "7zz",
            root / "defaults" / "7z",
        ]
        for candidate in candidates:
            if candidate.is_file():
                return str(candidate)

        for command in ("7zz", "7zr", "7z"):
            system_path = shutil.which(command)
            if system_path:
                return system_path

        raise SevenZipError("解压组件不可用，未找到内置 7z 或系统 7z 命令")

    def extract_archive(
        self,
        archive_path: str,
        output_dir: str,
        progress_cb: Optional[Callable[[float], None]] = None,
    ) -> None:
        """执行解压流程。"""
        archive = os.path.realpath(os.path.expanduser((archive_path or "").strip()))
        target = os.path.realpath(os.path.expanduser((output_dir or "").strip()))
        if not archive or not os.path.isfile(archive):
            raise SevenZipError(f"待解压文件不存在: {archive_path}")
        if not target:
            raise SevenZipError("安装目录无效")
        os.makedirs(target, exist_ok=True)

        binary = self._resolve_binary_path()
        try:
            mode = os.stat(binary).st_mode
            if mode & 0o111 == 0:
                os.chmod(binary, mode | 0o755)
        except Exception:
            # 权限修复失败时继续尝试执行，保留真实错误给调用方。
            pass

        args = [
            binary,
            "x",
            "-y",
            f"-o{target}",
            archive,
            "-bsp1",
        ]

        try:
            process = subprocess.Popen(
                args,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
                errors="ignore",
            )
        except Exception as exc:
            raise SevenZipError(f"启动 7z 失败: {exc}") from exc

        percent_re = re.compile(r"(\d{1,3})%")
        output_tail: List[str] = []
        if process.stdout is not None:
            for line in process.stdout:
                text = line.strip()
                if text:
                    output_tail.append(text)
                    if len(output_tail) > 12:
                        output_tail.pop(0)
                match = percent_re.search(text)
                if match and progress_cb:
                    try:
                        progress_cb(float(match.group(1)))
                    except Exception:
                        pass

        return_code = process.wait()
        if return_code != 0:
            hint = " | ".join(output_tail[-6:]) if output_tail else "no output"
            raise SevenZipError(f"7z 解压失败，exit={return_code}，诊断={hint}")

        if progress_cb:
            try:
                progress_cb(100.0)
            except Exception:
                pass

    def create_archive(
        self,
        archive_path: str,
        source_paths: Sequence[str],
        working_dir: str = "",
        progress_cb: Optional[Callable[[float], None]] = None,
    ) -> None:
        """执行压缩流程。"""
        archive = os.path.realpath(os.path.expanduser((archive_path or "").strip()))
        if not archive:
            raise SevenZipError("压缩包路径无效")

        sources: List[str] = []
        for item in list(source_paths or []):
            path = os.path.realpath(os.path.expanduser(str(item or "").strip()))
            if not path:
                continue
            if not os.path.exists(path):
                raise SevenZipError(f"待压缩路径不存在: {item}")
            sources.append(path)
        if not sources:
            raise SevenZipError("缺少待压缩路径")

        binary = self._resolve_binary_path()
        try:
            mode = os.stat(binary).st_mode
            if mode & 0o111 == 0:
                os.chmod(binary, mode | 0o755)
        except Exception:
            pass

        if working_dir:
            cwd = os.path.realpath(os.path.expanduser(str(working_dir).strip()))
        else:
            first = sources[0]
            cwd = first if os.path.isdir(first) else os.path.dirname(first)

        if not cwd or not os.path.isdir(cwd):
            raise SevenZipError("压缩工作目录无效")

        rel_sources: List[str] = []
        for source in sources:
            try:
                rel = os.path.relpath(source, cwd)
            except Exception:
                rel = source
            rel_sources.append(rel)

        archive_dir = os.path.dirname(archive)
        if archive_dir:
            os.makedirs(archive_dir, exist_ok=True)

        args = [
            binary,
            "a",
            "-t7z",
            "-y",
            archive,
            *rel_sources,
            "-bsp1",
        ]

        try:
            process = subprocess.Popen(
                args,
                cwd=cwd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
                errors="ignore",
            )
        except Exception as exc:
            raise SevenZipError(f"启动 7z 失败: {exc}") from exc

        percent_re = re.compile(r"(\d{1,3})%")
        output_tail: List[str] = []
        if process.stdout is not None:
            for line in process.stdout:
                text = line.strip()
                if text:
                    output_tail.append(text)
                    if len(output_tail) > 12:
                        output_tail.pop(0)
                match = percent_re.search(text)
                if match and progress_cb:
                    try:
                        progress_cb(float(match.group(1)))
                    except Exception:
                        pass

        return_code = process.wait()
        if return_code != 0:
            hint = " | ".join(output_tail[-6:]) if output_tail else "no output"
            raise SevenZipError(f"7z 压缩失败，exit={return_code}，诊断={hint}")

        if progress_cb:
            try:
                progress_cb(100.0)
            except Exception:
                pass
