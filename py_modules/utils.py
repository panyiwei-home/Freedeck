# utils.py - Freedeck 通用工具

import asyncio
import os
import shutil
import socket
import subprocess
import time

import config


_IP_CACHE_TTL_SECONDS = 30.0
_cached_ip_address: str | None = None
_cached_ip_timestamp = 0.0


def _is_vpn_interface(name: str) -> bool:
    """判断网卡名是否为 VPN/虚拟网卡。"""
    lowered = str(name or "").lower()
    prefixes = (
        "tun",
        "tap",
        "wg",
        "ppp",
        "pptp",
        "utun",
        "tailscale",
        "ts",
        "docker",
        "br-",
        "virbr",
        "vmnet",
        "vboxnet",
        "lo",
    )
    return lowered.startswith(prefixes)


def _get_ip_from_ip_cmd() -> str | None:
    """优先通过 ip 命令读取局域网 IPv4。"""
    ip_bin = shutil.which("ip") or "/sbin/ip" or "/usr/sbin/ip"
    if not os.path.exists(ip_bin):
        return None

    try:
        result = subprocess.run(
            [ip_bin, "-4", "-o", "addr", "show"],
            capture_output=True,
            text=True,
            check=True,
        )
    except Exception as exc:
        config.logger.debug(f"Failed to read ip addr output: {exc}")
        return None

    candidates: list[tuple[int, str]] = []
    for line in result.stdout.splitlines():
        parts = line.split()
        if len(parts) < 4:
            continue

        iface = parts[1]
        ip = parts[3].split("/")[0]
        if ip.startswith("127.") or ip.startswith("169.254."):
            continue
        if _is_vpn_interface(iface):
            continue

        priority = 1
        if iface.startswith(("wl", "wlan", "wlp", "wifi", "eth", "en", "eno", "ens", "enp")):
            priority = 0
        candidates.append((priority, ip))

    if not candidates:
        return None
    candidates.sort(key=lambda item: item[0])
    return candidates[0][1]


def get_ip_address(force_refresh: bool = False) -> str:
    """获取本机局域网 IP。"""
    global _cached_ip_address, _cached_ip_timestamp

    now = time.monotonic()
    if (
        not force_refresh
        and _cached_ip_address
        and (now - _cached_ip_timestamp) < _IP_CACHE_TTL_SECONDS
    ):
        return _cached_ip_address

    try:
        ip_address = _get_ip_from_ip_cmd()
        if not ip_address:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
                sock.connect(("8.8.8.8", 80))
                ip_address = sock.getsockname()[0]

        _cached_ip_address = ip_address
        _cached_ip_timestamp = now
        return ip_address
    except Exception as exc:
        config.logger.error(f"Failed to get IP address: {exc}")
        return _cached_ip_address or "127.0.0.1"


def is_port_in_use(port: int, timeout: float = 0.8, retries: int | None = None) -> bool:
    """检查端口是否被监听。"""
    if retries is None:
        retries = config.PORT_CHECK_RETRIES

    for attempt in range(max(1, int(retries))):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.settimeout(timeout)
                sock.connect(("127.0.0.1", int(port)))
            return True
        except (ConnectionRefusedError, TimeoutError, OSError):
            if attempt < retries - 1:
                time.sleep(config.PORT_CHECK_RETRY_DELAY)
            continue
        except Exception:
            if attempt < retries - 1:
                time.sleep(config.PORT_CHECK_RETRY_DELAY)
            continue
    return False


async def wait_for_port_release(port: int, timeout: float | None = None) -> bool:
    """等待端口释放。"""
    if timeout is None:
        timeout = config.PORT_RELEASE_TIMEOUT

    start = time.time()
    while time.time() - start < float(timeout):
        if not is_port_in_use(port):
            return True
        await asyncio.sleep(0.2)
    return False
