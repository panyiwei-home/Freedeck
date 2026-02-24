# config.py - Freedeck 配置与日志

import logging
import os
from pathlib import Path


def setup_logger() -> logging.Logger:
    """初始化日志器。"""
    try:
        logging.basicConfig(
            level=logging.INFO,
            filename="/tmp/freedeck.log",
            format="[%(asctime)s | %(filename)s:%(lineno)s:%(funcName)s] %(levelname)s: %(message)s",
            filemode="a",
            force=True,
        )
    except Exception:
        logging.basicConfig(
            level=logging.INFO,
            format="[%(asctime)s | %(filename)s:%(lineno)s:%(funcName)s] %(levelname)s: %(message)s",
            force=True,
        )
    return logging.getLogger("freedeck")


logger = setup_logger()
logger.setLevel(logging.INFO)

# 路径配置
HOME_DIR = str(Path.home())
DOWNLOADS_DIR = str(Path.home() / "Downloads")
SHARE_DIR = str(Path.home() / ".local" / "share")
DECKY_SEND_DIR = os.path.join(SHARE_DIR, "Freedeck")

# 服务配置
# 仅监听回环地址，禁止局域网访问。
DEFAULT_SERVER_HOST = "127.0.0.1"
DEFAULT_SERVER_PORT = 59271
PORT_CHECK_RETRIES = 5
PORT_CHECK_RETRY_DELAY = 0.3
PORT_RELEASE_TIMEOUT = 5.0

# 设置键
SETTINGS_KEY = "freedeck_settings"
SETTING_RUNNING = "running"
SETTING_PORT = "port"
SETTING_DOWNLOAD_DIR = "download_dir"
