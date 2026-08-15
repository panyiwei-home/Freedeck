"""
This module exposes various constants and helpers useful for decky plugins.
* Plugin's settings and configurations should be stored under `DECKY_PLUGIN_SETTINGS_DIR`.
* Plugin's runtime data should be stored under `DECKY_PLUGIN_RUNTIME_DIR`.
* Plugin's persistent log files should be stored under `DECKY_PLUGIN_LOG_DIR`.
"""

__version__ = '1.0.0'

import logging
from typing import Any

HOME: str
DECKY_HOME: str
DECKY_USER: str
DECKY_USER_HOME: str
DECKY_PLUGIN_DIR: str
DECKY_PLUGIN_SETTINGS_DIR: str
DECKY_PLUGIN_RUNTIME_DIR: str
DECKY_PLUGIN_LOG_DIR: str
DECKY_PLUGIN_NAME: str
DECKY_PLUGIN_VERSION: str
DECKY_PLUGIN_AUTHOR: str

logger: logging.Logger

def migrate_any(target: str, *files: str): ...
def migrate_settings(*files: str): ...
def migrate_runtime(*files: str): ...
def migrate_logs(*files: str): ...
