"""NewFreedeck — 模拟器适配器（bootstrap_install + 配置模板）
参考 EmuDeck 的配置策略：
  - 完整 SDL 控制器映射 (STEAM_DECK_SDL_GUID)
  - 模拟主机语言/区域自动设置
  - 模拟器 UI 语言中文化
  - BIOS 文件部署与验证
"""

import os
import stat
import glob as _glob
from typing import Dict, Any, List, Optional, Callable

# ── 配置常量 ──────────────────────────

# Steam Deck SDL GUID（EmuDeck 验证过的值）
STEAM_DECK_SDL_GUID = "03000000de280000ff11000001000000"

# ── 语言/区域检测 ─────────────────────

def _detect_language() -> str:
    """检测系统语言代码。"""
    lang = os.environ.get("LANG", os.environ.get("LANGUAGE", ""))
    code = lang.split(".")[0].split("_")[0].lower() if lang else "en"
    return code if code else "en"


# ═══════════════════════════════════════════════
# 适配器基类
# ═══════════════════════════════════════════════

class EmulatorAdapter:
    """模拟器适配器基类。每个平台一个子类。"""

    emulator_id: str = ""
    display_name: str = ""
    platform: str = ""
    package_game_id: str = ""
    launch_args: str = '"{rom}"'
    bios_hints: List[str] = []
    portable_write_targets: List[str] = []

    # ── 通用工具 ──────────────────────────

    def find_appimage(self, root_dir: str) -> Optional[str]:
        for root, _, files in os.walk(root_dir):
            for f in files:
                if f.lower().endswith(".appimage"):
                    return os.path.join(root, f)
        for pat in [os.path.join(root_dir, "*.AppImage"),
                     os.path.join(root_dir, "*", "*.AppImage"),
                     os.path.join(root_dir, "bin", "*")]:
            matches = _glob.glob(pat)
            if matches:
                return matches[0]
        return None

    def ensure_executable(self, path: str) -> bool:
        try:
            st = os.stat(path)
            os.chmod(path, st.st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH)
            return True
        except Exception:
            return False

    def enable_portable_mode(self, root_dir: str) -> None:
        for portable_file in self.portable_write_targets:
            try:
                p = os.path.join(root_dir, portable_file)
                if not os.path.exists(p):
                    os.makedirs(os.path.dirname(p) or root_dir, exist_ok=True)
                    with open(p, "w") as f:
                        f.write("")
            except Exception:
                pass

    def deploy_bios(self, root_dir: str, install_dir: str) -> List[str]:
        messages: List[str] = []
        if not self.bios_hints:
            return messages
        bios_dir = os.path.join(root_dir, "bios")
        os.makedirs(bios_dir, exist_ok=True)
        found = 0
        for hint in self.bios_hints:
            for root, _, files in os.walk(install_dir):
                for f in files:
                    if hint.lower() in f.lower():
                        src = os.path.join(root, f)
                        dst = os.path.join(bios_dir, f)
                        try:
                            if os.path.realpath(src) != os.path.realpath(dst):
                                import shutil
                                shutil.copy2(src, dst)
                            found += 1
                        except Exception:
                            pass
        if found > 0:
            messages.append(f"已部署 {found} 个 BIOS 文件到 {bios_dir}")
        elif self.bios_hints:
            messages.append(f"未找到 BIOS 文件（请手动放置到 {bios_dir}）")
        return messages

    def get_or_create_config_dir(self, root_dir: str) -> str:
        return root_dir

    def write_config(self, root_dir: str) -> List[str]:
        return []

    # ── Qt 配置辅助 ───────────────────────

    def _qt_set_str(self, cfg, section: str, key: str, value: str):
        r"""写入 QSettings INI 格式字符串，附带 \default=false。"""
        import configparser
        if isinstance(cfg, configparser.ConfigParser):
            if section not in cfg:
                cfg.add_section(section)
            cfg.set(section, key, str(value))
            # \default=false 防止模拟器重置
            dk = f"{key}\\default"
            if dk not in [k.lower() for k in cfg.options(section)]:
                cfg.set(section, dk, "false")

    def _qt_set_int(self, cfg, section: str, key: str, value: int):
        self._qt_set_str(cfg, section, key, str(value))

    # ── Qt 模拟器通用控制器 (Eden / Lime3DS) ──

    def _write_qt_player_bindings(self, cfg, player: int = 0):
        """写入 Qt 风格模拟器的 SDL 按键映射（参考 EmuDeck）。"""
        import configparser
        section = "Controls"
        if section not in cfg:
            cfg.add_section(section)
        p = f"player_{player}"

        cfg.set(section, f"{p}_type", "0")
        cfg.set(section, f"{p}_connected", "true")

        # 按钮映射
        btns = {
            "A": "0", "B": "1", "X": "2", "Y": "3",
            "SELECT": "4", "HOME": "5", "START": "6",
            "L3": "7", "R3": "8", "L1": "9", "R1": "10",
            "UP": "11", "DOWN": "12", "LEFT": "13", "RIGHT": "14",
        }
        for name, code in btns.items():
            cfg.set(section, f"button_{name.lower()}", f'"({STEAM_DECK_SDL_GUID}:{name})"')

        # D-Pad
        for d in ["up", "down", "left", "right"]:
            cfg.set(section, f"dpad_{d}", f'"({STEAM_DECK_SDL_GUID}:{d})"')

        # 摇杆
        for s in ["lstick", "rstick"]:
            cfg.set(section, s, f'"({STEAM_DECK_SDL_GUID}:{s})"')

        # 扳机
        for t in ["l", "r", "zl", "zr", "sl", "sr"]:
            cfg.set(section, f"button_{t}", f'"({STEAM_DECK_SDL_GUID}:{t})"')

        # 体感
        cfg.set(section, "motionleft", f'"({STEAM_DECK_SDL_GUID}:motionleft)"')
        cfg.set(section, "motionright", f'"({STEAM_DECK_SDL_GUID}:motionright)"')
        cfg.set(section, "udp_input_servers", "\"127.0.0.1:26760\"")

    def bootstrap_install(self, install_dir: str,
                          _settings: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """执行安装后初始化。返回 {ok: bool, messages: [...]}"""
        messages: List[str] = []
        appimage = self.find_appimage(install_dir)
        if not appimage:
            return {"ok": False, "messages": [f"未找到 {self.display_name} AppImage"]}

        if not self.ensure_executable(appimage):
            messages.append(f"{self.display_name} 执行权限设置失败")
        else:
            messages.append(f"{self.display_name} AppImage 已就绪")

        try:
            self.enable_portable_mode(install_dir)
        except Exception as e:
            messages.append(f"便携模式设置失败: {e}")

        bios_msgs = self.deploy_bios(install_dir, install_dir)
        messages.extend(bios_msgs)

        config_msgs = self.write_config(install_dir)
        messages.extend(config_msgs)

        return {"ok": True, "messages": messages, "appimage": appimage}


# ═══════════════════════════════════════════════
# DuckStation (PS1)
# ═══════════════════════════════════════════════

class DuckStationAdapter(EmulatorAdapter):
    emulator_id = "duckstation"
    display_name = "DuckStation"
    platform = "ps1"
    package_game_id = "ps1_emulator"
    launch_args = '-batch -fullscreen "{rom}"'
    bios_hints = ["scph"]  # 修复：去掉错误的 "ps2" hint
    portable_write_targets = ["portable.txt"]

    def get_or_create_config_dir(self, root_dir: str) -> str:
        d = os.path.join(root_dir, "portable_data", "settings")
        os.makedirs(d, exist_ok=True)
        return root_dir

    def write_config(self, root_dir: str) -> List[str]:
        lang = _detect_language()
        ui_lang = "zh-CN" if lang == "zh" else "en"
        settings_dir = os.path.join(root_dir, "portable_data", "settings")
        os.makedirs(settings_dir, exist_ok=True)
        ini_path = os.path.join(settings_dir, "settings.ini")

        lines = []
        lines.append("[Main]")
        lines.append("MainWindowGeometry = AdnQywADAAAAAAHNAAAAAAAAABQAAAGMAAADNgAAARcAAAAAAAAAFAAAAYwAAAM2AAABFwAAAAAAAAAACgAGAAUAAAAHAAAACAAAAAABAAA=")
        lines.append(f"Language = {ui_lang}")
        lines.append("")

        lines.append("[ControllerPorts]")
        lines.append("MultitapMode = false")
        lines.append("")

        lines.append("[InputSources]")
        lines.append(f"SDL-0 = {STEAM_DECK_SDL_GUID}")
        lines.append("")

        # 控制器1 按键映射（SDL 按钮码格式）
        lines.append("[Controller1]")
        lines.append("Type = AnalogController")
        bindings = {
            "Up": "SDL-0/DPadUp", "Down": "SDL-0/DPadDown",
            "Left": "SDL-0/DPadLeft", "Right": "SDL-0/DPadRight",
            "Start": "SDL-0/Start", "Select": "SDL-0/Back",
            "Triangle": "SDL-0/Y", "Circle": "SDL-0/B",
            "Cross": "SDL-0/A", "Square": "SDL-0/X",
            "L1": "SDL-0/LeftShoulder", "R1": "SDL-0/RightShoulder",
            "L2": "SDL-0/+LeftTrigger", "R2": "SDL-0/+RightTrigger",
            "L3": "SDL-0/LeftStick", "R3": "SDL-0/RightStick",
            "LeftStickUp": "SDL-0/LeftY-", "LeftStickDown": "SDL-0/LeftY+",
            "LeftStickLeft": "SDL-0/LeftX-", "LeftStickRight": "SDL-0/LeftX+",
            "RightStickUp": "SDL-0/RightY-", "RightStickDown": "SDL-0/RightY+",
            "RightStickLeft": "SDL-0/RightX-", "RightStickRight": "SDL-0/RightX+",
        }
        for k, v in bindings.items():
            lines.append(f"{k} = {v}")
        lines.append("")

        lines.append("[Hotkeys]")
        lines.append("Fullscreen = Keyboard/F11")
        lines.append("")

        lines.append("[General]")
        lines.append("UseOldMDECRoutines = false")
        lines.append("")

        lines.append("[Console]")
        lines.append("Region = Auto")
        lines.append("")

        lines.append("[BIOS]")
        lines.append("SearchDirectory = ./bios")
        lines.append("")

        lines.append("[Display]")
        lines.append("Fullscreen = true")
        lines.append("RenderAPI = Vulkan")
        lines.append("ShowOSDMessages = true")
        lines.append("")

        lines.append("[Audio]")
        lines.append("Backend = Cubeb")

        try:
            with open(ini_path, "w", encoding="utf-8") as f:
                f.write("\n".join(lines))
            return [f"{self.display_name} 配置已写入（中文/Vulkan/Steam Deck 手柄）"]
        except Exception as e:
            return [f"{self.display_name} 配置写入失败: {e}"]


# ═══════════════════════════════════════════════
# PCSX2 (PS2)
# ═══════════════════════════════════════════════

class Pcsx2Adapter(EmulatorAdapter):
    emulator_id = "pcsx2"
    display_name = "PCSX2"
    platform = "ps2"
    package_game_id = "ps2_emulator"
    launch_args = '-fullscreen "{rom}"'
    bios_hints = ["scph", "ps2", "bios"]
    portable_write_targets = ["portable.ini"]

    def get_or_create_config_dir(self, root_dir: str) -> str:
        d = os.path.join(root_dir, "inis")
        os.makedirs(d, exist_ok=True)
        return root_dir

    def write_config(self, root_dir: str) -> List[str]:
        lang = _detect_language()
        ui_lang = "zh-CN" if lang == "zh" else "en"
        inis_dir = os.path.join(root_dir, "inis")
        os.makedirs(inis_dir, exist_ok=True)

        messages: List[str] = []

        # PCSX2.ini
        pcsx2_ini = os.path.join(inis_dir, "PCSX2.ini")
        lines = []
        lines.append("[UI]")
        lines.append("ConfirmShutdown = false")
        lines.append("InhibitScreensaver = true")
        lines.append("Fullscreen = true")
        lines.append(f"Language = {ui_lang}")
        lines.append("")
        lines.append("[Folders]")
        lines.append(f"Bios = {os.path.join(root_dir, 'bios')}")
        lines.append("")
        lines.append("[EmuCore]")
        lines.append("CdvdSource = Iso")
        lines.append("")
        lines.append("[EmuCore/GS]")
        lines.append("Renderer = 2")
        lines.append("")
        lines.append("[Achievements]")
        lines.append("Enabled = false")
        try:
            with open(pcsx2_ini, "w", encoding="utf-8") as f:
                f.write("\n".join(lines))
            messages.append(f"{self.display_name} PCSX2.ini 已写入")
        except Exception as e:
            messages.append(f"{self.display_name} PCSX2.ini 失败: {e}")

        # PCSX2_ui.ini — 手柄 SDL 源
        ui_ini = os.path.join(inis_dir, "PCSX2_ui.ini")
        try:
            with open(ui_ini, "w", encoding="utf-8") as f:
                f.write(f"[InputSources]\n")
                f.write(f"SDL0 = {STEAM_DECK_SDL_GUID}\n")
                f.write("\n[Folders]\n")
                f.write(f"Bios = {os.path.join(root_dir, 'bios')}\n")
            messages.append(f"{self.display_name} PCSX2_ui.ini 已写入")
        except Exception as e:
            messages.append(f"{self.display_name} PCSX2_ui.ini 失败: {e}")

        return messages


# ═══════════════════════════════════════════════
# RPCS3 (PS3)
# ═══════════════════════════════════════════════

class Rpcs3Adapter(EmulatorAdapter):
    emulator_id = "rpcs3"
    display_name = "RPCS3"
    platform = "ps3"
    package_game_id = "ps3_emulator"
    launch_args = '--no-gui --fullscreen "{rom}"'
    bios_hints = ["PS3UPDAT.PUP"]

    def write_config(self, root_dir: str) -> List[str]:
        lang = _detect_language()
        ui_lang = "zh-CN" if lang == "zh" else "en-US"
        config_dir = os.path.join(root_dir, "config")
        gui_dir = os.path.join(config_dir, "gui_configs")
        os.makedirs(gui_dir, exist_ok=True)

        messages: List[str] = []

        # CurrentSettings.ini
        cs_ini = os.path.join(gui_dir, "CurrentSettings.ini")
        try:
            with open(cs_ini, "w", encoding="utf-8") as f:
                f.write("[GUI]\n")
                f.write("showWelcomeScreen = false\n")
                f.write("confirmationBoxExitGame = false\n")
                f.write(f"language = {ui_lang}\n")
            messages.append(f"{self.display_name} CurrentSettings.ini 已写入")
        except Exception as e:
            messages.append(f"{self.display_name} CurrentSettings.ini 失败: {e}")

        # config.yml — 系统语言
        import yaml
        try:
            yml_path = os.path.join(root_dir, "config.yml")
            cfg = {}
            if os.path.exists(yml_path):
                with open(yml_path, "r") as f:
                    cfg = yaml.safe_load(f) or {}
            cfg.setdefault("System", {})["Language"] = ui_lang
            cfg.setdefault("GUI", {})["Language"] = ui_lang
            cfg.setdefault("Video", {})["Renderer"] = "Vulkan"
            with open(yml_path, "w") as f:
                yaml.dump(cfg, f, default_flow_style=False, allow_unicode=True)
            messages.append(f"{self.display_name} config.yml 已写入")
        except Exception as e:
            messages.append(f"{self.display_name} config.yml 失败: {e}")

        return messages


# ═══════════════════════════════════════════════
# PPSSPP (PSP)
# ═══════════════════════════════════════════════

class PpssppAdapter(EmulatorAdapter):
    emulator_id = "ppsspp"
    display_name = "PPSSPP"
    platform = "psp"
    package_game_id = "psp_emulator"
    launch_args = '"{rom}"'
    portable_write_targets = ["portable.txt"]

    def get_or_create_config_dir(self, root_dir: str) -> str:
        d = os.path.join(root_dir, "memstick", "PSP", "SYSTEM")
        os.makedirs(d, exist_ok=True)
        return root_dir

    def write_config(self, root_dir: str) -> List[str]:
        lang = _detect_language()
        ui_lang = "zh" if lang == "zh" else "en"
        sys_lang = "1" if lang == "zh" else "0"  # 0=en, 1=zh
        memstick_dir = os.path.join(root_dir, "memstick", "PSP", "SYSTEM")
        os.makedirs(memstick_dir, exist_ok=True)

        messages: List[str] = []

        # ppsspp.ini — 完整配置
        ppsspp_ini = os.path.join(memstick_dir, "ppsspp.ini")
        lines = []
        lines.append("[General]")
        lines.append(f"Language = {ui_lang}")
        lines.append("FullScreen = True")
        lines.append("")
        lines.append("[System]")
        lines.append(f"Language = {sys_lang}")
        lines.append("")
        lines.append("[Graphics]")
        lines.append("Backend = 3")
        lines.append("RenderingMode = 1")
        lines.append("")
        lines.append("[Control]")
        lines.append(f"Device = X{STEAM_DECK_SDL_GUID}")
        # SDL 按钮码格式: 10-XXX
        btns = {
            "Up": "10-19", "Down": "10-20", "Left": "10-21", "Right": "10-22",
            "Circle": "10-190", "Cross": "10-189", "Square": "10-191", "Triangle": "10-188",
            "Start": "10-197", "Select": "10-196",
            "L": "10-4008,10-192", "R": "10-4010,10-193",
            "An.Up": "10-4003,10-4007", "An.Down": "10-4002,10-4006",
            "An.Left": "10-4001,10-4005", "An.Right": "10-4000,10-4004",
        }
        for k, v in btns.items():
            lines.append(f"{k} = {v}")
        lines.append("")
        lines.append("[Audio]")
        lines.append("Enable = True")

        try:
            with open(ppsspp_ini, "w", encoding="utf-8") as f:
                f.write("\n".join(lines))
            messages.append(f"{self.display_name} ppsspp.ini 已写入")
        except Exception as e:
            messages.append(f"{self.display_name} ppsspp.ini 失败: {e}")

        return messages


# ═══════════════════════════════════════════════
# Lime3DS (3DS)
# ═══════════════════════════════════════════════

class Lime3dsAdapter(EmulatorAdapter):
    emulator_id = "lime3ds"
    display_name = "Lime3DS"
    platform = "3ds"
    package_game_id = "3ds_emulator"
    launch_args = '"{rom}"'

    def get_or_create_config_dir(self, root_dir: str) -> str:
        d = os.path.join(root_dir, "user", "config")
        os.makedirs(d, exist_ok=True)
        return root_dir

    def write_config(self, root_dir: str) -> List[str]:
        lang = _detect_language()
        ui_lang = "zh" if lang == "zh" else "en"
        import configparser
        config_dir = os.path.join(root_dir, "user", "config")
        os.makedirs(config_dir, exist_ok=True)
        ini = os.path.join(config_dir, "qt-config.ini")

        c = configparser.ConfigParser()
        try:
            c.read(ini, encoding="utf-8")
        except Exception:
            pass

        sections = ["UI", "Renderer", "System", "Controls", "Layout", "Audio", "Camera",
                     "Data%20Storage", "Debugging", "WebService"]
        for sec in sections:
            if sec not in c:
                c.add_section(sec)

        self._qt_set_str(c, "UI", "language", ui_lang)
        self._qt_set_str(c, "UI", "confirmClose", "false")
        self._qt_set_str(c, "UI", "singleWindowMode", "true")
        self._qt_set_str(c, "UI", "fullscreen", "true")
        self._qt_set_str(c, "Renderer", "graphics_api", "1")
        self._qt_set_str(c, "Renderer", "resolution_factor", "2")
        self._qt_set_str(c, "System", "is_new_3ds", "true")
        self._qt_set_int(c, "System", "region_value", 2)
        self._qt_set_str(c, "System", "region_value\\default", "false")

        self._write_qt_player_bindings(c)

        try:
            with open(ini, "w", encoding="utf-8") as f:
                c.write(f)
            return [f"{self.display_name} 配置已写入（中文/2x/Vulkan/Steam Deck 手柄）"]
        except Exception as e:
            return [f"{self.display_name} 配置写入失败: {e}"]


# ═══════════════════════════════════════════════
# mGBA (GBA)
# ═══════════════════════════════════════════════

class MgbaAdapter(EmulatorAdapter):
    emulator_id = "mgba"
    display_name = "mGBA"
    platform = "gba"
    package_game_id = "gba_emulator"
    launch_args = '-f "{rom}"'
    portable_write_targets = ["portable.ini"]

    def write_config(self, root_dir: str) -> List[str]:
        lang = _detect_language()
        ui_lang = "zh" if lang == "zh" else "en"
        ini_path = os.path.join(root_dir, "qt.ini")
        os.makedirs(os.path.dirname(ini_path) or root_dir, exist_ok=True)

        lines = []
        lines.append("[General]")
        lines.append(f"language = {ui_lang}")
        lines.append("fullScreen = true")
        lines.append("videoRenderer = opengl")
        lines.append("pauseOnFocusLoss = false")
        lines.append("")

        # 控制器映射
        lines.append("[Input]")
        lines.append(f"gamepad_guid = {STEAM_DECK_SDL_GUID}")
        btn_map = {
            "Up": "0.0.0.19", "Down": "0.0.0.20", "Left": "0.0.0.21", "Right": "0.0.0.22",
            "A": "0.0.0.0", "B": "0.0.0.1", "L": "0.0.0.9", "R": "0.0.0.10",
            "Start": "0.0.0.6", "Select": "0.0.0.4",
        }
        for name, code in btn_map.items():
            lines.append(f"gamepad_{name} = {code}")

        try:
            with open(ini_path, "w", encoding="utf-8") as f:
                f.write("\n".join(lines))
            return [f"{self.display_name} 配置已写入（中文/全屏/Steam Deck 手柄）"]
        except Exception as e:
            return [f"{self.display_name} 配置写入失败: {e}"]


# ═══════════════════════════════════════════════
# melonDS (NDS)
# ═══════════════════════════════════════════════

class MelondsAdapter(EmulatorAdapter):
    emulator_id = "melonds"
    display_name = "melonDS"
    platform = "nds"
    package_game_id = "nds_emulator"
    launch_args = '"{rom}"'
    bios_hints = ["firmware", "bios7", "bios9", "dsi_firmware"]

    def deploy_bios(self, root_dir: str, install_dir: str) -> List[str]:
        messages: List[str] = []
        if not self.bios_hints:
            return messages
        found = 0
        for hint in self.bios_hints:
            for root, _, files in os.walk(install_dir):
                for f in files:
                    if hint.lower() in f.lower():
                        src = os.path.join(root, f)
                        dst = os.path.join(root_dir, f)
                        try:
                            if os.path.realpath(src) != os.path.realpath(dst):
                                import shutil
                                shutil.copy2(src, dst)
                            found += 1
                        except Exception:
                            pass
        if found > 0:
            messages.append(f"已部署 {found} 个 NDS 固件/BIOS 文件")
        else:
            messages.append("NDS 固件/BIOS 未找到（请手动放置 firmware.bin 等文件）")
        return messages

    def write_config(self, root_dir: str) -> List[str]:
        lang = _detect_language()
        fw_lang = "1" if lang == "zh" else "0"  # 0=Japanese, 1=English, ...近似
        ini_path = os.path.join(root_dir, "melonDS.ini")

        lines = []
        lines.append("[Video]")
        lines.append("Fullscreen = 1")
        lines.append("Renderer = 1")
        lines.append("ScreenLayout = 1")
        lines.append("")

        lines.append("[Audio]")
        lines.append("Volume = 100")
        lines.append("")

        lines.append("[Firmware]")
        lines.append("UseJit = 1")
        lines.append(f"Language = {fw_lang}")
        lines.append("")

        lines.append("[Input]")
        lines.append(f"JoyMapping = {STEAM_DECK_SDL_GUID}")
        lines.append("Joy_Up = 11")
        lines.append("Joy_Down = 12")
        lines.append("Joy_Left = 13")
        lines.append("Joy_Right = 14")
        lines.append("Joy_A = 0")
        lines.append("Joy_B = 1")
        lines.append("Joy_X = 2")
        lines.append("Joy_Y = 3")
        lines.append("Joy_L = 9")
        lines.append("Joy_R = 10")
        lines.append("Joy_Start = 6")
        lines.append("Joy_Select = 4")
        lines.append("Joy_Lid = -1")

        try:
            with open(ini_path, "w", encoding="utf-8") as f:
                f.write("\n".join(lines))
            return [f"{self.display_name} 配置已写入（全屏/Steam Deck 手柄）"]
        except Exception as e:
            return [f"{self.display_name} 配置写入失败: {e}"]


# ═══════════════════════════════════════════════
# Nestopia (FC)
# ═══════════════════════════════════════════════

class NestopiaAdapter(EmulatorAdapter):
    emulator_id = "nestopia"
    display_name = "Nestopia"
    platform = "fc"
    package_game_id = "fc_emulator"
    launch_args = '"{rom}"'

    def write_config(self, root_dir: str) -> List[str]:
        ini_path = os.path.join(root_dir, "nestopia.cfg")

        lines = []
        lines.append("[video]")
        lines.append("fullscreen = 1")
        lines.append("filter = none")
        lines.append("aspect = 4:3")
        lines.append("")

        lines.append("[input]")
        lines.append(f"joypad_guid = {STEAM_DECK_SDL_GUID}")
        btn_map = {
            "Up": "0.0.0.19", "Down": "0.0.0.20", "Left": "0.0.0.21", "Right": "0.0.0.22",
            "A": "0.0.0.0", "B": "0.0.0.1", "Select": "0.0.0.4", "Start": "0.0.0.6",
            "TurboA": "0.0.0.2", "TurboB": "0.0.0.3",
        }
        for name, code in btn_map.items():
            lines.append(f"joypad_{name} = {code}")

        try:
            with open(ini_path, "w", encoding="utf-8") as f:
                f.write("\n".join(lines))
            return [f"{self.display_name} 配置已写入（全屏/4:3/Steam Deck 手柄）"]
        except Exception as e:
            return [f"{self.display_name} 配置写入失败: {e}"]


# ═══════════════════════════════════════════════
# BlastEm (MD)
# ═══════════════════════════════════════════════

class BlastemAdapter(EmulatorAdapter):
    emulator_id = "blastem"
    display_name = "BlastEm"
    platform = "md"
    package_game_id = "md_emulator"
    launch_args = '"{rom}"'

    def write_config(self, root_dir: str) -> List[str]:
        ini_path = os.path.join(root_dir, "default.cfg")

        lines = []
        lines.append("[video]")
        lines.append("fullscreen = on")
        lines.append("vsync = on")
        lines.append("")

        lines.append("[audio]")
        lines.append("rate = 44100")
        lines.append("")

        lines.append("[ui]")
        lines.append("rom_name = on")
        lines.append("")

        lines.append("[bindings]")
        lines.append(f"pad.0.guid = {STEAM_DECK_SDL_GUID}")
        md_btns = {
            "up": "0.0.0.19", "down": "0.0.0.20", "left": "0.0.0.21", "right": "0.0.0.22",
            "a": "0.0.0.0", "b": "0.0.0.1", "c": "0.0.0.2",
            "x": "0.0.0.3", "y": "0.0.0.4", "z": "0.0.0.5",
            "start": "0.0.0.6", "mode": "0.0.0.4",
        }
        for name, code in md_btns.items():
            lines.append(f"pad.0.{name} = {code}")

        try:
            with open(ini_path, "w", encoding="utf-8") as f:
                f.write("\n".join(lines))
            return [f"{self.display_name} 配置已写入（全屏/Steam Deck 手柄）"]
        except Exception as e:
            return [f"{self.display_name} 配置写入失败: {e}"]


# ═══════════════════════════════════════════════
# Eden (Switch)
# ═══════════════════════════════════════════════

class EdenAdapter(EmulatorAdapter):
    emulator_id = "eden"
    display_name = "Eden"
    platform = "switch"
    package_game_id = "switch_emulator"
    launch_args = '-f -g "{rom}"'

    def get_or_create_config_dir(self, root_dir: str) -> str:
        d = os.path.join(root_dir, "user", "config")
        os.makedirs(d, exist_ok=True)
        return root_dir

    def write_config(self, root_dir: str) -> List[str]:
        lang = _detect_language()
        ui_lang = "zh" if lang == "zh" else "en"

        # 语言/区域索引（参考 EmuDeck）
        lang_map = {"zh": 6, "ja": 0, "ko": 7, "en": 1}
        region_map = {"zh": 4, "ja": 0, "ko": 5, "en": 1}
        sys_lang_idx = lang_map.get(lang, 1)
        sys_region_idx = region_map.get(lang, 1)

        import configparser
        config_dir = os.path.join(root_dir, "user", "config")
        os.makedirs(config_dir, exist_ok=True)
        ini = os.path.join(config_dir, "qt-config.ini")

        c = configparser.ConfigParser()
        try:
            c.read(ini, encoding="utf-8")
        except Exception:
            pass

        sections = ["UI", "Renderer", "System", "Controls", "Audio"]
        for sec in sections:
            if sec not in c:
                c.add_section(sec)

        # UI
        self._qt_set_str(c, "UI", "language", ui_lang)
        self._qt_set_str(c, "UI", "confirmClose", "false")
        self._qt_set_str(c, "UI", "singleWindowMode", "true")
        self._qt_set_str(c, "UI", "fullscreen", "true")

        # 渲染
        self._qt_set_str(c, "Renderer", "backend", "1")
        self._qt_set_str(c, "Renderer", "vulkan_device", "0")

        # System — 模拟主机语言/区域（P1）
        self._qt_set_int(c, "System", "language_index", sys_lang_idx)
        self._qt_set_str(c, "System", "language_index\\default", "false")
        self._qt_set_int(c, "System", "region_index", sys_region_idx)
        self._qt_set_str(c, "System", "region_index\\default", "false")
        self._qt_set_str(c, "System", "use_docked_mode", "true")

        # 控制器
        self._write_qt_player_bindings(c)

        try:
            with open(ini, "w", encoding="utf-8") as f:
                c.write(f)
            return [f"{self.display_name} 配置已写入（中文系统/中国区域/Vulkan/全屏/Steam Deck 手柄）"]
        except Exception as e:
            return [f"{self.display_name} 配置写入失败: {e}"]

    def deploy_bios(self, root_dir: str, install_dir: str) -> List[str]:
        """Eden 需要 prod.keys / title.keys。"""
        messages: List[str] = []
        keys_dir = os.path.join(root_dir, "user", "keys")
        os.makedirs(keys_dir, exist_ok=True)
        hints = ["prod.keys", "title.keys", "firmware"]
        found = 0
        for hint in hints:
            for root, _, files in os.walk(install_dir):
                for f in files:
                    if hint.lower() in f.lower():
                        src = os.path.join(root, f)
                        dst = os.path.join(keys_dir, f)
                        try:
                            if os.path.realpath(src) != os.path.realpath(dst):
                                import shutil
                                shutil.copy2(src, dst)
                            found += 1
                        except Exception:
                            pass
        if found > 0:
            messages.append(f"已部署 {found} 个 Switch 密钥文件")
        else:
            messages.append("Switch 模拟器需要 prod.keys / title.keys（请手动放置到 user/keys/）")
        return messages


# ═══════════════════════════════════════════════
# 注册表
# ═══════════════════════════════════════════════

_ADAPTERS: Dict[str, EmulatorAdapter] = {}

def _register():
    global _ADAPTERS
    for cls in [
        EdenAdapter, DuckStationAdapter, Pcsx2Adapter, Rpcs3Adapter,
        PpssppAdapter, Lime3dsAdapter, MgbaAdapter, MelondsAdapter,
        NestopiaAdapter, BlastemAdapter,
    ]:
        inst = cls()
        _ADAPTERS[inst.platform] = inst
        _ADAPTERS[inst.emulator_id] = inst
        _ADAPTERS[inst.package_game_id] = inst

_register()


def get_adapter(platform_or_id: str) -> Optional[EmulatorAdapter]:
    return _ADAPTERS.get(platform_or_id)


def bootstrap_emulator(platform: str, install_dir: str,
                       settings: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """执行模拟器安装后初始化。"""
    adapter = get_adapter(platform)
    if not adapter:
        return {"ok": False, "messages": [f"未找到 {platform} 模拟器适配器"]}
    return adapter.bootstrap_install(install_dir, settings)
