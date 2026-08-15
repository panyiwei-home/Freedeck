/* ═══════════════════════════════════════════
   Freedeck — 模拟器管理中心页面
   ES-DE 风格：网格卡片 + 配置状态 + 一键引导
   ═══════════════════════════════════════════ */

import {
  PanelSection,
  PanelSectionRow,
  ButtonItem,
  SteamSpinner,
  Focusable,
  gamepadLibraryClasses,
} from "@decky/ui";
import { toaster } from "@decky/api";
import { useState, useEffect, useMemo, useCallback, useRef } from "react";

/* ── API ─────────────────────────────── */

const EMU_ICON_BASE = "http://127.0.0.1:20065/emulator_icons";

interface EmuInfo {
  platform: string;
  name: string;
  icon: string;
  color: string;
  installed: boolean;
  config_ok: number;
  config_total: number;
  bios: string;
  msg: string;
}

/* ── 模拟器注册表 ────────────────────── */

const PLATFORMS = [
  { id: "switch", name: "Switch", icon: "switch-yuzu.svg", color: "#e74a2f" },
  { id: "ps1", name: "PS1", icon: "ps1-duckstation.svg", color: "#7f8cff" },
  { id: "ps2", name: "PS2", icon: "ps2-pcsx2.svg", color: "#3b83ff" },
  { id: "ps3", name: "PS3", icon: "ps3-rpcs3.svg", color: "#7f6cff" },
  { id: "psp", name: "PSP", icon: "psp-ppsspp.png", color: "#f39a30" },
  { id: "3ds", name: "3DS", icon: "3ds-lime.svg", color: "#db7a34" },
  { id: "gba", name: "GBA", icon: "gba-mgba.png", color: "#8db43f" },
  { id: "nds", name: "NDS", icon: "nds.svg", color: "#57b198" },
  { id: "fc", name: "FC", icon: "fc-nestopia.svg", color: "#f08a35" },
  { id: "md", name: "MD", icon: "md-blastem-og.png", color: "#4d96e8" },
];

/* ═══════════════════════════════════════════
   EmuManagerPage
   ═══════════════════════════════════════════ */

export default function EmuManagerPage() {
  const [emulators, setEmulators] = useState<EmuInfo[]>([]);
  const [loading, setLoading] = useState(true);
  const [busy, setBusy] = useState<Record<string, boolean>>({});
  const [msgs, setMsgs] = useState<Record<string, string>>({});
  const cardRefs = useRef<Record<string, HTMLDivElement | null>>({});

  const load = useCallback(async () => {
    try {
      const r = await fetch("http://127.0.0.1:21337/api/emulators");
      const data = await r.json();
      setEmulators(
        PLATFORMS.map((p) => {
          const d = data.find((e: any) => e.platform === p.id);
          return {
            platform: p.id,
            name: p.name,
            icon: p.icon,
            color: p.color,
            installed: d?.installed ?? false,
            config_ok: d?.config?.ok ?? 0,
            config_total: d?.config?.total ?? 5,
            bios: d?.bios ?? "n/a",
            msg: "",
          };
        })
      );
    } catch {
      toaster.toast({ title: "无法连接后端", body: "请确认 21337 端口服务运行中", duration: 3000 });
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { load(); }, [load]);

  const handleLaunch = useCallback(async (plat: string, name: string) => {
    setBusy((p) => ({ ...p, [plat]: true }));
    setMsgs((p) => ({ ...p, [plat]: "启动中..." }));
    try {
      const r = await fetch(`http://127.0.0.1:21337/api/emulators/${plat}/launch`);
      const d = await r.json();
      setMsgs((p) => ({ ...p, [plat]: d.ok ? "" : d.error || "启动失败" }));
    } catch {
      setMsgs((p) => ({ ...p, [plat]: "连接失败" }));
    } finally {
      setBusy((p) => ({ ...p, [plat]: false }));
    }
  }, []);

  const handleBootstrap = useCallback(async (plat: string, name: string) => {
    setBusy((p) => ({ ...p, [plat]: true }));
    setMsgs((p) => ({ ...p, [plat]: "配置中..." }));
    try {
      const r = await fetch(`http://127.0.0.1:21337/api/emulators/${plat}/bootstrap`, { method: "POST" });
      const d = await r.json();
      if (d.ok) {
        toaster.toast({ title: `${name} 配置完成`, body: "", duration: 2000 });
        await load();
      } else {
        setMsgs((p) => ({ ...p, [plat]: d.messages?.[0] || "配置失败" }));
      }
    } catch {
      setMsgs((p) => ({ ...p, [plat]: "连接失败" }));
    } finally {
      setBusy((p) => ({ ...p, [plat]: false }));
    }
  }, [load]);

  const focusClass = useMemo(() => {
    const c = gamepadLibraryClasses as Record<string, string> | undefined;
    return c?.["ItemFocusAnim-darkGrey"] || c?.["ItemFocusAnim-darkerGrey"] || "";
  }, []);

  const installed = emulators.filter((e) => e.installed);
  const notInstalled = emulators.filter((e) => !e.installed);

  if (loading) {
    return (
      <div style={{ display: "flex", justifyContent: "center", paddingTop: 80 }}>
        <SteamSpinner style={{ width: 40, height: 40 }} />
      </div>
    );
  }

  return (
    <div className="freedeck-emu-manager-root" style={{ padding: "0 16px 40px", overflow: "visible" }}>
      <style>{CSS}</style>

      {/* 已安装 */}
      {installed.length > 0 && (
        <PanelSection title={`已安装 · ${installed.length}`}>
          <div className="freedeck-emu-grid">
            {installed.map((emu, i) => (
              <EmulatorCard
                key={emu.platform}
                emu={emu}
                index={i}
                busy={busy[emu.platform]}
                msg={msgs[emu.platform]}
                focusClass={focusClass}
                onLaunch={handleLaunch}
                onBootstrap={handleBootstrap}
                refFn={(node) => { cardRefs.current[emu.platform] = node; }}
              />
            ))}
          </div>
        </PanelSection>
      )}

      {/* 未安装 */}
      {notInstalled.length > 0 && (
        <PanelSection title={`未安装 · ${notInstalled.length}`}>
          <div className="freedeck-emu-grid">
            {notInstalled.map((emu, i) => (
              <EmulatorCard
                key={emu.platform}
                emu={emu}
                index={i}
                busy={false}
                msg=""
                focusClass={focusClass}
                onLaunch={handleLaunch}
                onBootstrap={handleBootstrap}
                refFn={(node) => { cardRefs.current[emu.platform] = node; }}
              />
            ))}
          </div>
        </PanelSection>
      )}
    </div>
  );
}

/* ── 单张卡片 ──────────────────────────── */

function EmulatorCard({
  emu, busy, msg, focusClass, onLaunch, onBootstrap, refFn,
}: {
  emu: EmuInfo;
  index: number;
  busy?: boolean;
  msg?: string;
  focusClass: string;
  onLaunch: (plat: string, name: string) => void;
  onBootstrap: (plat: string, name: string) => void;
  refFn: (node: HTMLDivElement | null) => void;
}) {
  const cfgPct = emu.installed ? Math.round((emu.config_ok / emu.config_total) * 100) : 0;
  const cfgColor = cfgPct >= 80 ? "#5a5" : cfgPct >= 40 ? "#f5a623" : "#c44";

  return (
    <div className="freedeck-emu-card" ref={refFn}>
      <div className="freedeck-emu-card-inner">
        {/* 顶部色条 */}
        <div className="freedeck-emu-card-bar" style={{ background: emu.color }} />

        {/* 图标 */}
        <div className="freedeck-emu-card-icon-wrap">
          <img
            src={`${EMU_ICON_BASE}/${emu.icon}`}
            alt={emu.name}
            className="freedeck-emu-card-icon"
          />
        </div>

        {/* 名称 */}
        <div className="freedeck-emu-card-name">{emu.name}</div>

        {/* 状态 */}
        {emu.installed ? (
          <div className="freedeck-emu-card-status">
            <div className="freedeck-emu-card-cfg">
              <span style={{ color: cfgColor }}>配置 {emu.config_ok}/{emu.config_total}</span>
              <div className="freedeck-emu-card-cfg-bar">
                <div className="freedeck-emu-card-cfg-fill" style={{ width: `${cfgPct}%`, background: cfgColor }} />
              </div>
            </div>
            {emu.bios !== "n/a" && emu.bios !== "none" && (
              <div className="freedeck-emu-card-bios">
                BIOS: {emu.bios}
              </div>
            )}
            {msg ? (
              <div className="freedeck-emu-card-msg">{msg}</div>
            ) : null}
          </div>
        ) : (
          <div className="freedeck-emu-card-status" style={{ color: "#6b727a", fontSize: 11 }}>
            未安装 · 请在设置中下载
          </div>
        )}

        {/* 按钮 */}
        {emu.installed && (
          <div className="freedeck-emu-card-actions">
            <Focusable
              className="freedeck-emu-card-btn freedeck-emu-card-btn-launch"
              focusClassName={focusClass}
              onClick={() => onLaunch(emu.platform, emu.name)}
              onOKButton={() => onLaunch(emu.platform, emu.name)}
              onOKActionDescription="启动模拟器"
              disabled={busy}
            >
              启动
            </Focusable>
            <Focusable
              className="freedeck-emu-card-btn freedeck-emu-card-btn-config"
              focusClassName={focusClass}
              onClick={() => onBootstrap(emu.platform, emu.name)}
              onOKButton={() => onBootstrap(emu.platform, emu.name)}
              onOKActionDescription="自动配置"
              disabled={busy}
            >
              ⚙ 配置
            </Focusable>
          </div>
        )}
      </div>
    </div>
  );
}

/* ── CSS ───────────────────────────────── */

const CSS = `
.freedeck-emu-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(164px, 1fr));
  gap: 12px;
  padding: 4px 0 12px;
}

.freedeck-emu-card {
  position: relative;
}

.freedeck-emu-card-inner {
  background: rgba(255,255,255,0.03);
  border: 1px solid rgba(255,255,255,0.06);
  border-radius: 10px;
  padding: 0 12px 12px;
  display: flex;
  flex-direction: column;
  align-items: center;
  gap: 8px;
  overflow: hidden;
  transition: border-color 150ms ease, box-shadow 150ms ease;
}

.freedeck-emu-card-bar {
  height: 4px;
  width: calc(100% + 24px);
  margin: 0 -12px;
}

.freedeck-emu-card-icon-wrap {
  width: 56px;
  height: 56px;
  display: flex;
  align-items: center;
  justify-content: center;
  margin-top: 6px;
}

.freedeck-emu-card-icon {
  width: 48px;
  height: 48px;
  object-fit: contain;
}

.freedeck-emu-card-name {
  font-size: 14px;
  font-weight: 700;
  color: #cac4d0;
  text-align: center;
}

.freedeck-emu-card-status {
  width: 100%;
  display: flex;
  flex-direction: column;
  align-items: center;
  gap: 4px;
  font-size: 10px;
  color: #8b929a;
}

.freedeck-emu-card-cfg {
  width: 100%;
  text-align: center;
  font-size: 11px;
  font-weight: 600;
}

.freedeck-emu-card-cfg-bar {
  width: 100%;
  height: 3px;
  background: rgba(255,255,255,0.06);
  border-radius: 2px;
  margin-top: 3px;
  overflow: hidden;
}

.freedeck-emu-card-cfg-fill {
  height: 100%;
  border-radius: 2px;
  transition: width 0.5s ease;
}

.freedeck-emu-card-msg {
  color: #67c1f5;
  font-size: 10px;
  margin-top: 2px;
}

.freedeck-emu-card-actions {
  width: 100%;
  display: flex;
  gap: 6px;
  margin-top: 4px;
}

.freedeck-emu-card-btn {
  flex: 1;
  display: flex;
  align-items: center;
  justify-content: center;
  padding: 6px 8px;
  font-size: 11px;
  font-weight: 600;
  border-radius: 6px;
  border: 1px solid rgba(255,255,255,0.08);
  background: rgba(255,255,255,0.04);
  color: #cac4d0;
  cursor: pointer;
  min-height: 28px;
}

.freedeck-emu-card-btn-launch {
  background: rgba(74,158,255,0.1);
  border-color: rgba(74,158,255,0.2);
  color: #67c1f5;
}

.freedeck-emu-card-btn-config {
  flex: 0 0 auto;
  padding: 6px 8px;
}
`;
