/* ═══════════════════════════════════════════
   NewFreedeck — 插件入口

   SteamClient 事件注册（参考老插件架构）：
     - RegisterForGameActionStart/End → 检测非 Steam 快捷方式启动/退出
     - 仅处理 appid >= 0x80000000（非 Steam shortcut）
     - fallback: 轮询 GetRunningAppIDs（API 不可用时降级）
   ═══════════════════════════════════════════ */

import { definePlugin, routerHook } from "@decky/api";
import HomePanel from "./HomePanel";
import QrLoginPage from "./QrLoginPage";
import LibraryPage from "./LibraryPage";
import SettingsPage from "./SettingsPage";
import EmuManagerPage from "./EmuManagerPage";
import DeckyUiShowcase from "./DeckyUiShowcase";
import QuickAccessEntry from "./QuickAccessEntry";
import { HOME_ROUTE, LOGIN_ROUTE, LIBRARY_ROUTE, SETTINGS_ROUTE, EMU_MANAGER_ROUTE, SHOWCASE_ROUTE } from "./routes";
import { cloudSaveSyncOnLaunch, cloudSaveSyncOnExit } from "./api";

/* ── SteamClient 游戏事件检测 ─────────── */

const POLL_INTERVAL_MS = 3500;

function isNonSteamShortcut(appid: number): boolean {
  return appid >= 0x80000000;
}

function setupGameLifecycle() {
  const apps = (window as any).SteamClient?.Apps;
  const registrations: Array<{ unregister?: () => void }> = [];
  let pollTimer: ReturnType<typeof setInterval> | null = null;
  const runningSet = new Set<number>();

  const syncStart = async (appid: number) => {
    if (!isNonSteamShortcut(appid)) return;
    console.log("[NewFreedeck] game start appid=", appid);
    try {
      await cloudSaveSyncOnLaunch({ appid });
    } catch (e: any) {
      console.log("[NewFreedeck] sync_on_launch error:", e?.message);
    }
  };

  const syncEnd = async (appid: number) => {
    if (!isNonSteamShortcut(appid)) return;
    console.log("[NewFreedeck] game end appid=", appid);
    try {
      await cloudSaveSyncOnExit({ appid });
    } catch (e: any) {
      console.log("[NewFreedeck] sync_on_exit error:", e?.message);
    }
  };

  // 方式 A: RegisterForGameActionStart/End (优先)
  if (typeof apps?.RegisterForGameActionStart === "function") {
    try {
      registrations.push(
        apps.RegisterForGameActionStart((...args: any[]) => {
          const appid = typeof args[0] === "number" ? args[0] : (args[1] as any);
          if (typeof appid === "number") syncStart(appid);
        }) || {},
      );
      registrations.push(
        apps.RegisterForGameActionEnd((...args: any[]) => {
          const appid = typeof args[0] === "number" ? args[0] : (args[1] as any);
          if (typeof appid === "number") syncEnd(appid);
        }) || {},
      );
      console.log("[NewFreedeck] game lifecycle: RegisterForGameActionStart/End installed");
      return { unregister: () => registrations.forEach(r => r?.unregister?.()) };
    } catch {}
  }

  // 方式 B: 轮询 GetRunningAppIDs (降级)
  console.log("[NewFreedeck] game lifecycle: falling back to polling");
  const poll = async () => {
    try {
      const ids: number[] = await (apps?.GetRunningAppIDs?.() || []);
      const current = new Set(ids.filter(isNonSteamShortcut));
      for (const id of current) { if (!runningSet.has(id)) syncStart(id); }
      for (const id of runningSet) { if (!current.has(id)) syncEnd(id); }
      runningSet.clear();
      for (const id of current) runningSet.add(id);
    } catch {}
  };

  pollTimer = setInterval(poll, POLL_INTERVAL_MS);
  return { unregister: () => { if (pollTimer) clearInterval(pollTimer); } };
}

/* ── 插件入口 ──────────────────────────── */

export default definePlugin(() => {
  const registrations: Array<{ unregister: () => void }> = [];
  let lifecycleReg: { unregister: () => void } | null = null;

  // 注册游戏生命周期检测（retry 容错，Steam 模块可能未就绪）
  const tryInstall = () => {
    if (lifecycleReg) return;
    try {
      lifecycleReg = setupGameLifecycle();
      registrations.push(lifecycleReg);
    } catch {
      setTimeout(tryInstall, 5000);
    }
  };
  tryInstall();

  // 路由注册
  routerHook.addRoute(HOME_ROUTE, HomePanel);
  routerHook.addRoute(LOGIN_ROUTE, QrLoginPage);
  routerHook.addRoute(LIBRARY_ROUTE, LibraryPage);
  routerHook.addRoute(SETTINGS_ROUTE, SettingsPage);
  routerHook.addRoute(EMU_MANAGER_ROUTE, EmuManagerPage);
  routerHook.addRoute(SHOWCASE_ROUTE, DeckyUiShowcase);

  return {
    name: "NewFreedeck",
    title: "NewFreedeck",
    content: <HomePanel />,
    icon: null as any,
    onDismount() {
      registrations.forEach((r) => r.unregister());
      routerHook.removeRoute(HOME_ROUTE);
      routerHook.removeRoute(LOGIN_ROUTE);
      routerHook.removeRoute(LIBRARY_ROUTE);
      routerHook.removeRoute(SETTINGS_ROUTE);
      routerHook.removeRoute(EMU_MANAGER_ROUTE);
      routerHook.removeRoute(SHOWCASE_ROUTE);
    },
  };
});
