/* ═══════════════════════════════════════════
   NewFreedeck — 主面板（快捷操作 / 游戏预览 / 关于）
   ═══════════════════════════════════════════ */

import {
  ButtonItem,
  PanelSection,
  PanelSectionRow,
  Navigation,
  Router,
  Tabs,
  Focusable,
  ConfirmDialog,
} from "@decky/ui";
import { useState, useEffect, useCallback } from "react";
import { HOME_PANEL_CSS } from "./styles";
import { LOGIN_ROUTE, LIBRARY_ROUTE, SETTINGS_ROUTE } from "./routes";
import { DownloadStack } from "./library/DownloadStack";
import { onDownloadChange, getDownloadQueue } from "./downloadStore";
import type { DownloadTask, InstalledGame } from "./api";
import { useLoginState } from "./loginState";
import { getDownloadPoller } from "./downloadPoller";
import { listInstalledGames, uninstallGame } from "./api";
import { getLibraryFocusClassName } from "./library/GameCard";

const HOME_TABS = [
  { id: "actions", title: "快捷操作" },
  { id: "preview", title: "游戏预览" },
  { id: "about",   title: "关于"     },
];

const HOME_LOGIN_NOTICE =
  "登录后可使用天翼云盘分享解析、文件转存与下载等功能 · 天翼云盘为个人云存储服务 · 请遵守相关法律法规";

/* ── 快捷操作 ───────────────────────────── */

function ActionsTab({ login, loading, onLogin, dlQueue }: {
  login: any; loading: boolean; onLogin: () => void;
  dlQueue: DownloadTask[];
}) {
  const loggedIn = login?.logged_in ?? false;
  const accountName = login?.account_name || "";
  const loginText = loading
    ? "正在同步登录状态..."
    : (loggedIn ? `已登录：${accountName}` : "未登录");

  return (
    <div className="freedeck-home-tab-pane">
      {/* 登录状态 */}
      <div style={{ padding: "0 16px", fontSize: 12, color: "#8b929a", lineHeight: 1.4 }}>
        {loginText}
      </div>
      {/* 跑马灯 */}
      <div className="freedeck-home-login-notice" aria-label={HOME_LOGIN_NOTICE} title={HOME_LOGIN_NOTICE}
        style={{ padding: "2px 16px 0" }}>
        <div className="freedeck-home-login-notice-track">
          <span className="freedeck-home-login-notice-text">{HOME_LOGIN_NOTICE}</span>
          <span className="freedeck-home-login-notice-text" aria-hidden="true">{HOME_LOGIN_NOTICE}</span>
        </div>
      </div>
      {/* 下载列表 */}
      <DownloadStack tasks={dlQueue} />
      {/* 按钮区 */}
      <div style={{ padding: "0 16px" }}>
        {!loading && !loggedIn && (
          <div style={{ marginBottom: 4 }}>
            <ButtonItem layout="below" onClick={onLogin}>登录</ButtonItem>
          </div>
        )}
        <div style={{ marginBottom: 4 }}>
          <ButtonItem layout="below" onClick={() => { try { Navigation.Navigate(LIBRARY_ROUTE); } catch {} }}>游戏库</ButtonItem>
        </div>
        <div style={{ marginBottom: 4 }}>
          <ButtonItem layout="below" onClick={() => { try { Navigation.Navigate(SETTINGS_ROUTE); } catch {} }}>设置</ButtonItem>
        </div>
      </div>
    </div>
  );
}

/* ── 游戏预览 ───────────────────────────── */

function formatGameSize(bytes: number): string {
  if (!bytes || bytes <= 0) return "未知";
  const units = ["B", "KB", "MB", "GB", "TB"];
  let i = 0, v = bytes;
  while (v >= 1024 && i < units.length - 1) { v /= 1024; i++; }
  return `${v.toFixed(i > 0 ? 1 : 0)} ${units[i]}`;
}

function PreviewTab() {
  const [games, setGames] = useState<InstalledGame[]>([]);
  const [loading, setLoading] = useState(true);
  const [uninstallTarget, setUninstallTarget] = useState<InstalledGame | null>(null);

  const loadGames = useCallback(async () => {
    setLoading(true);
    try {
      const r = await listInstalledGames();
      setGames(r?.data?.games ?? []);
    } catch {} finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { loadGames(); }, [loadGames]);

  const handleLaunch = (steam_appid: number) => {
    const SC = (window as any).SteamClient;
    if (SC?.Apps?.RunGame) {
      SC.Apps.RunGame(steam_appid, "");
    }
  };

  const handleUninstall = async () => {
    if (!uninstallTarget) return;
    const gameId = uninstallTarget.game_id;
    const appid = uninstallTarget.steam_appid;
    setUninstallTarget(null);

    const SC = (window as any).SteamClient;
    if (appid && SC?.Apps?.RemoveShortcut) {
      try { await SC.Apps.RemoveShortcut(appid); } catch {}
    }

    await uninstallGame({ game_id: gameId });
    loadGames();
  };

  if (loading) {
    return (
      <div className="freedeck-home-tab-pane">
        <div style={{ padding: "40px 16px", color: "#8b929a", fontSize: 13, textAlign: "center" }}>
          加载中...
        </div>
      </div>
    );
  }

  if (games.length === 0) {
    return (
      <div className="freedeck-home-tab-pane">
        <div style={{ padding: "40px 16px", color: "#8b929a", fontSize: 13, textAlign: "center" }}>
          还没有已安装游戏 · 在游戏库中安装游戏后将在此处显示
        </div>
      </div>
    );
  }

  return (
    <div className="freedeck-home-tab-pane">
      <style>{`
        .nf-installed-card{position:relative;background:transparent}
        .nf-installed-card:focus{outline:none}
      `}</style>
      <div style={{ padding: "0 16px 16px", overflow: "visible" }}>
        {games.map((game) => (
          <Focusable
            key={game.game_id}
            className="nf-installed-card"
            focusClassName={getLibraryFocusClassName() || undefined}
            onOKButton={() => handleLaunch(game.steam_appid)}
            onOKActionDescription="启动游戏"
            onSecondaryButton={() => setUninstallTarget(game)}
            onSecondaryActionDescription="卸载游戏"
            style={{
              display: "flex", alignItems: "flex-start",
              padding: 12,
              marginBottom: 8,
              width: "100%", boxSizing: "border-box",
            } as any}
          >
            <div style={{
              flexShrink: 0,
              width: 60, height: 90,
              borderRadius: 4,
              overflow: "hidden",
              background: "rgba(255,255,255,0.04)",
            }}>
              {game.cover_url ? (
                <img src={game.cover_url} style={{ width: "100%", height: "100%", objectFit: "cover" }} alt="" />
              ) : (
                <div style={{
                  width: "100%", height: "100%", display: "flex",
                  alignItems: "center", justifyContent: "center",
                  fontSize: 20, color: "rgba(255,255,255,0.12)",
                }}>{game.title.charAt(0) || "?"}</div>
              )}
            </div>
            <div style={{ flex: 1, minWidth: 0, paddingLeft: 12, paddingTop: 8 }}>
              <div style={{
                fontSize: 14, fontWeight: 600, color: "#f4f8fc",
                whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis",
              }}>{game.title}</div>
              <div style={{ fontSize: 12, color: "#8b929a", marginTop: 4 }}>
                占用 {formatGameSize(game.size_bytes)}
              </div>
            </div>
          </Focusable>
        ))}
      </div>

      {uninstallTarget && (
        <ConfirmDialog
          strTitle="确认卸载"
          strDescription={`确定要卸载 ${uninstallTarget.title} 吗？这将删除快捷方式和所有游戏文件。`}
          strOKButtonText="确定卸载"
          strCancelButtonText="取消"
          onOK={handleUninstall}
          onCancel={() => setUninstallTarget(null)}
        />
      )}
    </div>
  );
}

/* ── 关于 ───────────────────────────────── */

function AboutTab() {
  return (
    <div className="freedeck-home-tab-pane">
      <PanelSection title="关于">
        <PanelSectionRow>
          <div style={{ color: "#8b929a", fontSize: 12, padding: "8px 0" }}>仅限研发测试，禁止商业获利。</div>
        </PanelSectionRow>
      </PanelSection>
    </div>
  );
}

/* ── 主面板入口 ─────────────────────────── */

const TAB_ORDER = ["actions", "preview", "about"];

export default function HomePanel() {
  const [tab, setTab] = useState("actions");
  const { login, loading } = useLoginState({ validateOnMount: true, refreshOnVisible: true });
  const [dlQueue, setDlQueue] = useState<DownloadTask[]>(getDownloadQueue);

  useEffect(() => {
    return onDownloadChange(setDlQueue);
  }, []);

  useEffect(() => {
    getDownloadPoller().resumeAll();
  }, []);

  // L1/R1 切换 Tab
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      const isL1 = e.key === "PageUp" || e.key === "[" || e.code === "BracketLeft";
      const isR1 = e.key === "PageDown" || e.key === "]" || e.code === "BracketRight";
      if (!isL1 && !isR1) return;
      const idx = TAB_ORDER.indexOf(tab);
      if (isL1 && idx > 0) { setTab(TAB_ORDER[idx - 1]); e.preventDefault(); }
      if (isR1 && idx < TAB_ORDER.length - 1) { setTab(TAB_ORDER[idx + 1]); e.preventDefault(); }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [tab]);

  const onLogin = () => {
    try { Router.CloseSideMenus?.(); } catch {}
    try { Navigation.Navigate(LOGIN_ROUTE); } catch {}
  };

  return (
    <div className="freedeck-home-root">
      <style>{HOME_PANEL_CSS}</style>
      <Tabs tabs={HOME_TABS} activeTab={tab} onShowTab={(id: string) => setTab(id)} autoFocusContents={false} />
      <div style={{ paddingTop: 12 }}>
        {tab === "actions" ? (
          <ActionsTab
            login={login} loading={loading} onLogin={onLogin}
            dlQueue={dlQueue}
          />
        ) : null}
        {tab === "preview" ? <PreviewTab /> : null}
        {tab === "about"   ? <AboutTab />   : null}
      </div>
    </div>
  );
}
