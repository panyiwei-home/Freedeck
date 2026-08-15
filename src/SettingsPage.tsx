/* ═══════════════════════════════════════════
   NewFreedeck — 设置页（SDH-GameSync 风格 SidebarNavigation）
   ═══════════════════════════════════════════ */

import {
  ButtonItem,
  PanelSection,
  PanelSectionRow,
  ToggleField,
  SidebarNavigation,
  Field,
  Focusable,
  DialogButton,
} from "@decky/ui";
import { toaster, openFilePicker, FileSelectionType } from "@decky/api";
import { useState, useEffect, useCallback, useRef } from "react";
import type { ReactNode, CSSProperties, ComponentType } from "react";
import { IoMdRefresh } from "react-icons/io";
import {
  getSettings, setSettings as saveSettingsApi,
  listMediaMounts, clearLogin,
  emulatorList, emulatorDownload, emulatorLaunchAppimage,
  downloadPoll,
  cloudSaveUpload, cloudSaveDownload, cloudSaveHistory,
} from "./api";
import type { EmulatorInfo, DownloadTask, LoginState, CloudSaveRecord } from "./api";
import { upsertDownload } from "./downloadStore";
import { getDownloadPoller } from "./downloadPoller";
import type { SettingsData, StorageMount } from "./api";
import { setLoginState, useLoginState } from "./loginState";
import { readLog } from "./api";

/* ── 工具函数 ────────────────────────── */

async function pickFolder(startPath: string): Promise<string> {
  const base = String(startPath || "/home/deck").trim() || "/home/deck";
  const result = await openFilePicker(FileSelectionType.FOLDER, base, false, true);
  return String(result?.realpath || result?.path || "").trim();
}

function formatFree(bytes: number): string {
  if (!bytes || bytes <= 0) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB"];
  let i = 0, v = bytes;
  while (v >= 1024 && i < units.length - 1) { v /= 1024; i++; }
  return `${v.toFixed(i > 0 ? 1 : 0)} ${units[i]}`;
}

/* ── 布局 helper（1:1 复刻 SDH-GameSync）── */

function container({ title, description, titleItem, children }: {
  title: ReactNode;
  description?: ReactNode;
  titleItem?: ReactNode;
  children: ReactNode;
}) {
  return (
    <>
      <Field
        label={title}
        description={description != null ? <small>{description}</small> : undefined}
        highlightOnFocus={false}
      >
        <Focusable style={{ display: "flex" }}>
          <div style={{ display: "flex", gap: "8px", alignItems: "center" }}>
            {titleItem}
          </div>
        </Focusable>
      </Field>
      <div style={{ overflowY: "auto" }}>{children}</div>
    </>
  );
}

function pageView({ title, description, titleItem, fullPage = true, children }: {
  title: ReactNode;
  description?: ReactNode;
  titleItem?: ReactNode;
  fullPage?: boolean;
  children: ReactNode;
}) {
  const baseStyles: CSSProperties = {
    height: "100%",
    maxHeight: "calc(100vh - 40px)",
    boxSizing: "border-box",
    display: "flex",
    flexDirection: "column",
  };
  const styles = fullPage
    ? { ...baseStyles, margin: "40px 0" }
    : { ...baseStyles, width: "100%", marginTop: "-24px", marginLeft: "-24px", position: "fixed" as const };
  const HeadingTag = fullPage ? 'h1' : 'h2';
  const headingStyle: CSSProperties = { margin: "0", minHeight: "28px", alignContent: "center" };

  return (
    <div style={styles}>
      {container({
        title: <HeadingTag style={headingStyle}>{title}</HeadingTag>,
        description,
        titleItem,
        children,
      })}
    </div>
  );
}

function iconButton({ icon: Icon, onOKActionDescription, disabled, onClick }: {
  icon: ComponentType<{ style?: CSSProperties }>;
  onOKActionDescription: string;
  disabled?: boolean;
  onClick: () => void;
}) {
  return (
    <DialogButton
      disabled={disabled}
      onOKActionDescription={onOKActionDescription}
      onClick={onClick}
      style={{ height: "28px", width: "28px", minWidth: 0, padding: "10px 6px" }}
    >
      <Icon style={{ marginTop: "-4px", display: "block" }} />
    </DialogButton>
  );
}

/* ── 默认值 ──────────────────────────── */

const EMPTY_SETTINGS: SettingsData = {
  download_path: "",
  install_path: "",
  auto_delete_after_install: false,
  auto_suspend_on_download: false,
  show_playtime_widget: true,
  show_freedeck_library_entry: true,
  screen_off_auto_suspend: false,
  cloud_save_auto_upload: false,
  cloud_save_provider: "",
};

/* ── 基础页 ────────────────────────────── */

function BasicSettings({ settings, saving, mounts, checkingMediaMounts,
  onPickDownloadDir, onUseDownloadDirFromMedia,
  onPickInstallDir, onUseInstallDirFromMedia,
  savePatch,
}: {
  settings: SettingsData;
  saving: boolean;
  mounts: StorageMount[];
  checkingMediaMounts: boolean;
  onPickDownloadDir: () => void;
  onUseDownloadDirFromMedia: () => void;
  onPickInstallDir: () => void;
  onUseInstallDirFromMedia: () => void;
  savePatch: (patch: Partial<SettingsData>, toast?: string) => Promise<void>;
}) {
  return (
    <pageView title="基础" fullPage={false}>
      <PanelSection title="下载目录">
        <PanelSectionRow>
          <div style={{ fontSize: 12, lineHeight: 1.5 }}>{settings.download_path || "未设置"}</div>
        </PanelSectionRow>
        <PanelSectionRow>
          <ButtonItem layout="below" onClick={onPickDownloadDir} disabled={saving}>
            选择下载目录
          </ButtonItem>
        </PanelSectionRow>
        <PanelSectionRow>
          <ButtonItem onClick={onUseDownloadDirFromMedia} disabled={saving || checkingMediaMounts}>
            {checkingMediaMounts ? "检测中..." : "内存卡"}
          </ButtonItem>
        </PanelSectionRow>
      </PanelSection>

      <PanelSection title="安装目录">
        <PanelSectionRow>
          <div style={{ fontSize: 12, lineHeight: 1.5 }}>{settings.install_path || "未设置"}</div>
        </PanelSectionRow>
        <PanelSectionRow>
          <ButtonItem layout="below" onClick={onPickInstallDir} disabled={saving}>
            选择安装目录
          </ButtonItem>
        </PanelSectionRow>
        <PanelSectionRow>
          <ButtonItem onClick={onUseInstallDirFromMedia} disabled={saving || checkingMediaMounts}>
            {checkingMediaMounts ? "检测中..." : "内存卡"}
          </ButtonItem>
        </PanelSectionRow>
      </PanelSection>

      <PanelSection title="安装行为">
        <PanelSectionRow>
          <ToggleField
            label="安装后自动删除压缩包"
            description="仅在自动安装成功后生效"
            checked={Boolean(settings.auto_delete_after_install)}
            onChange={(v: boolean) => savePatch({ auto_delete_after_install: v })}
            disabled={saving}
          />
        </PanelSectionRow>
      </PanelSection>
    </pageView>
  );
}

/* ── 运行页 ────────────────────────────── */

function RuntimeSettings({ settings, saving, savePatch }: {
  settings: SettingsData;
  saving: boolean;
  savePatch: (patch: Partial<SettingsData>, toast?: string) => Promise<void>;
}) {
  return (
    <pageView title="运行" fullPage={false}>
      <PanelSection title="库主页">
        <PanelSectionRow>
          <ToggleField
            label="显示时长"
            description="在 Steam 库主页显示：已玩时长 / 主线时长 / 总时长"
            checked={Boolean(settings.show_playtime_widget)}
            onChange={(v: boolean) => savePatch({ show_playtime_widget: v })}
            disabled={saving}
          />
        </PanelSectionRow>
        <PanelSectionRow>
          <ToggleField
            label="显示 Freedeck 游戏库入口"
            description="关闭后，Steam 按钮菜单中不显示 Freedeck 游戏库，只能通过主页的游戏列表进入"
            checked={Boolean(settings.show_freedeck_library_entry)}
            onChange={(v: boolean) =>
              savePatch({ show_freedeck_library_entry: v },
                v ? "已显示 Steam 菜单中的 Freedeck 游戏库入口" : "已隐藏 Steam 菜单中的 Freedeck 游戏库入口")
            }
            disabled={saving}
          />
        </PanelSectionRow>
      </PanelSection>

      <PanelSection title="启动项">
        <PanelSectionRow>
          <ToggleField
            label="复古模拟器 CRT 滤镜"
            description="影响 Switch / PS1 / PS2 / PS3 / PSP / 3DS / GBA / NDS / MD / FC 导入到 Steam 的快捷方式；关闭后新导入不再套用 CRT，旧 wrapper 启动时也会自动跳过滤镜"
            checked={true}
            onChange={() => {}}
            disabled={true}
          />
        </PanelSectionRow>
        <PanelSectionRow>
          <ToggleField
            label="自动添加小黄鸭（LSFG）"
            checked={false}
            onChange={() => {}}
            disabled={true}
          />
        </PanelSectionRow>
      </PanelSection>
    </pageView>
  );
}

/* ── 账号页 ────────────────────────────── */

function AccountSettings({ login, loginLoading, clearingLogin, onClearLogin, saving }: {
  login: LoginState | null;
  loginLoading: boolean;
  clearingLogin: boolean;
  onClearLogin: () => void;
  saving: boolean;
}) {
  return (
    <pageView title="账号" fullPage={false}>
      <PanelSection title="天翼云盘">
        <PanelSectionRow>
          <div style={{ fontSize: 12, lineHeight: 1.5 }}>
            {loginLoading
              ? "正在检查登录状态..."
              : login?.logged_in
                ? `当前已登录：${login.account_name || "未知账号"}`
                : "当前未登录"}
          </div>
        </PanelSectionRow>
        <PanelSectionRow>
          <ButtonItem layout="below" onClick={onClearLogin} disabled={clearingLogin || saving || !login?.logged_in}>
            {clearingLogin ? "注销中..." : "注销天翼云盘账号"}
          </ButtonItem>
        </PanelSectionRow>
      </PanelSection>
    </pageView>
  );
}

/* ═══════════════════════════════════════════
   [测试] 下载列表 UI 测试面板 — 用完即删
   删除时可以整个 TestDownloadPanel 函数连同
   SidebarNavigation 里对应的 page 条目一起删
   ═══════════════════════════════════════════ */

function TestDownloadPanel() {
  const createMockTask = (status: string) => {
    const taskId = `test-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
    const mock: any = {
      task_id: taskId,
      game_title: `测试游戏 - ${status}`,
      file_name: `test_game_${taskId.slice(0, 6)}.zip`,
      status,
      progress: status === "complete" ? 100 : status === "failed" ? 0 : Math.floor(Math.random() * 60) + 10,
      speed: status === "downloading" ? 1024000 + Math.floor(Math.random() * 5000000) : 0,
      file_size: 8589934592,
      game_id: "test-game-001",
      cover_url: "",
      extract_status: undefined,
      import_status: undefined,
      error: status === "failed" ? "模拟下载失败：网络超时" : "",
    };
    upsertDownload(mock);
    getDownloadPoller().addTask(taskId);
  };

  return (
    <pageView title="[测试] 下载列表" fullPage={false}>
      <PanelSection title="创建模拟下载任务（测试用，用完即删）">
        <PanelSectionRow>
          <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
            <ButtonItem layout="below" onClick={() => createMockTask("downloading")}>
              模拟下载中任务
            </ButtonItem>
            <ButtonItem layout="below" onClick={() => createMockTask("paused")}>
              模拟暂停任务
            </ButtonItem>
            <ButtonItem layout="below" onClick={() => createMockTask("failed")}>
              模拟失败任务
            </ButtonItem>
            <ButtonItem layout="below" onClick={() => createMockTask("complete")}>
              模拟完成任务
            </ButtonItem>
          </div>
        </PanelSectionRow>
      </PanelSection>
    </pageView>
  );
}

/* ═══════════════════════════════════════════
   组件: 设置页
   ═══════════════════════════════════════════ */

/* ── 日志查看器 ──────────────────────── */

function LogViewer() {
  const [log, setLog] = useState("");
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState("");
  const [autoRefresh, setAutoRefresh] = useState(false);
  const scrollRef = useRef<HTMLDivElement>(null);
  const timerRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const fetchLog = useCallback(async (silent?: boolean) => {
    if (!silent) setLoading(true); else setRefreshing(true);
    setError("");
    try {
      const r = await readLog({ lines: 300 });
      if (r?.data?.log != null) setLog(r.data.log);
      else setError(r?.message || "无法获取日志");
    } catch {
      setError("读取日志失败");
    }
    setLoading(false); setRefreshing(false);
  }, []);

  useEffect(() => { fetchLog(); }, [fetchLog]);

  // 自动刷新
  useEffect(() => {
    if (autoRefresh) {
      timerRef.current = setInterval(() => fetchLog(true), 5000);
    } else {
      if (timerRef.current) { clearInterval(timerRef.current); timerRef.current = null; }
    }
    return () => { if (timerRef.current) clearInterval(timerRef.current); };
  }, [autoRefresh, fetchLog]);

  // 滚动到底部
  useEffect(() => {
    if (scrollRef.current) scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
  }, [log]);

  return (
    <pageView title="日志" fullPage={false}
      titleItem={
        <div style={{ display: "flex", gap: 6, alignItems: "center" }}>
          <ToggleField
            label=""
            checked={autoRefresh}
            onChange={(v: boolean) => setAutoRefresh(v)}
            description={autoRefresh ? "自动刷新 (5s)" : "手动刷新"}
          />
          <iconButton icon={IoMdRefresh} onOKActionDescription="刷新日志"
            disabled={refreshing} onClick={() => fetchLog(true)} />
        </div>
      }>
      <PanelSection>
        <PanelSectionRow>
          <div ref={scrollRef} style={{
            height: "calc(100vh - 180px)", overflowY: "auto",
            background: "rgba(0,0,0,0.3)", borderRadius: 8, padding: 12,
            fontFamily: "monospace", fontSize: 11, lineHeight: 1.5,
            color: "#8b929a", whiteSpace: "pre-wrap", wordBreak: "break-all",
          }}>
            {loading ? "加载中..." : error ? <span style={{ color: "#e74a2f" }}>{error}</span> : log || "暂无日志"}
          </div>
        </PanelSectionRow>
      </PanelSection>
    </pageView>
  );
}

export default function SettingsPage() {
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [settings, setSettings] = useState<SettingsData>(EMPTY_SETTINGS);
  const [mounts, setMounts] = useState<StorageMount[]>([]);
  const [checkingMediaMounts, setCheckingMediaMounts] = useState(false);

  // 登录态
  const { login, loading: loginLoading } = useLoginState({ validateOnMount: true });
  const [clearingLogin, setClearingLogin] = useState(false);

  // ── 加载设置 ──
  const loadSettings = useCallback(async () => {
    const r = await getSettings();
    if (r?.data) {
      const d = r.data;
      setSettings({ ...EMPTY_SETTINGS, ...d } as SettingsData);
    }
    setLoading(false);
  }, []);

  // ── 加载挂载点 ──
  const loadMounts = useCallback(async () => {
    setCheckingMediaMounts(true);
    try {
      const r = await listMediaMounts();
      setMounts(r?.data?.mounts ?? []);
    } catch {} finally {
      setCheckingMediaMounts(false);
    }
  }, []);

  useEffect(() => { loadSettings(); loadMounts(); }, []);

  // ── 保存 ──
  const savePatch = useCallback(async (patch: Partial<SettingsData>, toast?: string) => {
    setSaving(true);
    try {
      const r = await saveSettingsApi(patch);
      if (r?.data) setSettings({ ...EMPTY_SETTINGS, ...r.data } as SettingsData);
      if (toast) toaster.toast({ title: "设置已保存", body: toast, duration: 2000 });
    } catch {} finally {
      setSaving(false);
    }
  }, []);

  // ── 内存卡挂载 ──
  const fetchMediaMounts = useCallback(async (): Promise<StorageMount[] | null> => {
    if (checkingMediaMounts) return null;
    setCheckingMediaMounts(true);
    try {
      const r = await listMediaMounts();
      const mounts: StorageMount[] = Array.isArray(r?.data?.mounts) ? r.data.mounts : [];
      return mounts
        .map(m => ({ ...m, path: String(m.path || "").trim() }))
        .filter(m => Boolean(m.path));
    } finally {
      setCheckingMediaMounts(false);
    }
  }, [checkingMediaMounts]);

  const onPickDownloadDir = useCallback(async () => {
    try {
      const selected = await pickFolder(settings.download_path || "/home/deck");
      if (!selected) return;
      await savePatch({ download_path: selected }, `下载目录已更新：${selected}`);
    } catch (error) {
      toaster.toast({ title: "Freedeck", body: `选择下载目录失败：${error}` });
    }
  }, [savePatch, settings.download_path]);

  const onPickInstallDir = useCallback(async () => {
    try {
      const selected = await pickFolder(settings.install_path || settings.download_path || "/home/deck");
      if (!selected) return;
      await savePatch({ install_path: selected }, `安装目录已更新：${selected}`);
    } catch (error) {
      toaster.toast({ title: "Freedeck", body: `选择安装目录失败：${error}` });
    }
  }, [savePatch, settings.download_path, settings.install_path]);

  const onUseDownloadDirFromMedia = useCallback(async () => {
    try {
      const mounts = await fetchMediaMounts();
      if (!mounts) return;
      if (mounts.length <= 0) {
        toaster.toast({ title: "Freedeck", body: "未检测到外接设备（/run/media）" });
        return;
      }
      const m = mounts[0];
      const target = `${String(m.path || "").trim()}/Game`;
      await savePatch({ download_path: target }, `下载目录已更新：${target}`);
    } catch (error) {
      toaster.toast({ title: "Freedeck", body: `加载外接设备失败：${error}` });
    }
  }, [fetchMediaMounts, savePatch]);

  const onUseInstallDirFromMedia = useCallback(async () => {
    try {
      const mounts = await fetchMediaMounts();
      if (!mounts) return;
      if (mounts.length <= 0) {
        toaster.toast({ title: "Freedeck", body: "未检测到外接设备（/run/media）" });
        return;
      }
      const m = mounts[0];
      const target = `${String(m.path || "").trim()}/Game`;
      await savePatch({ install_path: target }, `安装目录已更新：${target}`);
    } catch (error) {
      toaster.toast({ title: "Freedeck", body: `加载外接设备失败：${error}` });
    }
  }, [fetchMediaMounts, savePatch]);

  // ── 清除登录态 ──
  const onClearLogin = useCallback(async () => {
    setClearingLogin(true);
    try {
      const r = await clearLogin();
      if (r?.data) setLoginState(r.data);
    } catch {} finally {
      setClearingLogin(false);
    }
  }, []);

  // ── 模拟器刷新 key ──
  const [emuRefreshKey, setEmuRefreshKey] = useState(0);

  if (loading) {
    return (
      <div style={{ display: "flex", justifyContent: "center", paddingTop: 80, paddingBottom: 60 }}>
        <div style={{ fontSize: 13, color: "#8b929a" }}>加载设置...</div>
      </div>
    );
  }

  const s = settings;

  return (
    <div style={{
      height: "100vh", display: "flex", flexDirection: "column",
      boxSizing: "border-box", overflow: "hidden",
    }}>
      <SidebarNavigation pages={[
        {
          title: "基础", hideTitle: true, visible: true,
          content: (
            <BasicSettings
              settings={s} saving={saving}
              mounts={mounts} checkingMediaMounts={checkingMediaMounts}
              onPickDownloadDir={onPickDownloadDir}
              onUseDownloadDirFromMedia={onUseDownloadDirFromMedia}
              onPickInstallDir={onPickInstallDir}
              onUseInstallDirFromMedia={onUseInstallDirFromMedia}
              savePatch={savePatch}
            />
          ),
        },
        {
          title: "运行", hideTitle: true, visible: true,
          content: (
            <RuntimeSettings settings={s} saving={saving} savePatch={savePatch} />
          ),
        },
        {
          title: "模拟器", hideTitle: true, visible: true,
          content: (
            <pageView title="模拟器" fullPage={false}
              titleItem={
                <iconButton icon={IoMdRefresh} onOKActionDescription="刷新模拟器列表"
                  onClick={() => setEmuRefreshKey(k => k + 1)} />
              }>
              <EmulatorManager key={emuRefreshKey} />
            </pageView>
          ),
        },
        {
          title: "账号", hideTitle: true, visible: true,
          content: (
            <AccountSettings
              login={login} loginLoading={loginLoading}
              clearingLogin={clearingLogin} onClearLogin={onClearLogin}
              saving={saving}
            />
          ),
        },
        {
          title: "云存档", hideTitle: true, visible: true,
          content: <CloudSaveSettings saving={saving} savePatch={savePatch} />,
        },
          {
          title: "日志", hideTitle: true, visible: true,
          content: <LogViewer />,
        },
        /* ═══════════════════════════════════════
           [测试] 下载列表 UI 测试 — 用完即删
           ═══════════════════════════════════════ */
        {
          title: "[测试]", hideTitle: true, visible: true,
          content: <TestDownloadPanel />,
        },
        {
          title: "UI 组件", hideTitle: true, visible: true,
          content: <UiShowcaseEntry />,
        },
      ]} />
    </div>
  );
}

/* ── 云存档组件 ─────────────────────── */

function formatDate(ts: string): string {
  if (!ts) return "";
  try {
    const d = new Date(ts);
    if (isNaN(d.getTime())) return ts.slice(0, 16);
    return d.toLocaleString();
  } catch {
    return ts.slice(0, 16);
  }
}

function CloudSaveSettings({ saving, savePatch }: {
  saving: boolean;
  savePatch: (patch: Partial<SettingsData>, toast?: string) => Promise<void>;
}) {
  const [uploading, setUploading] = useState(false);
  const [downloading, setDownloading] = useState(false);
  const [history, setHistory] = useState<CloudSaveRecord[]>([]);
  const [historyLoaded, setHistoryLoaded] = useState(false);
  const [lastMessage, setLastMessage] = useState("");
  const [autoSync, setAutoSync] = useState(false);

  const loadHistory = useCallback(async () => {
    try {
      const r = await cloudSaveHistory();
      setHistory(r?.data?.records ?? []);
    } catch {} finally {
      setHistoryLoaded(true);
    }
  }, []);

  useEffect(() => { loadHistory(); }, [loadHistory]);

  const onUpload = async () => {
    setUploading(true);
    setLastMessage("");
    try {
      const r = await cloudSaveUpload();
      if (r?.data) {
        const msg = r.data.message || `上传完成，共 ${r.data.count} 个存档包`;
        toaster.toast({ title: "云存档", body: msg, duration: 3000 });
        setLastMessage(msg);
        await loadHistory();
      } else {
        setLastMessage(r?.message || "上传失败");
        toaster.toast({ title: "云存档", body: r?.message || "上传失败", duration: 3000 });
      }
    } catch (e: any) {
      setLastMessage("上传失败: " + (e?.message || "网络错误"));
      toaster.toast({ title: "云存档", body: "上传失败", duration: 3000 });
    } finally { setUploading(false); }
  };

  const onDownload = async () => {
    setDownloading(true);
    setLastMessage("");
    try {
      const r = await cloudSaveDownload();
      if (r?.data) {
        const msg = r.data.message || `下载完成，共 ${r.data.count} 个存档包`;
        toaster.toast({ title: "云存档", body: msg, duration: 3000 });
        setLastMessage(msg);
        await loadHistory();
      } else {
        setLastMessage(r?.message || "下载失败");
        toaster.toast({ title: "云存档", body: r?.message || "下载失败", duration: 3000 });
      }
    } catch (e: any) {
      setLastMessage("下载失败: " + (e?.message || "网络错误"));
      toaster.toast({ title: "云存档", body: "下载失败", duration: 3000 });
    } finally { setDownloading(false); }
  };

  const onAutoSyncToggle = (v: boolean) => {
    setAutoSync(v);
    // 通知 syncTaskQueue 更新自动同步状态
    import("./syncQueue").then(({ syncTaskQueue }) => {
      syncTaskQueue.setAutoSyncEnabled(v);
    });
    toaster.toast({
      title: "云存档",
      body: v ? "已开启自动同步" : "已关闭自动同步",
      duration: 2000,
    });
  };

  return (
    <pageView title="云存档" fullPage={false}>
      <PanelSection title="自动同步">
        <PanelSectionRow>
          <ToggleField
            label="游戏启动/退出时自动同步存档"
            description="启用后，每次启动游戏时自动下载云端存档，退出时自动上传本地存档"
            checked={autoSync}
            onChange={onAutoSyncToggle}
            disabled={false}
          />
        </PanelSectionRow>
      </PanelSection>

      <PanelSection title="操作">
        <PanelSectionRow>
          <div style={{ display: "flex", gap: 8, width: "100%" }}>
            <div style={{ flex: 1 }}>
              <ButtonItem onClick={onUpload} disabled={uploading || downloading || saving}>
                {uploading ? "上传中..." : "上传云存档（备份）"}
              </ButtonItem>
            </div>
            <div style={{ flex: 1 }}>
              <ButtonItem onClick={onDownload} disabled={uploading || downloading || saving}>
                {downloading ? "下载中..." : "下载云存档（恢复）"}
              </ButtonItem>
            </div>
          </div>
        </PanelSectionRow>
        {lastMessage && (
          <PanelSectionRow>
            <div style={{ fontSize: 12, color: "#8b929a", lineHeight: 1.5 }}>{lastMessage}</div>
          </PanelSectionRow>
        )}
      </PanelSection>

      <PanelSection title="同步记录">
        {!historyLoaded ? (
          <PanelSectionRow>
            <div style={{ color: "#8b929a", fontSize: 12 }}>加载中...</div>
          </PanelSectionRow>
        ) : history.length === 0 ? (
          <PanelSectionRow>
            <div style={{ color: "#8b929a", fontSize: 12 }}>暂无同步记录</div>
          </PanelSectionRow>
        ) : (
          history.map(record => (
            <PanelSectionRow key={record.id}>
              <div style={{ display: "flex", justifyContent: "space-between", width: "100%", alignItems: "center" }}>
                <span style={{ fontSize: 12, flex: 1, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                  {record.file_name}
                </span>
                <span style={{
                  fontSize: 11,
                  color: record.direction === "upload" ? "#6fb7ff" : "#5a5",
                  margin: "0 12px",
                  whiteSpace: "nowrap",
                }}>
                  {record.direction === "upload" ? "↑ 上传" : "↓ 下载"}
                </span>
                <span style={{ fontSize: 11, color: "#8b929a", whiteSpace: "nowrap" }}>
                  {formatDate(record.timestamp)}
                </span>
              </div>
            </PanelSectionRow>
          ))
        )}
      </PanelSection>
    </pageView>
  );
}

/* ── 模拟器管理组件 ─────────────────────── */

function EmulatorManager() {
  const [emulators, setEmulators] = useState<EmulatorInfo[]>([]);
  const [emuLoading, setEmuLoading] = useState(true);
  const [emuError, setEmuError] = useState("");
  const [installing, setInstalling] = useState<Set<string>>(new Set());
  const [messages, setMessages] = useState<Record<string, string>>({});
  const [progress, setProgress] = useState<Record<string, number>>({});

  const loadEmulators = useCallback(async () => {
    setEmuLoading(true);
    setEmuError("");
    try {
      const r = await emulatorList();
      if (r?.data?.emulators) {
        setEmulators(r.data.emulators);
      } else {
        setEmuError(r?.message || "获取模拟器列表失败");
      }
    } catch (e: any) {
      setEmuError(e?.message || "网络异常，无法加载模拟器列表");
    } finally {
      setEmuLoading(false);
    }
  }, []);

  useEffect(() => {
    loadEmulators();
  }, [loadEmulators]);

  const handleInstall = useCallback(async (plat: string, display: string) => {
    setInstalling(prev => new Set(prev).add(plat));
    setMessages(prev => ({ ...prev, [plat]: "下载中..." }));
    setProgress(prev => ({ ...prev, [plat]: 0 }));

    try {
      const r = await emulatorDownload({ platform: plat });
      if (!r?.data?.task_id) {
        setMessages(prev => ({ ...prev, [plat]: r?.message || "启动下载失败" }));
        setInstalling(prev => { const n = new Set(prev); n.delete(plat); return n; });
        return;
      }

      const taskId = r.data.task_id;

      upsertDownload(r.data as any);
      getDownloadPoller().addTask(taskId);

      const dlPoll = setInterval(async () => {
        try {
          const s = await downloadPoll({ task_id: taskId });
          const t: DownloadTask | undefined = s?.data;
          if (!t) return;

          if (t.status === "failed") {
            clearInterval(dlPoll);
            setMessages(prev => ({ ...prev, [plat]: t.error || "下载失败" }));
            setInstalling(prev => { const n = new Set(prev); n.delete(plat); return n; });
            return;
          }

          const pct = Math.round(t.progress || 0);
          setProgress(prev => ({ ...prev, [plat]: pct }));

          if (t.extract_status === "running") {
            setMessages(prev => ({ ...prev, [plat]: `解压中 ${t.extract_progress || 0}%` }));
            return;
          }

          if (t.status === "downloading" || t.status === "paused") {
            setMessages(prev => ({ ...prev, [plat]: `下载中 ${pct}%` }));
            return;
          }

          if (t.import_status === "discovering" || t.import_status === "importing") {
            setMessages(prev => ({ ...prev, [plat]: "导入 Steam..." }));
            return;
          }

          if (t.import_status === "applying") {
            setMessages(prev => ({ ...prev, [plat]: "应用封面..." }));
            return;
          }

          if (t.status === "complete" && (!t.import_status || t.import_status === "complete")) {
            clearInterval(dlPoll);
            toaster.toast({ title: `${display} 安装完成`, body: "", duration: 2000 });
            setMessages(prev => ({ ...prev, [plat]: "" }));
            setProgress(prev => { const n = { ...prev }; delete n[plat]; return n; });
            setInstalling(prev => { const n = new Set(prev); n.delete(plat); return n; });
            const list = await emulatorList();
            if (list?.data?.emulators) setEmulators(list.data.emulators);
          }
        } catch {
          // 轮询错误，继续
        }
      }, 2000);

      setTimeout(() => {
        clearInterval(dlPoll);
        setInstalling(prev => { const n = new Set(prev); n.delete(plat); return n; });
      }, 1800000);

    } catch (e: any) {
      setMessages(prev => ({ ...prev, [plat]: e?.message || "下载失败" }));
      setInstalling(prev => { const n = new Set(prev); n.delete(plat); return n; });
    }
  }, []);

  const [opening, setOpening] = useState<Set<string>>(new Set());

  const handleOpenApp = useCallback(async (plat: string, display: string) => {
    setOpening(prev => new Set(prev).add(plat));
    try {
      const r = await emulatorLaunchAppimage({ platform: plat });
      if (!r?.data?.appimage) {
        toaster.toast({ title: r?.message || `未找到 ${display} AppImage`, body: "", duration: 3000 });
        return;
      }

      const { appimage, install_dir } = r.data;
      const appname = `${display} 模拟器`;

      // 创建临时 Steam 快捷方式（照搬 decky-desktop-apps）
      // 关键：AddShortcut 第二个参数传 appimage 路径，不是 appname
      const steamAppId = await (window as any).SteamClient.Apps.AddShortcut(appname, appimage, install_dir, "");
      const appStore = (window as any).appStore;

      // 等待 Steam 识别快捷方式
      const deadline = Date.now() + 3000;
      let gameInfo: any = null;
      while (Date.now() < deadline) {
        await new Promise(resolve => setTimeout(resolve, 100));
        gameInfo = appStore.GetAppOverviewByAppID(steamAppId);
        if (gameInfo != null) break;
      }

      if (!gameInfo) {
        toaster.toast({ title: "启动失败", body: "Steam 未能识别快捷方式", duration: 3000 });
        await (window as any).SteamClient.Apps.RemoveShortcut(steamAppId).catch(() => {});
        return;
      }

      // 注册生命周期监听 — 关闭后自动清理
      let resolved = false;
      const sub = (window as any).SteamClient.GameSessions.RegisterForAppLifetimeNotifications((e: any) => {
        if (e.unAppID === steamAppId) {
          if (!resolved && e.bRunning) resolved = true;
          if (!e.bRunning) {
            sub?.unregister();
            (window as any).SteamClient.Apps.RemoveShortcut(steamAppId).catch(() => {});
          }
        }
      });

      // 启动
      (window as any).SteamClient.Apps.RunGame(gameInfo.m_gameid, "", -1, 100);

      // 超时保护
      setTimeout(() => {
        if (!resolved) {
          sub?.unregister();
          (window as any).SteamClient.Apps.RemoveShortcut(steamAppId).catch(() => {});
        }
      }, 5000);

    } catch (e: any) {
      toaster.toast({ title: "启动失败", body: e?.message || "未知错误", duration: 3000 });
    } finally {
      setOpening(prev => { const n = new Set(prev); n.delete(plat); return n; });
    }
  }, []);

  if (emuLoading) {
    return (
      <PanelSection title="模拟器管理">
        <PanelSectionRow>
          <div style={{ fontSize: 12, color: "#8b929a", padding: "4px 0" }}>加载中...</div>
        </PanelSectionRow>
      </PanelSection>
    );
  }

  if (emuError) {
    return (
      <PanelSection title="模拟器管理">
        <PanelSectionRow>
          <div style={{ fontSize: 12, color: "#e74a2f", padding: "4px 0" }}>{emuError}</div>
        </PanelSectionRow>
        <PanelSectionRow>
          <ButtonItem layout="below" onClick={loadEmulators}>重试</ButtonItem>
        </PanelSectionRow>
      </PanelSection>
    );
  }

  if (emulators.length === 0) {
    return (
      <PanelSection title="模拟器管理">
        <PanelSectionRow>
          <div style={{ fontSize: 12, color: "#8b929a", padding: "4px 0" }}>暂无可用模拟器</div>
        </PanelSectionRow>
      </PanelSection>
    );
  }

  return (
    <PanelSection title="模拟器管理">
      <div style={{
        display: "grid",
        gridTemplateColumns: "repeat(auto-fill, minmax(160px, 1fr))",
        gap: 12,
        padding: "4px 0",
      }}>
        {emulators.map(emu => {
          const busy = installing.has(emu.platform);
          const msg = messages[emu.platform] || "";
          const pct = progress[emu.platform];
          const iconPath = emu.icon ? `http://127.0.0.1:20065/emulator_icons/${emu.icon}` : "";

          return (
            <div key={emu.platform} style={{
              background: "rgba(255,255,255,0.03)",
              border: emu.installed ? "1px solid rgba(90,170,90,0.25)" : "1px solid rgba(255,255,255,0.06)",
              borderRadius: 8,
              padding: "14px 12px",
              display: "flex",
              flexDirection: "column",
              alignItems: "center",
              gap: 10,
              minWidth: 0,
              minHeight: 220,
            }}>
              {iconPath && (
                <img src={iconPath} alt={emu.display}
                  style={{ width: 48, height: 48, objectFit: "contain" }} />
              )}
              <span style={{ fontSize: 13, fontWeight: 700, color: "#cac4d0" }}>{emu.display}</span>

              {emu.installed && !busy && (
                <span style={{ fontSize: 9, color: "#5a5", textTransform: "uppercase", letterSpacing: "0.06em" }}>已安装</span>
              )}

              {msg && (
                <span style={{ fontSize: 10, color: "#6b727a", textAlign: "center" }}>{msg}</span>
              )}

              {busy && pct != null && pct > 0 && (
                <div style={{
                  width: "100%", height: 4, background: "rgba(255,255,255,0.08)", borderRadius: 2, overflow: "hidden",
                }}>
                  <div style={{
                    height: "100%", width: `${Math.min(100, pct)}%`,
                    background: "linear-gradient(90deg, #4a9eff, #6fb7ff)",
                    borderRadius: 2, transition: "width 0.5s ease",
                  }} />
                </div>
              )}

              {/* 按钮区 — 竖向排布 */}
              <div style={{ width: "100%", display: "flex", gap: 6, marginTop: "auto" }}>
                <div style={{ flex: 1, display: "flex", flexDirection: "column", gap: 6 }}>
                  <ButtonItem onClick={() => handleInstall(emu.platform, emu.display)} disabled={busy}>
                    {busy ? (msg || "处理中...") : emu.installed ? "重新安装" : emu.has_download ? "安装" : "无源"}
                  </ButtonItem>
                  {emu.installed && !busy && (
                    <ButtonItem
                      onClick={() => handleOpenApp(emu.platform, emu.display)}
                      disabled={opening.has(emu.platform)}
                    >
                      {opening.has(emu.platform) ? "启动中..." : "配置"}
                    </ButtonItem>
                  )}
                </div>
                <div style={{ flex: 1 }} />
              </div>
            </div>
          );
        })}
      </div>
    </PanelSection>
  );
}

/* ── UI 组件展示入口 ──────────────────── */

import { Navigation } from "@decky/ui";
import { SHOWCASE_ROUTE } from "./routes";

function UiShowcaseEntry() {
  return (
    <pageView title="UI 组件展示" fullPage={false}>
      <PanelSection title="开发者工具">
        <PanelSectionRow>
          <ButtonItem
            onClick={() => { try { Navigation.Navigate(SHOWCASE_ROUTE); } catch {} }}
            onOKActionDescription="打开"
          >
            打开 @decky/ui 组件展示页
          </ButtonItem>
        </PanelSectionRow>
        <PanelSectionRow>
          <div style={{ fontSize: 12, color: "#8b929a", lineHeight: 1.5 }}>
            展示所有 @decky/ui 组件（PanelSection, ButtonItem, ToggleField, Field, Focusable,
            SliderField, TextField, Spinner, ProgressBar, Dropdown, DialogCheckbox, Tabs,
            Modal, ContextMenu, ScrollPanel, SidebarNavigation 等），方便 UI 开发时参考。
          </div>
        </PanelSectionRow>
      </PanelSection>
    </pageView>
  );
}
