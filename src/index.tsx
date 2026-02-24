import {
  ButtonItem,
  ConfirmModal,
  DialogButton,
  Focusable,
  Navigation,
  PanelSection,
  PanelSectionRow,
  Router,
  DropdownItem,
  SliderField,
  Tabs,
  ToggleField,
  showModal,
  staticClasses,
} from "@decky/ui";
import * as DeckyUiNS from "@decky/ui";
import {
  FileSelectionType,
  callable,
  definePlugin,
  openFilePicker,
  routerHook,
  toaster,
} from "@decky/api";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { FaCloudDownloadAlt } from "react-icons/fa";

type ApiStatus = "success" | "error";
const SETTINGS_ROUTE = "/freedeck/settings";

interface ApiResponse<T> {
  status: ApiStatus;
  message?: string;
  data?: T;
  url?: string;
  reason?: string;
  diagnostics?: unknown;
}

interface LoginState {
  logged_in: boolean;
  user_account: string;
  message: string;
}

interface InstalledGameItem {
  game_id: string;
  title: string;
  install_path: string;
  size_text?: string;
  status?: string;
  steam_app_id?: number;
  playtime_seconds?: number;
  playtime_text?: string;
  playtime_sessions?: number;
  playtime_last_played_at?: number;
  playtime_active?: boolean;
}

interface InstalledState {
  total: number;
  preview: InstalledGameItem[];
}

interface TaskItem {
  task_id: string;
  game_id: string;
  game_title?: string;
  game_name?: string;
  file_name: string;
  status: string;
  progress: number;
  speed: number;
  error_reason?: string;
  install_status?: string;
  install_message?: string;
  installed_path?: string;
}

interface SettingsState {
  download_dir: string;
  install_dir: string;
  split_count: number;
  page_size: number;
  auto_delete_package: boolean;
  auto_install: boolean;
}

interface PanelState {
  login: LoginState;
  installed: InstalledState;
  tasks: TaskItem[];
  settings: SettingsState;
  library_url: string;
  power_diagnostics?: Record<string, unknown>;
}

interface SettingsPayload {
  download_dir: string;
  install_dir: string;
  split_count: number;
  page_size: number;
  auto_delete_package: boolean;
  auto_install: boolean;
}

interface UrlResponse {
  url: string;
}

interface ClearLoginResponse {
  logged_in: boolean;
  user_account: string;
  message: string;
}

interface CloudSaveUploadItem {
  game_id: string;
  game_title: string;
  game_key: string;
  status: string;
  reason: string;
  cloud_path: string;
}

interface CloudSaveLastResult {
  stage: string;
  reason: string;
  message: string;
  started_at: number;
  finished_at: number;
  timestamp: string;
  total_games: number;
  processed_games: number;
  uploaded: number;
  skipped: number;
  failed: number;
  results: CloudSaveUploadItem[];
}

interface CloudSaveUploadState {
  stage: string;
  message: string;
  reason: string;
  running: boolean;
  progress: number;
  current_game: string;
  total_games: number;
  processed_games: number;
  uploaded: number;
  skipped: number;
  failed: number;
  results: CloudSaveUploadItem[];
  last_result: CloudSaveLastResult;
}

interface CloudSaveUploadStartData {
  accepted: boolean;
  message: string;
  state: CloudSaveUploadState;
}

interface CloudSaveUploadStatusData {
  state: CloudSaveUploadState;
}

interface CloudSaveRestoreEntry {
  entry_id: string;
  entry_name: string;
  archive_rel_path?: string;
  file_count?: number;
}

interface CloudSaveRestoreVersion {
  version_name: string;
  timestamp: number;
  display_time: string;
  size_bytes: number;
  file_id?: string;
}

interface CloudSaveRestoreGameOption {
  game_id: string;
  game_title: string;
  game_key: string;
  versions: CloudSaveRestoreVersion[];
  available: boolean;
  reason: string;
}

interface CloudSaveRestoreOptionsData {
  games: CloudSaveRestoreGameOption[];
  updated_at: number;
}

interface CloudSaveRestoreEntriesData {
  game_id: string;
  game_key: string;
  game_title: string;
  version_name: string;
  entries: CloudSaveRestoreEntry[];
}

interface CloudSaveRestorePlanData {
  accepted: boolean;
  plan_id?: string;
  message: string;
  reason: string;
  requires_confirmation: boolean;
  conflict_count: number;
  conflict_samples: string[];
  target_candidates: string[];
  selected_target_dir: string;
  selected_entry_ids: string[];
  available_entries: CloudSaveRestoreEntry[];
  restorable_files: number;
  restorable_entries: number;
}

interface CloudSaveRestoreApplyData {
  status: string;
  reason: string;
  message: string;
  target_dir: string;
  restored_files: number;
  restored_entries: number;
  conflicts_overwritten: number;
}

interface CloudSaveRestoreResultItem {
  entry_id: string;
  entry_name: string;
  status: string;
  reason: string;
  file_count: number;
}

interface CloudSaveRestoreLastResult {
  status: string;
  reason: string;
  message: string;
  target_dir: string;
  restored_files: number;
  restored_entries: number;
  conflicts_overwritten: number;
  results: CloudSaveRestoreResultItem[];
}

interface CloudSaveRestoreState {
  stage: string;
  message: string;
  reason: string;
  running: boolean;
  progress: number;
  target_game_id: string;
  target_game_title: string;
  target_game_key: string;
  target_version: string;
  selected_entry_ids: string[];
  selected_target_dir: string;
  requires_confirmation: boolean;
  conflict_count: number;
  conflict_samples: string[];
  restored_files: number;
  restored_entries: number;
  results: CloudSaveRestoreResultItem[];
  last_result: CloudSaveRestoreLastResult;
}

interface CloudSaveRestoreStatusData {
  state: CloudSaveRestoreState;
}

interface UninstallInstalledPayload {
  game_id: string;
  install_path: string;
  delete_files: boolean;
}

interface LibraryGameTimeStatsData {
  managed: boolean;
  reason?: string;
  message?: string;
  app_id?: number;
  game_id?: string;
  title?: string;
  my_playtime_seconds?: number;
  my_playtime_text?: string;
  my_playtime_active?: boolean;
  main_story_hours?: number;
  main_story_time_text?: string;
  total_hours?: number;
  total_time_text?: string;
}

const getTianyiPanelState = callable<[Record<string, unknown>?], ApiResponse<PanelState>>("get_tianyi_panel_state");
const getTianyiLibraryUrl = callable<[], ApiResponse<UrlResponse>>("get_tianyi_library_url");
const getTianyiLoginUrl = callable<[], ApiResponse<UrlResponse>>("get_tianyi_login_url");
const setTianyiSettings = callable<[SettingsPayload], ApiResponse<SettingsState>>("set_tianyi_settings");
const clearTianyiLogin = callable<[], ApiResponse<ClearLoginResponse>>("clear_tianyi_login");
const startTianyiCloudSaveUpload = callable<[], ApiResponse<CloudSaveUploadStartData>>("start_tianyi_cloud_save_upload");
const getTianyiCloudSaveUploadStatus = callable<[], ApiResponse<CloudSaveUploadStatusData>>(
  "get_tianyi_cloud_save_upload_status",
);
const listTianyiCloudSaveRestoreOptions = callable<[], ApiResponse<CloudSaveRestoreOptionsData>>(
  "list_tianyi_cloud_save_restore_options",
);
const listTianyiCloudSaveRestoreEntries = callable<
  [{ game_id: string; game_key: string; game_title: string; version_name: string }],
  ApiResponse<CloudSaveRestoreEntriesData>
>("list_tianyi_cloud_save_restore_entries");
const planTianyiCloudSaveRestore = callable<
  [{ game_id: string; game_key: string; game_title: string; version_name: string; selected_entry_ids: string[]; target_dir?: string }],
  ApiResponse<CloudSaveRestorePlanData>
>("plan_tianyi_cloud_save_restore");
const applyTianyiCloudSaveRestore = callable<
  [{ plan_id: string; confirm_overwrite: boolean }],
  ApiResponse<CloudSaveRestoreApplyData>
>("apply_tianyi_cloud_save_restore");
const getTianyiCloudSaveRestoreStatus = callable<[], ApiResponse<CloudSaveRestoreStatusData>>(
  "get_tianyi_cloud_save_restore_status",
);
const recordTianyiGameAction = callable<
  [{ phase: string; app_id: string; action_name?: string }],
  ApiResponse<Record<string, unknown>>
>("record_tianyi_game_action");
const getTianyiLibraryGameTimeStats = callable<
  [{ app_id: string; title?: string }],
  ApiResponse<LibraryGameTimeStatsData>
>("get_tianyi_library_game_time_stats");
const uninstallTianyiInstalledGame = callable<[UninstallInstalledPayload], ApiResponse<Record<string, unknown>>>(
  "uninstall_tianyi_installed_game",
);
const PANEL_REQUEST_TIMEOUT_MS = 6000;
const PANEL_POLL_MODE_ACTIVE = "active";
const PANEL_POLL_MODE_IDLE = "idle";
const PANEL_POLL_MODE_BACKGROUND = "background";
type PanelPollMode = typeof PANEL_POLL_MODE_ACTIVE | typeof PANEL_POLL_MODE_IDLE | typeof PANEL_POLL_MODE_BACKGROUND;
const PANEL_ACTIVE_POLL_MS = 900;
const PANEL_IDLE_POLL_MS = 6000;
const PANEL_BACKGROUND_POLL_MS = 30000;

const EMPTY_SETTINGS: SettingsState = {
  download_dir: "",
  install_dir: "",
  split_count: 16,
  page_size: 50,
  auto_delete_package: false,
  auto_install: true,
};

const EMPTY_CLOUD_SAVE_STATE: CloudSaveUploadState = {
  stage: "idle",
  message: "未开始",
  reason: "",
  running: false,
  progress: 0,
  current_game: "",
  total_games: 0,
  processed_games: 0,
  uploaded: 0,
  skipped: 0,
  failed: 0,
  results: [],
  last_result: {
    stage: "",
    reason: "",
    message: "",
    started_at: 0,
    finished_at: 0,
    timestamp: "",
    total_games: 0,
    processed_games: 0,
    uploaded: 0,
    skipped: 0,
    failed: 0,
    results: [],
  },
};

const EMPTY_CLOUD_SAVE_RESTORE_STATE: CloudSaveRestoreState = {
  stage: "idle",
  message: "未开始",
  reason: "",
  running: false,
  progress: 0,
  target_game_id: "",
  target_game_title: "",
  target_game_key: "",
  target_version: "",
  selected_entry_ids: [],
  selected_target_dir: "",
  requires_confirmation: false,
  conflict_count: 0,
  conflict_samples: [],
  restored_files: 0,
  restored_entries: 0,
  results: [],
  last_result: {
    status: "",
    reason: "",
    message: "",
    target_dir: "",
    restored_files: 0,
    restored_entries: 0,
    conflicts_overwritten: 0,
    results: [],
  },
};

const PAGE_SIZE_OPTIONS = [20, 30, 50, 80, 100, 150, 200].map((value) => ({
  data: value,
  label: `${value} / 页`,
}));

const EMPTY_STATE: PanelState = {
  login: { logged_in: false, user_account: "", message: "" },
  installed: { total: 0, preview: [] },
  tasks: [],
  settings: EMPTY_SETTINGS,
  library_url: "",
};

interface GamepadTabClassMap {
  TabsRowScroll?: string;
  TabRowTabs?: string;
  Tab?: string;
  Active?: string;
  Selected?: string;
}

function getGamepadTabClassMap(): GamepadTabClassMap | null {
  const map = (DeckyUiNS as unknown as { gamepadTabbedPageClasses?: GamepadTabClassMap }).gamepadTabbedPageClasses;
  if (!map || typeof map !== "object") return null;
  return map;
}

function formatSpeed(speed: number): string {
  let value = Number(speed || 0);
  if (!Number.isFinite(value) || value <= 0) return "0 B/s";
  const units = ["B/s", "KB/s", "MB/s", "GB/s"];
  let index = 0;
  while (value >= 1024 && index < units.length - 1) {
    value /= 1024;
    index += 1;
  }
  return `${value.toFixed(index === 0 ? 0 : 2)} ${units[index]}`;
}

function formatPlaytimeText(seconds: number, fallback?: string): string {
  const fallbackText = String(fallback || "").trim();
  if (fallbackText) return fallbackText;
  const totalSeconds = Math.max(0, Number(seconds || 0));
  const hours = Math.floor(totalSeconds / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  if (hours > 0) return `${hours} 小时 ${minutes} 分钟`;
  if (minutes > 0) return `${minutes} 分钟`;
  return "0 分钟";
}

function clampProgress(progress: number): number {
  const value = Number(progress || 0);
  if (!Number.isFinite(value)) return 0;
  if (value < 0) return 0;
  if (value > 100) return 100;
  return value;
}

function downloadStatusText(status: string): string {
  const value = String(status || "").toLowerCase();
  if (value === "active") return "下载中";
  if (value === "waiting") return "等待中";
  if (value === "paused") return "已暂停";
  if (value === "complete") return "下载完成";
  if (value === "error") return "下载失败";
  if (value === "removed") return "已移除";
  return status || "未知";
}

function installStatusText(status: string): string {
  const value = String(status || "").toLowerCase();
  if (value === "pending") return "待处理";
  if (value === "installing") return "安装中";
  if (value === "installed") return "已安装";
  if (value === "skipped") return "已跳过";
  if (value === "failed") return "安装失败";
  return status || "未开始";
}

function progressColors(task: TaskItem): { track: string; fill: string; label: string } {
  const downloadStatus = String(task.status || "").toLowerCase();
  const installStatus = String(task.install_status || "").toLowerCase();
  if (downloadStatus === "error" || installStatus === "failed") {
    return { track: "rgba(255, 87, 87, 0.2)", fill: "#ff5757", label: "#ffb2b2" };
  }
  if (downloadStatus === "complete" && installStatus === "installed") {
    return { track: "rgba(67, 181, 129, 0.2)", fill: "#43b581", label: "#b8f3d7" };
  }
  if (downloadStatus === "paused") {
    return { track: "rgba(255, 184, 64, 0.22)", fill: "#ffb840", label: "#ffe2a8" };
  }
  return { track: "rgba(208, 188, 255, 0.24)", fill: "#d0bcff", label: "#ece2ff" };
}

function TaskProgressRow(task: TaskItem) {
  const progress = clampProgress(task.progress);
  const colors = progressColors(task);
  const title = task.game_title || task.game_name || task.file_name || "未命名任务";
  const installStatus = String(task.install_status || "").trim();
  const installMessage = String(task.install_message || "").trim();
  const errorReason = String(task.error_reason || "").trim();

  return (
    <div
      style={{
        width: "100%",
        padding: "8px 0",
        display: "flex",
        flexDirection: "column",
        gap: "6px",
      }}
    >
      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", gap: "8px" }}>
        <div
          style={{
            fontSize: "14px",
            fontWeight: 600,
            lineHeight: 1.35,
            overflow: "hidden",
            textOverflow: "ellipsis",
            whiteSpace: "nowrap",
            maxWidth: "70%",
          }}
          title={title}
        >
          {title}
        </div>
        <div style={{ fontSize: "11px", color: colors.label }}>{downloadStatusText(task.status)}</div>
      </div>

      <div
        style={{
          width: "100%",
          height: "8px",
          borderRadius: "999px",
          background: colors.track,
          overflow: "hidden",
        }}
      >
        <div
          style={{
            width: `${progress.toFixed(1)}%`,
            height: "100%",
            borderRadius: "999px",
            background: colors.fill,
            transition: "width 860ms linear",
          }}
        />
      </div>

      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", gap: "8px", fontSize: "11px", color: "#c9c9c9" }}>
        <span>{`进度 ${progress.toFixed(1)}%`}</span>
        <span>{`速度 ${formatSpeed(task.speed)}`}</span>
      </div>

      {installStatus ? (
        <div style={{ fontSize: "11px", color: "#bcbcbc", lineHeight: 1.4 }}>
          {`安装：${installStatusText(installStatus)}${installMessage ? ` | ${installMessage}` : ""}`}
        </div>
      ) : null}
      {errorReason ? (
        <div style={{ fontSize: "11px", color: "#ff8e8e", lineHeight: 1.4 }}>{`错误：${errorReason}`}</div>
      ) : null}
    </div>
  );
}

function openExternalUrl(rawUrl: string): void {
  const url = String(rawUrl || "").trim();
  if (!url) return;

  try {
    Navigation.NavigateToExternalWeb(url);
    return;
  } catch {
    // 忽略并回退到备用跳转。
  }

  try {
    const steamClient = (window as unknown as {
      SteamClient?: { Browser?: { OpenUrl?: (u: string) => void } };
    }).SteamClient;
    if (steamClient?.Browser?.OpenUrl) {
      steamClient.Browser.OpenUrl(url);
      return;
    }
  } catch {
    // 忽略并回退到 window.open。
  }

  const popup = window.open(url, "_blank");
  if (!popup) window.location.href = url;
}

function describeOpenError(result: ApiResponse<UrlResponse>, fallback: string): string {
  const message = String(result.message || fallback);
  const reason = String(result.reason || "").trim();
  if (!reason) return message;
  if (!result.diagnostics) return `${message}（${reason}）`;
  try {
    return `${message}（${reason}）\n${JSON.stringify(result.diagnostics)}`;
  } catch {
    return `${message}（${reason}）`;
  }
}

function toPayload(settings: SettingsState): SettingsPayload {
  return {
    download_dir: String(settings.download_dir || ""),
    install_dir: String(settings.install_dir || ""),
    split_count: Math.max(1, Math.min(64, Number(settings.split_count || 16))),
    page_size: Math.max(10, Math.min(200, Number(settings.page_size || 50))),
    auto_delete_package: Boolean(settings.auto_delete_package),
    auto_install: true,
  };
}

function normalizeCloudSaveUploadState(raw: Partial<CloudSaveUploadState> | undefined): CloudSaveUploadState {
  const source = raw || {};
  const lastRaw = source.last_result || EMPTY_CLOUD_SAVE_STATE.last_result;
  return {
    ...EMPTY_CLOUD_SAVE_STATE,
    ...source,
    stage: String(source.stage || "idle"),
    message: String(source.message || "未开始"),
    reason: String(source.reason || ""),
    running: Boolean(source.running),
    progress: clampProgress(Number(source.progress || 0)),
    current_game: String(source.current_game || ""),
    total_games: Math.max(0, Number(source.total_games || 0)),
    processed_games: Math.max(0, Number(source.processed_games || 0)),
    uploaded: Math.max(0, Number(source.uploaded || 0)),
    skipped: Math.max(0, Number(source.skipped || 0)),
    failed: Math.max(0, Number(source.failed || 0)),
    results: Array.isArray(source.results) ? source.results : [],
    last_result: {
      ...EMPTY_CLOUD_SAVE_STATE.last_result,
      ...lastRaw,
      stage: String(lastRaw.stage || ""),
      reason: String(lastRaw.reason || ""),
      message: String(lastRaw.message || ""),
      timestamp: String(lastRaw.timestamp || ""),
      started_at: Math.max(0, Number(lastRaw.started_at || 0)),
      finished_at: Math.max(0, Number(lastRaw.finished_at || 0)),
      total_games: Math.max(0, Number(lastRaw.total_games || 0)),
      processed_games: Math.max(0, Number(lastRaw.processed_games || 0)),
      uploaded: Math.max(0, Number(lastRaw.uploaded || 0)),
      skipped: Math.max(0, Number(lastRaw.skipped || 0)),
      failed: Math.max(0, Number(lastRaw.failed || 0)),
      results: Array.isArray(lastRaw.results) ? lastRaw.results : [],
    },
  };
}

function normalizeCloudSaveRestoreState(raw: Partial<CloudSaveRestoreState> | undefined): CloudSaveRestoreState {
  const source = raw || {};
  const lastRaw = source.last_result || EMPTY_CLOUD_SAVE_RESTORE_STATE.last_result;
  return {
    ...EMPTY_CLOUD_SAVE_RESTORE_STATE,
    ...source,
    stage: String(source.stage || "idle"),
    message: String(source.message || "未开始"),
    reason: String(source.reason || ""),
    running: Boolean(source.running),
    progress: clampProgress(Number(source.progress || 0)),
    target_game_id: String(source.target_game_id || ""),
    target_game_title: String(source.target_game_title || ""),
    target_game_key: String(source.target_game_key || ""),
    target_version: String(source.target_version || ""),
    selected_entry_ids: Array.isArray(source.selected_entry_ids) ? source.selected_entry_ids : [],
    selected_target_dir: String(source.selected_target_dir || ""),
    requires_confirmation: Boolean(source.requires_confirmation),
    conflict_count: Math.max(0, Number(source.conflict_count || 0)),
    conflict_samples: Array.isArray(source.conflict_samples) ? source.conflict_samples : [],
    restored_files: Math.max(0, Number(source.restored_files || 0)),
    restored_entries: Math.max(0, Number(source.restored_entries || 0)),
    results: Array.isArray(source.results) ? source.results : [],
    last_result: {
      ...EMPTY_CLOUD_SAVE_RESTORE_STATE.last_result,
      ...lastRaw,
      status: String(lastRaw.status || ""),
      reason: String(lastRaw.reason || ""),
      message: String(lastRaw.message || ""),
      target_dir: String(lastRaw.target_dir || ""),
      restored_files: Math.max(0, Number(lastRaw.restored_files || 0)),
      restored_entries: Math.max(0, Number(lastRaw.restored_entries || 0)),
      conflicts_overwritten: Math.max(0, Number(lastRaw.conflicts_overwritten || 0)),
      results: Array.isArray(lastRaw.results) ? lastRaw.results : [],
    },
  };
}

function formatBytes(value: number): string {
  let size = Number(value || 0);
  if (!Number.isFinite(size) || size <= 0) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB"];
  let index = 0;
  while (size >= 1024 && index < units.length - 1) {
    size /= 1024;
    index += 1;
  }
  return `${size.toFixed(index === 0 ? 0 : 2)} ${units[index]}`;
}

function cloudSaveStageText(stage: string): string {
  const value = String(stage || "").trim().toLowerCase();
  if (value === "listing") return "拉取版本中";
  if (value === "planning") return "规划中";
  if (value === "ready") return "待确认";
  if (value === "applying") return "恢复中";
  if (value === "scanning") return "扫描中";
  if (value === "packaging") return "打包中";
  if (value === "uploading") return "上传中";
  if (value === "completed") return "已完成";
  if (value === "failed") return "失败";
  return "空闲";
}

type AnyFunction = (...args: unknown[]) => unknown;

interface PatchHandle {
  unpatch: () => void;
}

const LIBRARY_TIME_CACHE_TTL_MS = 5 * 60 * 1000;
const LIBRARY_TIME_NEGATIVE_CACHE_TTL_MS = 3 * 1000;
const LIBRARY_TIME_RETRY_MS = 3 * 1000;
const LIBRARY_TIME_REFRESH_MS = 2 * 60 * 1000;
const libraryTimeCache = new Map<string, { updatedAt: number; payload: LibraryGameTimeStatsData }>();

interface SteamAppsGameActionApi {
  RegisterForGameActionStart?: (cb: (...args: unknown[]) => void) => { unregister?: () => void };
  RegisterForGameActionEnd?: (cb: (...args: unknown[]) => void) => { unregister?: () => void };
}

function parseGameActionAppIdCandidate(value: unknown): number {
  if (typeof value === "number" && Number.isFinite(value)) {
    return Math.max(0, Math.trunc(value));
  }
  if (typeof value === "string") {
    const text = value.trim();
    if (/^\d+$/.test(text)) {
      return Math.max(0, Number.parseInt(text, 10));
    }
    return 0;
  }
  if (!value || typeof value !== "object") {
    return 0;
  }

  const obj = value as Record<string, unknown>;
  const keys = ["app_id", "appid", "appId", "unAppID", "nAppID", "m_unAppID", "m_nAppID"];
  for (const key of keys) {
    const parsed = parseGameActionAppIdCandidate(obj[key]);
    if (parsed > 0) return parsed;
  }
  return 0;
}

function resolveGameActionAppId(args: unknown[]): string {
  const ordered = [args[1], args[0], args[2], ...args];
  for (const value of ordered) {
    const parsed = parseGameActionAppIdCandidate(value);
    if (parsed > 0) return String(parsed);
  }
  return "";
}

function invalidateLibraryTimeCacheByAppId(appId: string): void {
  const prefix = `${String(appId || "").trim()}::`;
  if (!prefix || prefix === "::") return;
  for (const key of Array.from(libraryTimeCache.keys())) {
    if (key.startsWith(prefix)) {
      libraryTimeCache.delete(key);
    }
  }
}

function resolveSteamAppsGameActionApi(): SteamAppsGameActionApi | null {
  if (typeof window === "undefined") return null;
  const apps = (
    window as unknown as {
      SteamClient?: { Apps?: SteamAppsGameActionApi };
    }
  ).SteamClient?.Apps;
  if (!apps || typeof apps !== "object") return null;
  return apps;
}

function installGlobalGameActionReporter(): () => void {
  let disposed = false;
  let retryTimer: number | null = null;
  let teardownListener: (() => void) | null = null;

  const clearRetry = () => {
    if (retryTimer === null) return;
    if (typeof window !== "undefined") {
      window.clearTimeout(retryTimer);
    }
    retryTimer = null;
  };

  const scheduleRetry = () => {
    if (disposed || retryTimer !== null || typeof window === "undefined") return;
    retryTimer = window.setTimeout(() => {
      retryTimer = null;
      tryInstall();
    }, 1500);
  };

  const tryInstall = () => {
    if (disposed || teardownListener) return;

    let apps: SteamAppsGameActionApi | null = null;
    try {
      apps = resolveSteamAppsGameActionApi();
    } catch {
      scheduleRetry();
      return;
    }

    const registerStart = apps?.RegisterForGameActionStart;
    const registerEnd = apps?.RegisterForGameActionEnd;
    if (typeof registerStart !== "function" || typeof registerEnd !== "function") {
      scheduleRetry();
      return;
    }

    const activeAppIds = new Set<string>();
    const reportAction = (phase: "start" | "end", args: unknown[]) => {
      const appId = resolveGameActionAppId(args);
      if (!appId) return;

      if (phase === "start") {
        if (activeAppIds.has(appId)) return;
        activeAppIds.add(appId);
      } else {
        activeAppIds.delete(appId);
      }

      invalidateLibraryTimeCacheByAppId(appId);
      void recordTianyiGameAction({
        phase,
        app_id: appId,
        action_name: "",
      }).catch(() => {
        // 忽略事件上报失败，避免影响主流程。
      });
    };

    let startListener: { unregister?: () => void } | undefined;
    let endListener: { unregister?: () => void } | undefined;
    try {
      startListener = registerStart((...args: unknown[]) => {
        reportAction("start", args);
      });
      endListener = registerEnd((...args: unknown[]) => {
        reportAction("end", args);
      });
    } catch {
      scheduleRetry();
      return;
    }

    teardownListener = () => {
      activeAppIds.clear();
      try {
        startListener?.unregister?.();
      } catch {
        // 忽略反注册异常。
      }
      try {
        endListener?.unregister?.();
      } catch {
        // 忽略反注册异常。
      }
    };
  };

  tryInstall();
  return () => {
    disposed = true;
    clearRetry();
    if (teardownListener) {
      teardownListener();
      teardownListener = null;
    }
  };
}

function hasPositiveLibraryHltb(payload: LibraryGameTimeStatsData | null | undefined): boolean {
  if (!payload) return false;
  const main = Number(payload.main_story_hours || 0);
  const total = Number(payload.total_hours || 0);
  return (Number.isFinite(main) && main > 0) || (Number.isFinite(total) && total > 0);
}

function normalizeHoursText(hoursRaw: number | undefined, fallbackRaw: string | undefined): string {
  const fallback = String(fallbackRaw || "").trim();
  if (fallback) return fallback;
  const hours = Number(hoursRaw || 0);
  if (!Number.isFinite(hours) || hours <= 0) return "-";
  const rounded = Math.round(hours * 10) / 10;
  if (Math.abs(rounded - Math.round(rounded)) < 0.05) {
    return `${Math.round(rounded)} 小时`;
  }
  return `${rounded.toFixed(1)} 小时`;
}

function wrapReactType(node: unknown, prop = "type"): Record<string, unknown> | null {
  if (!node || typeof node !== "object") return null;
  const owner = node as Record<string, unknown>;
  const current = owner[prop];
  if (!current || typeof current !== "object") return null;
  const currentMap = current as Record<string, unknown>;
  if (Boolean(currentMap.__FREDECK_WRAPPED)) return currentMap;
  const wrapped: Record<string, unknown> = { ...currentMap, __FREDECK_WRAPPED: true };
  owner[prop] = wrapped;
  return wrapped;
}

function afterPatch(object: Record<string, unknown>, property: string, handler: (args: unknown[], ret: unknown) => unknown): PatchHandle {
  const original = object[property];
  if (typeof original !== "function") {
    return { unpatch: () => {} };
  }

  const originalFn = original as AnyFunction;
  const patched: AnyFunction = function patchedFunction(this: unknown, ...args: unknown[]) {
    const result = originalFn.apply(this, args);
    const next = handler.call(this, args, result);
    return typeof next === "undefined" ? result : next;
  };

  try {
    Object.assign(patched, originalFn);
  } catch {
    // 忽略函数属性拷贝失败。
  }
  try {
    Object.defineProperty(patched, "toString", {
      value: () => originalFn.toString(),
      configurable: true,
    });
  } catch {
    // 忽略 toString 覆盖失败。
  }

  object[property] = patched;
  return {
    unpatch: () => {
      if (object[property] === patched) {
        object[property] = original;
      }
    },
  };
}

function resolveSpliceTarget(children: unknown): unknown[] | null {
  if (!children || typeof children !== "object") return null;
  const root = children as Record<string, unknown>;
  const part1 = root.props as Record<string, unknown> | undefined;
  const part2 = part1?.children as unknown[];
  if (!Array.isArray(part2) || part2.length < 2) return null;
  const part3 = part2[1] as Record<string, unknown> | undefined;
  const part4 = part3?.props as Record<string, unknown> | undefined;
  const part5 = part4?.children as Record<string, unknown> | undefined;
  const part6 = part5?.props as Record<string, unknown> | undefined;
  const list = part6?.children;
  if (Array.isArray(list)) return list;

  const visited = new Set<object>();
  const queue: unknown[] = [children];
  while (queue.length > 0) {
    const current = queue.shift();
    if (!current || typeof current !== "object") continue;
    if (visited.has(current as object)) continue;
    visited.add(current as object);

    if (Array.isArray(current)) {
      if (current.length > 0) {
        const hasOverviewCarrier = current.some((node) => {
          if (!node || typeof node !== "object") return false;
          const nodeProps = (node as Record<string, unknown>).props as Record<string, unknown> | undefined;
          const inner = (nodeProps?.children as Record<string, unknown> | undefined)?.props as Record<string, unknown> | undefined;
          return typeof inner?.overview !== "undefined" || typeof inner?.details !== "undefined";
        });
        if (hasOverviewCarrier) return current;
      }
      for (const item of current) {
        if (item && typeof item === "object") queue.push(item);
      }
      continue;
    }

    const map = current as Record<string, unknown>;
    for (const value of Object.values(map)) {
      if (!value) continue;
      if (typeof value === "object") queue.push(value);
    }
  }

  return null;
}

function findLibraryInsertIndex(nodes: unknown[]): number {
  return nodes.findIndex((child) => {
    if (!child || typeof child !== "object") return false;
    const props = (child as Record<string, unknown>).props as Record<string, unknown> | undefined;
    if (!props) return false;
    const hasFocusFlag = typeof props.childFocusDisabled !== "undefined";
    const hasNavRef = typeof props.navRef !== "undefined";
    const innerProps = (props.children as Record<string, unknown> | undefined)?.props as Record<string, unknown> | undefined;
    const hasDetails = typeof innerProps?.details !== "undefined";
    const hasOverview = typeof innerProps?.overview !== "undefined";
    const hasFastRender = typeof innerProps?.bFastRender !== "undefined";
    return hasFocusFlag && hasNavRef && hasDetails && hasOverview && hasFastRender;
  });
}

function resolveLibraryInsertIndex(nodes: unknown[]): number {
  const exact = findLibraryInsertIndex(nodes);
  if (exact >= 0) return exact;

  const firstNative = nodes.findIndex((child) => {
    if (!child || typeof child !== "object") return false;
    const key = String((child as Record<string, unknown>).key || "");
    return !key.startsWith("freedeck-library-times-");
  });
  if (firstNative >= 0) return Math.min(firstNative + 1, nodes.length);

  return Math.min(1, nodes.length);
}

const LIBRARY_TIME_BLOCK_ID = "freedeck-library-times";

function isLibraryTimeNode(child: unknown): boolean {
  if (!child || typeof child !== "object") return false;
  const childProps = (child as Record<string, unknown>).props as Record<string, unknown> | undefined;
  return String(childProps?.id || "") === LIBRARY_TIME_BLOCK_ID;
}

function ensureNodeChildrenArray(node: Record<string, unknown>): unknown[] {
  const props = (node.props as Record<string, unknown> | undefined) || {};
  node.props = props;
  const children = props.children;
  if (Array.isArray(children)) return children;
  if (typeof children === "undefined") {
    const next: unknown[] = [];
    props.children = next;
    return next;
  }
  const next: unknown[] = [children];
  props.children = next;
  return next;
}

function findInReactTreeLike(node: unknown, filter: (value: unknown) => boolean): Record<string, unknown> | null {
  if (!node || typeof node !== "object") return null;
  const visited = new Set<object>();
  const queue: unknown[] = [node];
  while (queue.length > 0) {
    const current = queue.shift();
    if (!current || typeof current !== "object") continue;
    if (visited.has(current as object)) continue;
    visited.add(current as object);

    if (filter(current)) return current as Record<string, unknown>;

    if (Array.isArray(current)) {
      for (const item of current) {
        if (item && typeof item === "object") queue.push(item);
      }
      continue;
    }

    const map = current as Record<string, unknown>;
    const walkKeys = ["props", "children", "child", "sibling"];
    for (const key of walkKeys) {
      const value = map[key];
      if (value && typeof value === "object") queue.push(value);
    }
    for (const value of Object.values(map)) {
      if (value && typeof value === "object") queue.push(value);
    }
  }
  return null;
}

function resolveStaticClassByKey(key: string): string {
  const root = staticClasses as unknown;
  if (!root || typeof root !== "object") return "";
  const visited = new Set<object>();
  const queue: unknown[] = [root];
  while (queue.length > 0) {
    const current = queue.shift();
    if (!current || typeof current !== "object") continue;
    if (visited.has(current as object)) continue;
    visited.add(current as object);

    if (Array.isArray(current)) {
      for (const item of current) {
        if (item && typeof item === "object") queue.push(item);
      }
      continue;
    }

    const map = current as Record<string, unknown>;
    if (typeof map[key] === "string" && String(map[key]).trim()) {
      return String(map[key]).trim();
    }
    for (const value of Object.values(map)) {
      if (value && typeof value === "object") queue.push(value);
    }
  }
  return "";
}

function resolveLibraryOverlayHost(tree: unknown): Record<string, unknown> | null {
  const topCapsuleClass = resolveStaticClassByKey("TopCapsule");
  const headerClass = resolveStaticClassByKey("Header");
  const appDetailsRootClass = resolveStaticClassByKey("AppDetailsRoot");
  const markers = [topCapsuleClass, appDetailsRootClass, headerClass].filter(Boolean);

  const classMatched = findInReactTreeLike(tree, (value) => {
    if (!value || typeof value !== "object") return false;
    const props = (value as Record<string, unknown>).props as Record<string, unknown> | undefined;
    if (!props) return false;
    const className = String(props.className || "");
    const children = props.children;
    if (!className || typeof children === "undefined") return false;
    return markers.some((marker) => className.includes(marker));
  });
  if (classMatched) return classMatched;

  // 回退：命中包含 overview/details 的容器，确保至少能显示。
  return findInReactTreeLike(tree, (value) => {
    if (!value || typeof value !== "object") return false;
    const props = (value as Record<string, unknown>).props as Record<string, unknown> | undefined;
    if (!props) return false;
    const children = props.children;
    if (!children || typeof children !== "object" || Array.isArray(children)) return false;
    const inner = (children as Record<string, unknown>).props as Record<string, unknown> | undefined;
    return typeof inner?.overview !== "undefined" && typeof inner?.details !== "undefined";
  });
}

function resolveLibraryOverviewNode(tree: unknown): Record<string, unknown> | null {
  return findInReactTreeLike(tree, (value) => {
    if (!value || typeof value !== "object") return false;
    const map = value as Record<string, unknown>;
    const appId = Math.max(
      0,
      Number(
        map.appid ??
          map.appId ??
          map.unAppID ??
          map.nAppID ??
          map.m_unAppID ??
          map.m_nAppID ??
          0,
      ),
    );
    if (!Number.isFinite(appId) || appId <= 0) return false;
    const title =
      String(map.display_name || "").trim() ||
      String(map.name || "").trim() ||
      String(map.app_name || "").trim() ||
      String(map.title || "").trim();
    return Boolean(title);
  });
}

function resolvePatchHandleUnpatch(handle: unknown): () => void {
  if (handle && typeof handle === "object" && typeof (handle as { unpatch?: unknown }).unpatch === "function") {
    return () => {
      try {
        (handle as { unpatch: () => void }).unpatch();
      } catch {
        // 忽略移除 patch 异常。
      }
    };
  }
  if (typeof handle === "function") {
    return () => {
      try {
        (handle as () => void)();
      } catch {
        // 忽略移除 patch 异常。
      }
    };
  }
  return () => {};
}

function LibraryTimeBlock({ appId, title }: { appId: number; title: string }) {
  const appIdText = String(Math.max(0, Number(appId || 0)));
  const titleText = String(title || "").trim();
  const cacheKey = `${appIdText}::${titleText.toLowerCase()}`;
  const initialCached = (() => {
    const cached = libraryTimeCache.get(cacheKey);
    if (!cached) return null;
    const ttl = hasPositiveLibraryHltb(cached.payload) ? LIBRARY_TIME_CACHE_TTL_MS : LIBRARY_TIME_NEGATIVE_CACHE_TTL_MS;
    if (Date.now() - cached.updatedAt > ttl) {
      libraryTimeCache.delete(cacheKey);
      return null;
    }
    return cached.payload;
  })();

  const [data, setData] = useState<LibraryGameTimeStatsData | null>(initialCached);

  useEffect(() => {
    let alive = true;
    let timerId: number | null = null;
    let inFlight = false;

    const clearTimer = () => {
      if (timerId === null) return;
      window.clearTimeout(timerId);
      timerId = null;
    };

    const scheduleNext = (payload: LibraryGameTimeStatsData | null) => {
      if (!alive) return;
      clearTimer();
      const waitMs = hasPositiveLibraryHltb(payload) ? LIBRARY_TIME_REFRESH_MS : LIBRARY_TIME_RETRY_MS;
      timerId = window.setTimeout(() => {
        void fetchStats();
      }, waitMs);
    };

    const fetchStats = async () => {
      if (!alive || inFlight) return;
      if (!appIdText || appIdText === "0") return;
      inFlight = true;
      try {
        const result = await withTimeout(
          getTianyiLibraryGameTimeStats({
            app_id: appIdText,
            title: titleText,
          }),
          7000,
          "library_time_stats_timeout",
        );
        if (!alive) return;
        if (result.status !== "success") {
          scheduleNext(null);
          return;
        }
        const payload = (result.data || {}) as LibraryGameTimeStatsData;
        libraryTimeCache.set(cacheKey, { updatedAt: Date.now(), payload });
        setData(payload);
        scheduleNext(payload);
      } catch {
        // 忽略库页面时长读取失败，不影响原页面。
        scheduleNext(null);
      } finally {
        inFlight = false;
      }
    };

    if (!appIdText || appIdText === "0") {
      setData(null);
      return () => {
        alive = false;
        clearTimer();
      };
    }

    const cached = libraryTimeCache.get(cacheKey);
    const cachedTtl = cached ? (hasPositiveLibraryHltb(cached.payload) ? LIBRARY_TIME_CACHE_TTL_MS : LIBRARY_TIME_NEGATIVE_CACHE_TTL_MS) : 0;
    if (cached && Date.now() - cached.updatedAt <= cachedTtl) {
      setData(cached.payload);
      // 正缓存按周期刷新；负缓存或未托管立即重查，避免“要等很久才出现”。
      if (hasPositiveLibraryHltb(cached.payload) && cached.payload.managed !== false) {
        scheduleNext(cached.payload);
      } else {
        void fetchStats();
      }
      return () => {
        alive = false;
        clearTimer();
      };
    }

    void fetchStats();

    return () => {
      alive = false;
      clearTimer();
    };
  }, [appIdText, cacheKey, titleText]);

  const isLoading = !data;
  const myPlaytime = isLoading ? "加载中" : formatPlaytimeText(data.my_playtime_seconds || 0, data.my_playtime_text);
  const mainStory = isLoading ? "加载中" : normalizeHoursText(data.main_story_hours, data.main_story_time_text);
  const totalTime = isLoading ? "加载中" : normalizeHoursText(data.total_hours, data.total_time_text);
  const activeSuffix = !isLoading && data?.my_playtime_active ? "（进行中）" : "";

  return (
    <div
      id={LIBRARY_TIME_BLOCK_ID}
      style={{
        position: "absolute",
        top: "14px",
        left: "360px",
        right: "auto",
        zIndex: 40,
        minWidth: "250px",
        maxWidth: "320px",
        padding: "8px 10px",
        borderRadius: 1,
        background: "transparent",
        border: "none",
        boxShadow: "none",
        pointerEvents: "none",
      }}
    >
      <div style={{ display: "grid", gridTemplateColumns: "minmax(0, 1fr) minmax(0, 1fr) minmax(0, 1fr)", gap: "10px" }}>
        <div style={{ minWidth: 0 }}>
          <div style={{ fontSize: "14px", fontWeight: 700, lineHeight: 1.2, whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }}>
            {`${myPlaytime}${activeSuffix}`}
          </div>
          <div style={{ fontSize: "10px", opacity: 0.84 }}>已玩时长</div>
        </div>
        <div style={{ minWidth: 0 }}>
          <div style={{ fontSize: "14px", fontWeight: 700, lineHeight: 1.2, whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }}>
            {mainStory}
          </div>
          <div style={{ fontSize: "10px", opacity: 0.84 }}>主线时长</div>
        </div>
        <div style={{ minWidth: 0 }}>
          <div style={{ fontSize: "14px", fontWeight: 700, lineHeight: 1.2, whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }}>
            {totalTime}
          </div>
          <div style={{ fontSize: "10px", opacity: 0.84 }}>总时长</div>
        </div>
      </div>
    </div>
  );
}

function installLibraryPlaytimePatch(): () => void {
  const hookApi = routerHook as unknown as {
    addPatch?: (route: string, patcher: (props: unknown) => unknown) => unknown;
  };
  if (typeof hookApi.addPatch !== "function") {
    return () => {};
  }

  const patchHandle = hookApi.addPatch("/library/app/:appid", (props: unknown) => {
    if (!props || typeof props !== "object") return props;
    const propsMap = props as Record<string, unknown>;
    const childNode = propsMap.children as Record<string, unknown> | undefined;
    const childProps = childNode?.props as Record<string, unknown> | undefined;
    if (!childProps) return props;

    const renderFunc = childProps.renderFunc;
    if (typeof renderFunc !== "function") return props;
    if (Boolean((renderFunc as unknown as Record<string, unknown>).__FREDECK_LIBRARY_RENDER_PATCHED)) return props;

    const renderPatch = afterPatch(childProps, "renderFunc", (_args, ret1) => {
      if (!ret1 || typeof ret1 !== "object") return ret1;
      const ret1Map = ret1 as Record<string, unknown>;
      const contentNode = ret1Map.props as Record<string, unknown> | undefined;
      const primaryOverview = ((contentNode?.children as Record<string, unknown> | undefined)?.props as Record<string, unknown> | undefined)
        ?.overview as Record<string, unknown> | undefined;
      const fallbackOverview = resolveLibraryOverviewNode(ret1Map);
      const overviewNode = primaryOverview || fallbackOverview;
      const gameTitle = String(
        overviewNode?.display_name || overviewNode?.name || overviewNode?.app_name || overviewNode?.title || "",
      ).trim();
      const appId = Math.max(
        0,
        Number(
          overviewNode?.appid ??
            overviewNode?.appId ??
            overviewNode?.unAppID ??
            overviewNode?.nAppID ??
            overviewNode?.m_unAppID ??
            overviewNode?.m_nAppID ??
            0,
        ),
      );
      if (!appId) return ret1;

      const nodeWithType = (contentNode?.children as Record<string, unknown> | undefined) || null;
      const wrappedType = wrapReactType(nodeWithType, "type");
      if (!wrappedType) return ret1;
      const typeFn = wrappedType.type;
      if (typeof typeFn !== "function") return ret1;
      const typeState = typeFn as unknown as Record<string, unknown>;
      typeState.__FREDECK_LIBRARY_APP_ID = appId;
      typeState.__FREDECK_LIBRARY_GAME_TITLE = gameTitle;
      if (Boolean(typeState.__FREDECK_LIBRARY_TYPE_PATCHED)) return ret1;

      const typePatch = afterPatch(wrappedType, "type", (_innerArgs, ret2) => {
        if (!ret2 || typeof ret2 !== "object") return ret2;
        const dynamicType = wrappedType.type as Record<string, unknown>;
        const runtimeAppId = Math.max(0, Number(dynamicType.__FREDECK_LIBRARY_APP_ID || appId));
        const runtimeTitle = String(dynamicType.__FREDECK_LIBRARY_GAME_TITLE || gameTitle).trim();
        if (!runtimeAppId) return ret2;
        const nextComponent = (
          <LibraryTimeBlock key={`freedeck-library-times-${runtimeAppId}`} appId={runtimeAppId} title={runtimeTitle} />
        );

        const overlayHost = resolveLibraryOverlayHost(ret2);
        if (overlayHost) {
          const hostChildren = ensureNodeChildrenArray(overlayHost);
          const hostProps = (overlayHost.props as Record<string, unknown> | undefined) || {};
          overlayHost.props = hostProps;
          const currentStyle = hostProps.style;
          if (!currentStyle || typeof currentStyle !== "object") {
            hostProps.style = { position: "relative" };
          } else {
            const styleMap = currentStyle as Record<string, unknown>;
            if (!styleMap.position) {
              hostProps.style = { ...styleMap, position: "relative" };
            }
          }

          const existingInHost = hostChildren.findIndex((child) => isLibraryTimeNode(child));
          if (existingInHost >= 0) {
            hostChildren.splice(existingInHost, 1, nextComponent);
          } else {
            hostChildren.push(nextComponent);
          }
          return ret2;
        }

        const spliceTarget = resolveSpliceTarget(ret2);
        if (!spliceTarget) return ret2;
        const existingInFallback = spliceTarget.findIndex((child) => isLibraryTimeNode(child));
        if (existingInFallback >= 0) {
          spliceTarget.splice(existingInFallback, 1, nextComponent);
          return ret2;
        }

        const insertIndex = resolveLibraryInsertIndex(spliceTarget);
        spliceTarget.splice(insertIndex, 0, nextComponent);
        return ret2;
      });

      const patchedType = wrappedType.type as Record<string, unknown>;
      patchedType.__FREDECK_LIBRARY_TYPE_PATCHED = true;
      patchedType.__FREDECK_LIBRARY_APP_ID = appId;
      patchedType.__FREDECK_LIBRARY_GAME_TITLE = gameTitle;
      patchedType.__FREDECK_LIBRARY_TYPE_UNPATCH = typePatch.unpatch;
      return ret1;
    });

    const patchedRender = childProps.renderFunc as Record<string, unknown>;
    patchedRender.__FREDECK_LIBRARY_RENDER_PATCHED = true;
    patchedRender.__FREDECK_LIBRARY_RENDER_UNPATCH = renderPatch.unpatch;
    return props;
  });

  return resolvePatchHandleUnpatch(patchHandle);
}

async function withTimeout<T>(promise: Promise<T>, timeoutMs: number, timeoutMessage: string): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const timer = window.setTimeout(() => {
      reject(new Error(timeoutMessage));
    }, timeoutMs);
    promise
      .then((value) => {
        window.clearTimeout(timer);
        resolve(value);
      })
      .catch((error) => {
        window.clearTimeout(timer);
        reject(error);
      });
  });
}

async function pickFolder(startPath: string): Promise<string> {
  const base = String(startPath || "/home/deck").trim() || "/home/deck";
  const result = await openFilePicker(FileSelectionType.FOLDER, base, false, true);
  return String(result?.realpath || result?.path || "").trim();
}

function isTaskAlreadyInstalled(task: TaskItem): boolean {
  const installStatus = String(task.install_status || "").trim().toLowerCase();
  if (installStatus === "installed") return true;
  const downloadStatus = String(task.status || "").trim().toLowerCase();
  if (downloadStatus === "complete" && String(task.installed_path || "").trim()) return true;
  return false;
}

function isTaskActive(task: TaskItem): boolean {
  const status = String(task.status || "").trim().toLowerCase();
  if (!status) return false;
  return !["complete", "error", "removed"].includes(status);
}

function countActiveTasks(tasks: TaskItem[]): number {
  let count = 0;
  for (const task of tasks || []) {
    if (isTaskActive(task)) count += 1;
  }
  return count;
}

function resolvePanelPollMode(state: PanelState): PanelPollMode {
  const visible = !document.hidden;
  if (!visible) return PANEL_POLL_MODE_BACKGROUND;
  return countActiveTasks(state.tasks || []) > 0 ? PANEL_POLL_MODE_ACTIVE : PANEL_POLL_MODE_IDLE;
}

function pollIntervalByMode(mode: PanelPollMode): number {
  if (mode === PANEL_POLL_MODE_ACTIVE) return PANEL_ACTIVE_POLL_MS;
  if (mode === PANEL_POLL_MODE_BACKGROUND) return PANEL_BACKGROUND_POLL_MS;
  return PANEL_IDLE_POLL_MS;
}

function SettingsPage() {
  const [loading, setLoading] = useState<boolean>(true);
  const [saving, setSaving] = useState<boolean>(false);
  const [clearingLogin, setClearingLogin] = useState<boolean>(false);
  const [activeTab, setActiveTab] = useState<string>("paths");
  const [settings, setSettings] = useState<SettingsState>(EMPTY_SETTINGS);
  const [login, setLogin] = useState<LoginState>(EMPTY_STATE.login);
  const [cloudSaveUploadState, setCloudSaveUploadState] = useState<CloudSaveUploadState>(EMPTY_CLOUD_SAVE_STATE);
  const [cloudSaveRestoreState, setCloudSaveRestoreState] = useState<CloudSaveRestoreState>(EMPTY_CLOUD_SAVE_RESTORE_STATE);
  const [startingCloudSaveUpload, setStartingCloudSaveUpload] = useState<boolean>(false);
  const [loadingRestoreOptions, setLoadingRestoreOptions] = useState<boolean>(false);
  const [loadingRestoreEntries, setLoadingRestoreEntries] = useState<boolean>(false);
  const [planningRestore, setPlanningRestore] = useState<boolean>(false);
  const [applyingRestore, setApplyingRestore] = useState<boolean>(false);
  const [restoreOptions, setRestoreOptions] = useState<CloudSaveRestoreGameOption[]>([]);
  const [selectedRestoreGameKey, setSelectedRestoreGameKey] = useState<string>("");
  const [selectedRestoreVersion, setSelectedRestoreVersion] = useState<string>("");
  const [restoreEntries, setRestoreEntries] = useState<CloudSaveRestoreEntry[]>([]);
  const [selectedRestoreEntryIds, setSelectedRestoreEntryIds] = useState<string[]>([]);
  const [targetCandidates, setTargetCandidates] = useState<string[]>([]);
  const [selectedRestoreTargetDir, setSelectedRestoreTargetDir] = useState<string>("");
  const [splitDraft, setSplitDraft] = useState<number>(16);
  const settingsContainerRef = useRef<HTMLDivElement | null>(null);
  const tabStabilityCss = useMemo(
    () => `
      .freedeck-settings-root [class*="TabContentsScroll"],
      .freedeck-settings-root [class*="TabContents"],
      .freedeck-settings-root [class*="TabContent"],
      .freedeck-settings-root [class*="ScrollPanel"] {
        scrollbar-gutter: stable both-edges !important;
        overflow-y: auto !important;
      }
      .freedeck-settings-root [class*="TabHeaderRowWrapper"],
      .freedeck-settings-root [class*="TabRowTabs"],
      .freedeck-settings-root [class*="TabsRowScroll"],
      .freedeck-settings-root [class*="TabRow"] {
        transition: none !important;
        animation: none !important;
        scroll-behavior: auto !important;
      }
      .freedeck-settings-root [role="tablist"] {
        scroll-behavior: auto !important;
      }
    `,
    [],
  );
  const selectedRestoreGame = useMemo(
    () => restoreOptions.find((item) => item.game_key === selectedRestoreGameKey) || null,
    [restoreOptions, selectedRestoreGameKey],
  );
  const restoreVersionOptions = useMemo(
    () => (selectedRestoreGame?.versions || []).map((item) => ({
      data: item.version_name,
      label: `${item.display_time || item.version_name} | ${formatBytes(item.size_bytes)}`,
    })),
    [selectedRestoreGame],
  );

  useEffect(() => {
    const classMap = getGamepadTabClassMap();
    if (!classMap) return;
    const styleId = "freedeck-settings-tabs-no-jitter";
    if (document.getElementById(styleId)) return;
    const style = document.createElement("style");
    style.id = styleId;
    const rules: string[] = [];
    if (classMap.TabsRowScroll) {
      rules.push(`.${classMap.TabsRowScroll}{scroll-behavior:auto !important;}`);
    }
    if (classMap.TabRowTabs) {
      rules.push(`.${classMap.TabRowTabs}{transition:none !important;}`);
      rules.push(`.${classMap.TabRowTabs}{scroll-snap-type:none !important;}`);
    }
    if (classMap.Tab) {
      rules.push(`.${classMap.Tab}{transition:none !important;}`);
    }
    style.textContent = rules.join("\n");
    document.head.appendChild(style);
  }, []);

  useEffect(() => {
    if (!restoreOptions.length) {
      setSelectedRestoreGameKey("");
      setSelectedRestoreVersion("");
      return;
    }
    const exists = restoreOptions.some((item) => item.game_key === selectedRestoreGameKey);
    if (!exists) {
      const fallback = restoreOptions.find((item) => item.available && item.versions.length > 0);
      setSelectedRestoreGameKey(String(fallback?.game_key || ""));
    }
  }, [restoreOptions, selectedRestoreGameKey]);

  useEffect(() => {
    if (!selectedRestoreGame) {
      setSelectedRestoreVersion("");
      return;
    }
    const exists = selectedRestoreGame.versions.some((item) => item.version_name === selectedRestoreVersion);
    if (!exists) {
      setSelectedRestoreVersion(String(selectedRestoreGame.versions[0]?.version_name || ""));
    }
  }, [selectedRestoreGame, selectedRestoreVersion]);

  useEffect(() => {
    setRestoreEntries([]);
    setSelectedRestoreEntryIds([]);
    setTargetCandidates([]);
    setSelectedRestoreTargetDir("");
  }, [selectedRestoreGameKey, selectedRestoreVersion]);

  const refreshSettings = useCallback(async () => {
    const result = await withTimeout(
      getTianyiPanelState(),
      PANEL_REQUEST_TIMEOUT_MS,
      "读取设置超时，请稍后重试",
    );
    if (result.status !== "success" || !result.data) {
      throw new Error(result.message || "读取设置失败");
    }
    const next = Object.assign({}, EMPTY_SETTINGS, result.data.settings || {});
    setSettings(next);
    setLogin(result.data.login || EMPTY_STATE.login);
    setSplitDraft(Math.max(1, Math.min(64, Number(next.split_count || 16))));
  }, []);

  const refreshCloudSaveUploadStatus = useCallback(async () => {
    const result = await getTianyiCloudSaveUploadStatus();
    if (result.status !== "success") {
      throw new Error(result.message || "读取云存档状态失败");
    }
    const nextState = normalizeCloudSaveUploadState(result.data?.state);
    setCloudSaveUploadState(nextState);
  }, []);

  const refreshCloudSaveRestoreStatus = useCallback(async () => {
    const result = await getTianyiCloudSaveRestoreStatus();
    if (result.status !== "success") {
      throw new Error(result.message || "读取恢复状态失败");
    }
    const nextState = normalizeCloudSaveRestoreState(result.data?.state);
    setCloudSaveRestoreState(nextState);
    if (Array.isArray(nextState.selected_entry_ids) && nextState.selected_entry_ids.length > 0 && selectedRestoreEntryIds.length === 0) {
      setSelectedRestoreEntryIds(nextState.selected_entry_ids);
    }
  }, [selectedRestoreEntryIds.length]);

  const refreshRestoreOptions = useCallback(async () => {
    if (loadingRestoreOptions) return;
    setLoadingRestoreOptions(true);
    try {
      const result = await listTianyiCloudSaveRestoreOptions();
      if (result.status !== "success" || !result.data) {
        throw new Error(result.message || "读取云存档列表失败");
      }
      const games = Array.isArray(result.data.games) ? result.data.games : [];
      setRestoreOptions(games);
      const fallback = games.find((item) => item.available && item.versions.length > 0);
      if (fallback && !selectedRestoreGameKey) {
        setSelectedRestoreGameKey(String(fallback.game_key || ""));
      }
      toaster.toast({ title: "Freedeck", body: `已刷新云存档版本（游戏 ${games.length} 个）` });
    } catch (error) {
      toaster.toast({ title: "Freedeck", body: String(error) });
    } finally {
      setLoadingRestoreOptions(false);
    }
  }, [loadingRestoreOptions, selectedRestoreGameKey]);

  useEffect(() => {
    let alive = true;
    (async () => {
      try {
        await refreshSettings();
        await refreshCloudSaveUploadStatus();
        await refreshCloudSaveRestoreStatus();
      } catch (error) {
        if (alive) toaster.toast({ title: "Freedeck", body: String(error) });
      } finally {
        if (alive) setLoading(false);
      }
    })();
    return () => {
      alive = false;
    };
  }, [refreshSettings, refreshCloudSaveRestoreStatus, refreshCloudSaveUploadStatus]);

  useEffect(() => {
    if (!cloudSaveUploadState.running) return;
    let alive = true;
    let timer = 0;
    const poll = async () => {
      if (!alive) return;
      try {
        await refreshCloudSaveUploadStatus();
      } catch {
        // 轮询失败时保留当前状态，避免干扰 UI。
      } finally {
        if (alive) timer = window.setTimeout(poll, 1200);
      }
    };
    timer = window.setTimeout(poll, 1200);
    return () => {
      alive = false;
      if (timer) window.clearTimeout(timer);
    };
  }, [cloudSaveUploadState.running, refreshCloudSaveUploadStatus]);

  useEffect(() => {
    if (!cloudSaveRestoreState.running) return;
    let alive = true;
    let timer = 0;
    const poll = async () => {
      if (!alive) return;
      try {
        await refreshCloudSaveRestoreStatus();
      } catch {
        // 轮询失败时保留当前状态，避免干扰 UI。
      } finally {
        if (alive) timer = window.setTimeout(poll, 1200);
      }
    };
    timer = window.setTimeout(poll, 1200);
    return () => {
      alive = false;
      if (timer) window.clearTimeout(timer);
    };
  }, [cloudSaveRestoreState.running, refreshCloudSaveRestoreStatus]);

  const savePatch = useCallback(
    async (patch: Partial<SettingsState>, successMessage = "设置已保存") => {
      if (saving) return;
      setSaving(true);
      try {
        const merged = Object.assign({}, settings, patch);
        const result = await setTianyiSettings(toPayload(merged));
        if (result.status !== "success" || !result.data) {
          throw new Error(result.message || "保存设置失败");
        }
        const next = Object.assign({}, EMPTY_SETTINGS, result.data || {});
        setSettings(next);
        setSplitDraft(Math.max(1, Math.min(64, Number(next.split_count || 16))));
        toaster.toast({ title: "Freedeck", body: successMessage });
      } catch (error) {
        toaster.toast({ title: "Freedeck", body: String(error) });
      } finally {
        setSaving(false);
      }
    },
    [saving, settings],
  );

  const onPickDownloadDir = useCallback(async () => {
    try {
      const selected = await pickFolder(settings.download_dir || "/home/deck");
      if (!selected) return;
      await savePatch({ download_dir: selected }, `下载目录已更新：${selected}`);
    } catch (error) {
      toaster.toast({ title: "Freedeck", body: `选择下载目录失败：${error}` });
    }
  }, [savePatch, settings.download_dir]);

  const onPickInstallDir = useCallback(async () => {
    try {
      const selected = await pickFolder(settings.install_dir || settings.download_dir || "/home/deck");
      if (!selected) return;
      await savePatch({ install_dir: selected }, `安装目录已更新：${selected}`);
    } catch (error) {
      toaster.toast({ title: "Freedeck", body: `选择安装目录失败：${error}` });
    }
  }, [savePatch, settings.download_dir, settings.install_dir]);

  const onSaveSplit = useCallback(async () => {
    const value = Number(splitDraft);
    if (!Number.isFinite(value) || value < 1 || value > 64) {
      toaster.toast({ title: "Freedeck", body: "分片数必须在 1 到 64 之间" });
      return;
    }
    await savePatch({ split_count: value }, "下载参数已更新");
  }, [savePatch, splitDraft]);

  const onClearLogin = useCallback(async () => {
    if (clearingLogin) return;
    setClearingLogin(true);
    try {
      const result = await clearTianyiLogin();
      if (result.status !== "success") {
        throw new Error(result.message || "注销失败");
      }
      await refreshSettings();
      await refreshCloudSaveUploadStatus();
      await refreshCloudSaveRestoreStatus();
      setRestoreOptions([]);
      setSelectedRestoreGameKey("");
      setSelectedRestoreVersion("");
      setRestoreEntries([]);
      setSelectedRestoreEntryIds([]);
      setTargetCandidates([]);
      setSelectedRestoreTargetDir("");
      const body = String(result.data?.message || "已注销天翼云账号");
      toaster.toast({ title: "Freedeck", body });
    } catch (error) {
      toaster.toast({ title: "Freedeck", body: String(error) });
    } finally {
      setClearingLogin(false);
    }
  }, [clearingLogin, refreshCloudSaveRestoreStatus, refreshCloudSaveUploadStatus, refreshSettings]);

  const onStartCloudSaveUpload = useCallback(async () => {
    if (startingCloudSaveUpload) return;
    setStartingCloudSaveUpload(true);
    try {
      const result = await startTianyiCloudSaveUpload();
      if (result.status !== "success") {
        throw new Error(result.message || "启动云存档上传失败");
      }
      const data = result.data;
      if (data?.state) {
        setCloudSaveUploadState(normalizeCloudSaveUploadState(data.state));
      }
      toaster.toast({ title: "Freedeck", body: String(data?.message || "云存档上传任务已启动") });
      await refreshCloudSaveUploadStatus();
    } catch (error) {
      toaster.toast({ title: "Freedeck", body: String(error) });
    } finally {
      setStartingCloudSaveUpload(false);
    }
  }, [refreshCloudSaveUploadStatus, startingCloudSaveUpload]);

  const onLoadRestoreEntries = useCallback(async () => {
    if (loadingRestoreEntries) return;
    if (!selectedRestoreGame || !selectedRestoreVersion) {
      toaster.toast({ title: "Freedeck", body: "请先选择游戏和版本" });
      return;
    }
    setLoadingRestoreEntries(true);
    try {
      const result = await listTianyiCloudSaveRestoreEntries({
        game_id: String(selectedRestoreGame.game_id || ""),
        game_key: String(selectedRestoreGame.game_key || ""),
        game_title: String(selectedRestoreGame.game_title || ""),
        version_name: String(selectedRestoreVersion || ""),
      });
      if (result.status !== "success" || !result.data) {
        throw new Error(result.message || "读取存档项失败");
      }
      const rows = Array.isArray(result.data.entries) ? result.data.entries : [];
      setRestoreEntries(rows);
      setSelectedRestoreEntryIds(rows.map((item) => String(item.entry_id || "")).filter(Boolean));
      setTargetCandidates([]);
      setSelectedRestoreTargetDir("");
      toaster.toast({ title: "Freedeck", body: `已读取存档项 ${rows.length} 个` });
      await refreshCloudSaveRestoreStatus();
    } catch (error) {
      toaster.toast({ title: "Freedeck", body: String(error) });
    } finally {
      setLoadingRestoreEntries(false);
    }
  }, [loadingRestoreEntries, refreshCloudSaveRestoreStatus, selectedRestoreGame, selectedRestoreVersion]);

  const toggleRestoreEntry = useCallback((entryId: string, checked: boolean) => {
    const id = String(entryId || "").trim();
    if (!id) return;
    setSelectedRestoreEntryIds((prev) => {
      const set = new Set(prev.map((item) => String(item || "").trim()).filter(Boolean));
      if (checked) set.add(id);
      else set.delete(id);
      return Array.from(set);
    });
  }, []);

  const runRestoreApply = useCallback(
    async (planId: string, confirmOverwrite: boolean) => {
      const normalizedPlanId = String(planId || "").trim();
      if (!normalizedPlanId) return;
      setApplyingRestore(true);
      try {
        const result = await applyTianyiCloudSaveRestore({
          plan_id: normalizedPlanId,
          confirm_overwrite: Boolean(confirmOverwrite),
        });
        if (result.status !== "success" || !result.data) {
          throw new Error(result.message || "执行恢复失败");
        }
        toaster.toast({ title: "Freedeck", body: String(result.data.message || "恢复流程已结束") });
      } catch (error) {
        toaster.toast({ title: "Freedeck", body: String(error) });
      } finally {
        setApplyingRestore(false);
        await refreshCloudSaveRestoreStatus();
      }
    },
    [refreshCloudSaveRestoreStatus],
  );

  const onStartCloudSaveRestore = useCallback(async () => {
    if (planningRestore || applyingRestore) return;
    if (!selectedRestoreGame || !selectedRestoreVersion) {
      toaster.toast({ title: "Freedeck", body: "请先选择要恢复的游戏和版本" });
      return;
    }
    if (!restoreEntries.length) {
      toaster.toast({ title: "Freedeck", body: "请先读取存档项" });
      return;
    }
    if (!selectedRestoreEntryIds.length) {
      toaster.toast({ title: "Freedeck", body: "请至少选择一个存档项" });
      return;
    }

    setPlanningRestore(true);
    try {
      const result = await planTianyiCloudSaveRestore({
        game_id: String(selectedRestoreGame.game_id || ""),
        game_key: String(selectedRestoreGame.game_key || ""),
        game_title: String(selectedRestoreGame.game_title || ""),
        version_name: String(selectedRestoreVersion || ""),
        selected_entry_ids: selectedRestoreEntryIds,
        target_dir: String(selectedRestoreTargetDir || ""),
      });
      if (result.status !== "success" || !result.data) {
        throw new Error(result.message || "生成恢复计划失败");
      }

      const plan = result.data;
      const nextCandidates = Array.isArray(plan.target_candidates) ? plan.target_candidates : [];
      setTargetCandidates(nextCandidates);
      if (nextCandidates.length === 1) {
        setSelectedRestoreTargetDir(nextCandidates[0]);
      } else if (nextCandidates.length > 1 && !selectedRestoreTargetDir) {
        setSelectedRestoreTargetDir(nextCandidates[0]);
      }

      if (!plan.accepted) {
        toaster.toast({ title: "Freedeck", body: String(plan.message || "请先完成恢复前置步骤") });
        await refreshCloudSaveRestoreStatus();
        return;
      }

      const planId = String(plan.plan_id || "");
      if (!planId) {
        throw new Error("恢复计划缺少 plan_id");
      }
      if (plan.requires_confirmation) {
        const samples = Array.isArray(plan.conflict_samples) ? plan.conflict_samples.slice(0, 5) : [];
        showModal(
          <ConfirmModal
            strTitle="确认覆盖存档"
            strDescription={
              `检测到 ${Number(plan.conflict_count || 0)} 个冲突文件，确认后会覆盖原有存档。` +
              (samples.length ? `\n\n示例：\n${samples.join("\n")}` : "")
            }
            strOKButtonText="确认覆盖"
            strCancelButtonText="取消"
            onOK={() => {
              void runRestoreApply(planId, true);
            }}
            onCancel={() => {
              void runRestoreApply(planId, false);
            }}
          />,
        );
      } else {
        await runRestoreApply(planId, false);
      }
      await refreshCloudSaveRestoreStatus();
    } catch (error) {
      toaster.toast({ title: "Freedeck", body: String(error) });
    } finally {
      setPlanningRestore(false);
    }
  }, [
    applyingRestore,
    planningRestore,
    refreshCloudSaveRestoreStatus,
    restoreEntries.length,
    runRestoreApply,
    selectedRestoreEntryIds,
    selectedRestoreGame,
    selectedRestoreTargetDir,
    selectedRestoreVersion,
  ]);

  const cloudSaveSummaryText = useMemo(() => {
    const current = cloudSaveUploadState;
    if (current.running) {
      const stage = cloudSaveStageText(current.stage);
      return `${stage}：${current.processed_games}/${current.total_games}，成功 ${current.uploaded}，跳过 ${current.skipped}，失败 ${current.failed}`;
    }
    const last = current.last_result;
    if (last && Number(last.finished_at || 0) > 0) {
      const stage = cloudSaveStageText(last.stage);
      return `最近一次：${stage}，成功 ${last.uploaded}，跳过 ${last.skipped}，失败 ${last.failed}`;
    }
    return "尚未执行云存档上传";
  }, [cloudSaveUploadState]);

  const cloudSaveRestoreSummaryText = useMemo(() => {
    const current = cloudSaveRestoreState;
    if (current.running) {
      const stage = cloudSaveStageText(current.stage);
      return `${stage}：已恢复 ${current.restored_files} 个文件`;
    }
    const last = current.last_result;
    if (last && String(last.message || "").trim()) {
      return `最近一次：${last.message}`;
    }
    return "尚未执行云存档恢复";
  }, [cloudSaveRestoreState]);

  const restoreGameOptions = useMemo(
    () =>
      restoreOptions.map((item) => ({
        data: item.game_key,
        label: item.available
          ? `${item.game_title}（${item.versions.length} 个版本）`
          : `${item.game_title}（不可恢复：${item.reason || "无可用版本"}）`,
      })),
    [restoreOptions],
  );

  const restoreBusy =
    saving ||
    clearingLogin ||
    loadingRestoreOptions ||
    loadingRestoreEntries ||
    planningRestore ||
    applyingRestore ||
    cloudSaveRestoreState.running;

  const tabs = useMemo(
    () => [
      {
        id: "paths",
        title: "路径",
        content: (
          <>
            <PanelSection title="下载目录">
              <PanelSectionRow>
                <div style={{ fontSize: "12px", lineHeight: 1.5 }}>{settings.download_dir || "未设置"}</div>
              </PanelSectionRow>
              <PanelSectionRow>
                <ButtonItem layout="below" onClick={onPickDownloadDir} disabled={saving}>
                  选择下载目录
                </ButtonItem>
              </PanelSectionRow>
            </PanelSection>
            <PanelSection title="安装目录">
              <PanelSectionRow>
                <div style={{ fontSize: "12px", lineHeight: 1.5 }}>{settings.install_dir || "未设置"}</div>
              </PanelSectionRow>
              <PanelSectionRow>
                <ButtonItem layout="below" onClick={onPickInstallDir} disabled={saving}>
                  选择安装目录
                </ButtonItem>
              </PanelSectionRow>
            </PanelSection>
          </>
        ),
      },
      {
        id: "install",
        title: "安装",
        content: (
          <PanelSection title="安装行为">
            <PanelSectionRow>
              <ToggleField
                label="安装后自动删除压缩包"
                description="仅在自动安装成功后生效"
                checked={Boolean(settings.auto_delete_package)}
                onChange={(value: boolean) => savePatch({ auto_delete_package: value })}
                disabled={saving}
              />
            </PanelSectionRow>
            <PanelSectionRow>
              <div style={{ fontSize: "12px", lineHeight: 1.5, opacity: 0.86 }}>
                下载完成后自动安装已固定开启。
              </div>
            </PanelSectionRow>
          </PanelSection>
        ),
      },
      {
        id: "download",
        title: "下载",
        content: (
          <PanelSection title="下载参数">
            <PanelSectionRow>
              <SliderField
                label="aria2 分片数"
                description="建议 8~32，范围 1~64"
                value={Math.max(1, Math.min(64, Number(splitDraft || 16)))}
                min={1}
                max={64}
                step={1}
                showValue
                editableValue
                onChange={(value: number) => setSplitDraft(Math.max(1, Math.min(64, Number(value || 1))))}
                disabled={saving}
              />
            </PanelSectionRow>
            <PanelSectionRow>
              <DropdownItem
                label="分页数量"
                description="游戏列表每页显示条数"
                rgOptions={PAGE_SIZE_OPTIONS}
                selectedOption={Math.max(10, Math.min(200, Number(settings.page_size || 50)))}
                disabled={saving}
                onChange={(option) => {
                  const nextValue = Math.max(10, Math.min(200, Number(option?.data || 50)));
                  if (nextValue === Number(settings.page_size || 50)) return;
                  void savePatch({ page_size: nextValue }, `分页数量已更新：${nextValue} / 页`);
                }}
              />
            </PanelSectionRow>
            <PanelSectionRow>
              <ButtonItem layout="below" onClick={onSaveSplit} disabled={saving}>
                保存下载参数
              </ButtonItem>
            </PanelSectionRow>
          </PanelSection>
        ),
      },
      {
        id: "account",
        title: "账号",
        content: (
          <>
            <PanelSection title="天翼云账号">
              <PanelSectionRow>
                <div style={{ fontSize: "12px", lineHeight: 1.5 }}>
                  {login.logged_in
                    ? `当前已登录：${login.user_account || "未知账号"}`
                    : "当前未登录"}
                </div>
              </PanelSectionRow>
              <PanelSectionRow>
                <ButtonItem
                  layout="below"
                  onClick={onClearLogin}
                  disabled={clearingLogin || saving || !login.logged_in}
                >
                  {clearingLogin ? "注销中..." : "注销天翼云账号"}
                </ButtonItem>
              </PanelSectionRow>
            </PanelSection>

            <PanelSection title="云存档上传">
              <PanelSectionRow>
                <ButtonItem
                  layout="below"
                  onClick={onStartCloudSaveUpload}
                  disabled={saving || clearingLogin || startingCloudSaveUpload || !login.logged_in || cloudSaveUploadState.running}
                >
                  {startingCloudSaveUpload
                    ? "启动中..."
                    : cloudSaveUploadState.running
                      ? "上传进行中..."
                      : "上传云存档"}
                </ButtonItem>
              </PanelSectionRow>
              <PanelSectionRow>
                <div style={{ fontSize: "12px", lineHeight: 1.5 }}>{cloudSaveSummaryText}</div>
              </PanelSectionRow>
              <PanelSectionRow>
                <div style={{ fontSize: "11px", lineHeight: 1.5, opacity: 0.88 }}>
                  {`${cloudSaveUploadState.message || "未开始"} | ${clampProgress(cloudSaveUploadState.progress).toFixed(1)}%`}
                </div>
              </PanelSectionRow>
            </PanelSection>

            <PanelSection title="云存档下载恢复">
              <PanelSectionRow>
                <ButtonItem
                  layout="below"
                  onClick={refreshRestoreOptions}
                  disabled={!login.logged_in || restoreBusy}
                >
                  {loadingRestoreOptions ? "刷新中..." : "刷新云存档列表"}
                </ButtonItem>
              </PanelSectionRow>
              <PanelSectionRow>
                <DropdownItem
                  label="选择游戏"
                  description="按游戏分组显示云端版本"
                  rgOptions={restoreGameOptions}
                  selectedOption={selectedRestoreGameKey}
                  disabled={!login.logged_in || restoreBusy || restoreGameOptions.length <= 0}
                  onChange={(option) => setSelectedRestoreGameKey(String(option?.data || ""))}
                />
              </PanelSectionRow>
              <PanelSectionRow>
                <DropdownItem
                  label="选择版本时间"
                  description="按时间倒序"
                  rgOptions={restoreVersionOptions}
                  selectedOption={selectedRestoreVersion}
                  disabled={!login.logged_in || restoreBusy || !selectedRestoreGame || restoreVersionOptions.length <= 0}
                  onChange={(option) => setSelectedRestoreVersion(String(option?.data || ""))}
                />
              </PanelSectionRow>
              <PanelSectionRow>
                <ButtonItem
                  layout="below"
                  onClick={onLoadRestoreEntries}
                  disabled={!login.logged_in || restoreBusy || !selectedRestoreGame || !selectedRestoreVersion}
                >
                  {loadingRestoreEntries ? "读取中..." : "读取可选存档项"}
                </ButtonItem>
              </PanelSectionRow>

              {restoreEntries.length > 0 &&
                restoreEntries.map((entry) => {
                  const entryId = String(entry.entry_id || "");
                  const checked = selectedRestoreEntryIds.includes(entryId);
                  return (
                    <PanelSectionRow key={`restore_entry_${entryId}`}>
                      <ToggleField
                        label={`${entry.entry_name || entryId}${entry.file_count ? `（${entry.file_count} 文件）` : ""}`}
                        checked={checked}
                        onChange={(value: boolean) => toggleRestoreEntry(entryId, value)}
                        disabled={restoreBusy}
                      />
                    </PanelSectionRow>
                  );
                })}

              {restoreEntries.length > 0 && (
                <PanelSectionRow>
                  <div style={{ display: "flex", gap: "8px", width: "100%" }}>
                    <ButtonItem
                      layout="below"
                      onClick={() => {
                        setSelectedRestoreEntryIds(
                          restoreEntries.map((item) => String(item.entry_id || "")).filter(Boolean),
                        );
                      }}
                      disabled={restoreBusy}
                    >
                      全选
                    </ButtonItem>
                    <ButtonItem
                      layout="below"
                      onClick={() => setSelectedRestoreEntryIds([])}
                      disabled={restoreBusy}
                    >
                      清空
                    </ButtonItem>
                  </div>
                </PanelSectionRow>
              )}

              {targetCandidates.length > 1 && (
                <PanelSectionRow>
                  <DropdownItem
                    label="恢复目标目录"
                    description="检测到多个候选目录，请明确选择"
                    rgOptions={targetCandidates.map((path) => ({ data: path, label: path }))}
                    selectedOption={selectedRestoreTargetDir}
                    disabled={restoreBusy}
                    onChange={(option) => setSelectedRestoreTargetDir(String(option?.data || ""))}
                  />
                </PanelSectionRow>
              )}

              <PanelSectionRow>
                <ButtonItem
                  layout="below"
                  onClick={onStartCloudSaveRestore}
                  disabled={!login.logged_in || restoreBusy || !selectedRestoreGame || !selectedRestoreVersion || !selectedRestoreEntryIds.length}
                >
                  {planningRestore ? "规划中..." : applyingRestore ? "恢复中..." : "下载并恢复云存档"}
                </ButtonItem>
              </PanelSectionRow>
              <PanelSectionRow>
                <div style={{ fontSize: "12px", lineHeight: 1.5 }}>{cloudSaveRestoreSummaryText}</div>
              </PanelSectionRow>
              <PanelSectionRow>
                <div style={{ fontSize: "11px", lineHeight: 1.5, opacity: 0.88 }}>
                  {`${cloudSaveRestoreState.message || "未开始"} | ${clampProgress(cloudSaveRestoreState.progress).toFixed(1)}%`}
                </div>
              </PanelSectionRow>
            </PanelSection>
          </>
        ),
      },
    ],
    [
      clearingLogin,
      login.logged_in,
      login.user_account,
      onPickDownloadDir,
      onPickInstallDir,
      onClearLogin,
      onStartCloudSaveUpload,
      onSaveSplit,
      savePatch,
      saving,
      cloudSaveSummaryText,
      cloudSaveUploadState.message,
      cloudSaveUploadState.progress,
      cloudSaveUploadState.running,
      startingCloudSaveUpload,
      settings.auto_delete_package,
      settings.page_size,
      settings.download_dir,
      settings.install_dir,
      splitDraft,
      refreshRestoreOptions,
      restoreBusy,
      loadingRestoreOptions,
      restoreGameOptions,
      selectedRestoreGameKey,
      selectedRestoreVersion,
      selectedRestoreGame,
      restoreVersionOptions,
      onLoadRestoreEntries,
      loadingRestoreEntries,
      restoreEntries,
      selectedRestoreEntryIds,
      toggleRestoreEntry,
      targetCandidates,
      selectedRestoreTargetDir,
      onStartCloudSaveRestore,
      planningRestore,
      applyingRestore,
      cloudSaveRestoreSummaryText,
      cloudSaveRestoreState.message,
      cloudSaveRestoreState.progress,
    ],
  );

  const focusSettingsTabRow = useCallback(
    (tabId?: string) => {
      const classMap = getGamepadTabClassMap();
      if (!classMap || !settingsContainerRef.current) return;
      const tabClass = classMap.Tab;
      if (!tabClass) return;

      let target: HTMLElement | null = null;
      if (tabId) {
        const tabTitle = String(tabs.find((tab) => tab.id === tabId)?.title || "").trim();
        if (tabTitle) {
          const elements = settingsContainerRef.current.querySelectorAll(`.${tabClass}`);
          for (const element of Array.from(elements)) {
            const item = element as HTMLElement;
            if (String(item.textContent || "").trim() === tabTitle) {
              target = item;
              break;
            }
          }
        }
      }
      if (!target) {
        const activeClass = classMap.Active || classMap.Selected;
        const selector = activeClass ? `.${tabClass}.${activeClass}` : `.${tabClass}`;
        target = settingsContainerRef.current.querySelector(selector) as HTMLElement | null;
      }
      target?.focus?.();
    },
    [tabs],
  );

  useEffect(() => {
    const classMap = getGamepadTabClassMap();
    if (!classMap) return;
    const rowClass = classMap.TabsRowScroll || classMap.TabRowTabs;
    if (!rowClass) return;
    const handle = window.requestAnimationFrame(() => {
      const root = settingsContainerRef.current;
      if (!root) return;
      const row = root.querySelector(`.${rowClass}`) as HTMLElement | null;
      if (!row) return;
      row.style.scrollBehavior = "auto";
      row.scrollLeft = 0;
    });
    return () => window.cancelAnimationFrame(handle);
  }, [activeTab]);

  const onShowSettingsTab = useCallback(
    (tabId: string) => {
      focusSettingsTabRow();
      setActiveTab(tabId);
      window.requestAnimationFrame(() => focusSettingsTabRow(tabId));
    },
    [focusSettingsTabRow],
  );

  if (loading) {
    return (
      <PanelSection title="Freedeck 设置">
        <PanelSectionRow>加载中...</PanelSectionRow>
      </PanelSection>
    );
  }

  return (
    <div
      ref={settingsContainerRef}
      className="freedeck-settings-root"
      style={{
        paddingTop: 48,
        paddingBottom: 24,
        minHeight: "100%",
        boxSizing: "border-box",
        overflowX: "hidden",
      }}
    >
      <style>{tabStabilityCss}</style>
      <Tabs tabs={tabs} activeTab={activeTab} onShowTab={onShowSettingsTab} autoFocusContents={false} />
    </div>
  );
}

function Content() {
  const [loading, setLoading] = useState<boolean>(true);
  const [state, setState] = useState<PanelState>(EMPTY_STATE);
  const [openingLogin, setOpeningLogin] = useState<boolean>(false);
  const [openingLibrary, setOpeningLibrary] = useState<boolean>(false);
  const [uninstallingKey, setUninstallingKey] = useState<string>("");
  const syncingRef = useRef<boolean>(false);
  const latestStateRef = useRef<PanelState>(EMPTY_STATE);

  const syncState = useCallback(async (pollMode: PanelPollMode = PANEL_POLL_MODE_IDLE) => {
    if (syncingRef.current) return;
    syncingRef.current = true;
    try {
      const result = await withTimeout(
        getTianyiPanelState({
          poll_mode: pollMode,
          visible: !document.hidden,
          has_focus: document.hasFocus(),
        }),
        PANEL_REQUEST_TIMEOUT_MS,
        "读取面板状态超时，请稍后重试",
      );
      if (result.status !== "success" || !result.data) {
        throw new Error(result.message || "读取状态失败");
      }
      const next = result.data;
      const normalized: PanelState = {
        login: next.login || EMPTY_STATE.login,
        installed: next.installed || EMPTY_STATE.installed,
        tasks: next.tasks || [],
        settings: Object.assign({}, EMPTY_SETTINGS, next.settings || {}),
        library_url: next.library_url || "",
        power_diagnostics: next.power_diagnostics || {},
      };
      latestStateRef.current = normalized;
      setState(normalized);
    } finally {
      syncingRef.current = false;
    }
  }, []);

  useEffect(() => {
    let alive = true;
    let firstRun = true;
    let timer = 0;

    const clearTimer = () => {
      if (!timer) return;
      window.clearTimeout(timer);
      timer = 0;
    };

    const scheduleNext = () => {
      if (!alive) return;
      clearTimer();
      const mode = resolvePanelPollMode(latestStateRef.current);
      const delay = pollIntervalByMode(mode);
      timer = window.setTimeout(() => {
        void runPoll(false);
      }, delay);
    };

    const runPoll = async (showErrorToast: boolean) => {
      if (!alive) return;
      const mode = resolvePanelPollMode(latestStateRef.current);
      try {
        await syncState(mode);
      } catch (error) {
        if (showErrorToast && alive) {
          toaster.toast({ title: "Freedeck", body: String(error) });
        }
      } finally {
        if (firstRun) {
          firstRun = false;
          if (alive) setLoading(false);
        }
        scheduleNext();
      }
    };

    const handleVisibilityChange = () => {
      if (!alive) return;
      clearTimer();
      if (document.hidden) {
        scheduleNext();
        return;
      }
      void runPoll(false);
    };

    const handleFocus = () => {
      if (!alive) return;
      clearTimer();
      void runPoll(false);
    };

    const handleBlur = () => {
      if (!alive) return;
      clearTimer();
      scheduleNext();
    };

    void runPoll(true);
    document.addEventListener("visibilitychange", handleVisibilityChange);
    window.addEventListener("focus", handleFocus);
    window.addEventListener("blur", handleBlur);

    return () => {
      alive = false;
      clearTimer();
      document.removeEventListener("visibilitychange", handleVisibilityChange);
      window.removeEventListener("focus", handleFocus);
      window.removeEventListener("blur", handleBlur);
    };
  }, [syncState]);

  const onLogin = useCallback(async () => {
    if (openingLogin) return;
    setOpeningLogin(true);
    try {
      const result = await getTianyiLoginUrl();
      if (result.status !== "success") {
        throw new Error(describeOpenError(result, "获取登录地址失败"));
      }
      openExternalUrl(result.url || result.data?.url || "");
    } catch (error) {
      toaster.toast({ title: "Freedeck", body: String(error) });
    } finally {
      setOpeningLogin(false);
    }
  }, [openingLogin]);

  const onOpenLibrary = useCallback(async () => {
    if (openingLibrary) return;
    setOpeningLibrary(true);
    try {
      const result = await getTianyiLibraryUrl();
      if (result.status !== "success") {
        throw new Error(describeOpenError(result, "获取游戏列表地址失败"));
      }
      openExternalUrl(result.url || result.data?.url || state.library_url || "");
    } catch (error) {
      toaster.toast({ title: "Freedeck", body: String(error) });
    } finally {
      setOpeningLibrary(false);
    }
  }, [openingLibrary, state.library_url]);

  const onOpenSettings = useCallback(() => {
    try {
      Router.CloseSideMenus?.();
    } catch {
      // 忽略菜单关闭失败。
    }
    try {
      Navigation.Navigate(SETTINGS_ROUTE);
    } catch (error) {
      toaster.toast({ title: "Freedeck", body: `打开设置失败：${error}` });
    }
  }, []);

  const installedGames = useMemo(() => state.installed?.preview || [], [state.installed]);
  const installedGameIds = useMemo(() => {
    const set = new Set<string>();
    for (const item of installedGames) {
      const gameId = String(item.game_id || "").trim();
      if (gameId) set.add(gameId);
    }
    return set;
  }, [installedGames]);
  const visibleTasks = useMemo(
    () =>
      (state.tasks || []).filter((task) => {
        if (isTaskAlreadyInstalled(task)) return false;
        const gameId = String(task.game_id || "").trim();
        if (gameId && installedGameIds.has(gameId)) return false;
        return true;
      }),
    [installedGameIds, state.tasks],
  );
  const sortedInstalledGames = useMemo(() => {
    const list = [...installedGames];
    list.sort((a, b) =>
      String(a.title || "").localeCompare(String(b.title || ""), "zh-Hans-CN", {
        sensitivity: "base",
        numeric: true,
      }),
    );
    return list;
  }, [installedGames]);
  const loginStatusText = useMemo(() => {
    if (!state.login.logged_in) return "没登录";
    const account = String(state.login.user_account || "").trim() || "未知账号";
    return `已登录：${account}（账号）`;
  }, [state.login.logged_in, state.login.user_account]);

  const performUninstallInstalledGame = useCallback(
    async (item: InstalledGameItem) => {
      const gameId = String(item.game_id || "").trim();
      const installPath = String(item.install_path || "").trim();
      const title = String(item.title || gameId || "该游戏");
      if (!installPath) {
        toaster.toast({ title: "Freedeck", body: "安装路径为空，无法卸载" });
        return;
      }
      if (uninstallingKey) return;

      const key = `${gameId}::${installPath}`;
      setUninstallingKey(key);
      try {
        const result = await uninstallTianyiInstalledGame({
          game_id: gameId,
          install_path: installPath,
          delete_files: true,
        });
        if (result.status !== "success") {
          throw new Error(result.message || "卸载失败");
        }
        toaster.toast({ title: "Freedeck", body: `已卸载：${title}` });
        await syncState();
      } catch (error) {
        toaster.toast({ title: "Freedeck", body: String(error) });
      } finally {
        setUninstallingKey("");
      }
    },
    [syncState, uninstallingKey],
  );

  const onUninstallInstalledGame = useCallback(
    (item: InstalledGameItem) => {
      const gameId = String(item.game_id || "").trim();
      const installPath = String(item.install_path || "").trim();
      const title = String(item.title || gameId || "该游戏");
      if (!installPath) {
        toaster.toast({ title: "Freedeck", body: "安装路径为空，无法卸载" });
        return;
      }
      if (uninstallingKey) return;
      showModal(
        <ConfirmModal
          strTitle="确认卸载"
          strDescription={`确定卸载「${title}」吗？\n\n将删除安装目录：\n${installPath}`}
          strOKButtonText="确认卸载"
          strCancelButtonText="取消"
          onOK={() => {
            void performUninstallInstalledGame(item);
          }}
          onCancel={() => {}}
        />,
      );
    },
    [performUninstallInstalledGame, uninstallingKey],
  );

  if (loading) {
    return (
      <PanelSection>
        <PanelSectionRow>加载中...</PanelSectionRow>
      </PanelSection>
    );
  }

  return (
    <>
      <PanelSection>
        <PanelSectionRow>
          <div
            style={{
              width: "100%",
              overflow: "hidden",
              textOverflow: "ellipsis",
              whiteSpace: "nowrap",
            }}
            title={loginStatusText}
          >
            {loginStatusText}
          </div>
        </PanelSectionRow>
        <PanelSectionRow>
          {!state.login.logged_in && (
            <ButtonItem layout="below" onClick={onLogin} disabled={openingLogin || openingLibrary}>
              {openingLogin ? "登录入口准备中..." : "登录"}
            </ButtonItem>
          )}
          <ButtonItem layout="below" onClick={onOpenLibrary} disabled={openingLibrary || openingLogin}>
            {openingLibrary ? "游戏列表准备中..." : "游戏列表"}
          </ButtonItem>
          <ButtonItem layout="below" onClick={onOpenSettings}>设置</ButtonItem>
        </PanelSectionRow>
      </PanelSection>

      {visibleTasks.length > 0 && (
        <PanelSection title={`下载列表（${visibleTasks.length}）`}>
          {visibleTasks.slice(0, 20).map((task) => (
            <PanelSectionRow key={task.task_id}>
              {TaskProgressRow(task)}
            </PanelSectionRow>
          ))}
        </PanelSection>
      )}

      {sortedInstalledGames.length > 0 && (
        <PanelSection title={`游戏预览（已安装 ${state.installed.total || sortedInstalledGames.length}）`}>
          {sortedInstalledGames.map((item, index) => {
            const uninstallKey = `${item.game_id || ""}::${item.install_path || ""}`;
            const uninstalling = uninstallingKey === uninstallKey;
            return (
              <PanelSectionRow key={`${item.game_id || item.title || "game"}_${index}`}>
                <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", gap: "10px", width: "100%" }}>
                  <div style={{ minWidth: 0, flex: 1 }}>
                    <div
                      style={{
                        overflow: "hidden",
                        textOverflow: "ellipsis",
                        whiteSpace: "nowrap",
                      }}
                      title={item.title || "未命名游戏"}
                    >
                      {item.title || "未命名游戏"}
                    </div>
                    <div style={{ fontSize: "12px" }}>
                      {`${item.size_text || "-"}${item.status ? ` | ${item.status}` : ""}`}
                    </div>
                    <div style={{ fontSize: "12px" }}>
                      {`游玩时长：${formatPlaytimeText(item.playtime_seconds || 0, item.playtime_text)}${
                        item.playtime_active ? "（进行中）" : ""
                      }`}
                    </div>
                    <div
                      style={{
                        fontSize: "12px",
                        overflow: "hidden",
                        textOverflow: "ellipsis",
                        whiteSpace: "nowrap",
                      }}
                      title={item.install_path || "-"}
                    >
                      {item.install_path || "-"}
                    </div>
                  </div>
                  <Focusable style={{ flex: "0 0 auto" }}>
                    <DialogButton
                      onClick={() => onUninstallInstalledGame(item)}
                      onOKButton={() => onUninstallInstalledGame(item)}
                      disabled={Boolean(uninstallingKey) || uninstalling}
                      style={{
                        minWidth: "88px",
                        borderRadius: "10px",
                        border: "1px solid rgba(255, 255, 255, 0.26)",
                        background: uninstalling ? "rgba(255, 106, 106, 0.34)" : "rgba(255, 106, 106, 0.2)",
                        color: "#ffe8e8",
                      }}
                    >
                      {uninstalling ? "卸载中..." : "卸载"}
                    </DialogButton>
                  </Focusable>
                </div>
              </PanelSectionRow>
            );
          })}
        </PanelSection>
      )}
    </>
  );
}

export default definePlugin(() => {
  routerHook.addRoute(SETTINGS_ROUTE, SettingsPage);
  const unpatchLibraryPlaytime = installLibraryPlaytimePatch();
  let uninstallGameActionReporter = () => {};
  try {
    uninstallGameActionReporter = installGlobalGameActionReporter();
  } catch {
    // 忽略全局游戏事件监听初始化失败，避免影响插件主 UI。
  }

  return {
    name: "Freedeck",
    titleView: <div className={staticClasses.Title}>Freedeck</div>,
    content: <Content />,
    icon: <FaCloudDownloadAlt />,
    onDismount() {
      uninstallGameActionReporter();
      unpatchLibraryPlaytime();
      routerHook.removeRoute(SETTINGS_ROUTE);
    },
  };
});
