/* ═══════════════════════════════════════════
   NewFreedeck — callable 绑定 + 类型
   ═══════════════════════════════════════════ */

import { callable } from "@decky/api";

/* ── 类型 ───────────────────────────────── */

export type LoginState = {
  logged_in?: boolean;
  account_name?: string;
  message?: string;
};

export type PanelStateSnapshot = {
  login?: LoginState;
  tasks?: unknown[];
  installed?: unknown[];
};

export type QrState = {
  session_id?: string;
  stage?: string;
  message?: string;
  reason?: string;
  user_account?: string;
  qr_data_url?: string;
};

export type ApiResponse<T> = {
  status?: string;
  data?: T;
  message?: string;
};

export type CatalogItem = {
  game_id: string;
  title: string;
  title_zh: string;
  title_en: string;
  platform: string;
  cover_url: string;
  down_url: string;
  pwd: string;
  size_text: string;
  size_bytes: number;
  file_name?: string;
  server?: string;
  version?: string;
  validity?: string;
  genre?: string;
  install_size_bytes?: number;
  install_size_text?: string;
  category_parent?: string;
  grouped?: boolean;
  variant_count?: number;
  multi_disc?: boolean;
  disc_count?: number;
  variants?: CatalogItem[];
};

export type StorageMount = {
  path: string;
  label: string;
  free_bytes?: number;
  total_bytes?: number;
};

export type SettingsData = {
  download_path: string;
  install_path: string;
  auto_delete_after_install: boolean;
  auto_suspend_on_download: boolean;
  // 运行
  show_playtime_widget: boolean;
  show_freedeck_library_entry: boolean;
  screen_off_auto_suspend: boolean;
  // 云存档
  cloud_save_auto_upload: boolean;
  cloud_save_provider: string;
};

export type CatalogVersion = { date?: string; csv_path?: string };

export type EmulatorStatus = {
  installed: boolean;
  version?: string;
  size_text?: string;
  loaded?: boolean;
};

export type CloudSaveState = {
  upload_status?: string;
  message?: string;
};

export type CloudSaveRecord = {
  id: string;
  file_name: string;
  direction: "upload" | "download";
  timestamp: string;
  size?: number;
};

export type CloudSaveResult = {
  uploaded?: { file_name: string; local_path: string; remote_file_id?: string; size: number }[];
  downloaded?: { file_name: string; extracted_to: string; size: number }[];
  count: number;
  message?: string;
  history?: CloudSaveRecord[];
  remote_files?: { file_name: string; size: number }[];
};

export type RuntimeRepairState = {
  repaired_count?: number;
  message?: string;
};

export type PluginReleaseState = {
  current_version?: string;
  latest_version?: string;
  has_update?: boolean;
  download_progress?: number;
  message?: string;
};

export type ArtworkAsset = {
  data: string;
  format?: string;
};

/* ── 核心 callable ─────────────────────── */

export const getPanelState = callable<[], ApiResponse<PanelStateSnapshot>>("tianyi_get_panel_state");
export const checkLogin = callable<[{ force?: boolean }?], ApiResponse<LoginState>>("tianyi_check_login");
export const clearLogin = callable<[], ApiResponse<LoginState>>("tianyi_clear_login");
export const qrStart = callable<[], ApiResponse<QrState>>("tianyi_qr_start");
export const qrStatus = callable<[string], ApiResponse<QrState>>("tianyi_qr_status");
export const qrStop = callable<[string], ApiResponse<QrState>>("tianyi_qr_stop");
export const listCatalog = callable<
  [{ page?: number; page_size?: number; platform?: string; query?: string }],
  ApiResponse<{ items: CatalogItem[]; page: number; total: number; has_more: boolean }>
>("list_catalog");

/* ── 设置 callable ─────────────────────── */

export const getSettings = callable<[], ApiResponse<SettingsData>>("get_settings");
export const setSettings = callable<[Partial<SettingsData>], ApiResponse<SettingsData>>("set_settings");
export const listMediaMounts = callable<[], ApiResponse<{ mounts: StorageMount[] }>>("list_media_mounts");
export const deleteArchive = callable<[{ file_path: string }], ApiResponse<{ deleted: string }>>("delete_archive");
export const getCatalogVersion = callable<[], ApiResponse<CatalogVersion>>("get_catalog_version");
export const updateCatalog = callable<[], ApiResponse<CatalogVersion>>("update_catalog");

/* ── 模拟器 callable ─────────────── */

export interface EmulatorInfo {
  platform: string;
  display: string;
  adapter: string;
  icon: string;
  installed: boolean;
  has_download: boolean;
}

export interface EmulatorBootstrapResult {
  ok: boolean;
  messages: string[];
  appimage?: string;
}

export const emulatorList = callable<[], ApiResponse<{ emulators: EmulatorInfo[] }>>("emulator_list");
export const emulatorDownload = callable<[{ platform: string }], ApiResponse<{ task_id: string; platform: string }>>("emulator_download");
export const emulatorBootstrap = callable<[{ platform: string }], ApiResponse<EmulatorBootstrapResult>>("emulator_bootstrap");
export const emulatorImportSteam = callable<[{ platform: string }], ApiResponse<{ appid?: number; platform: string }>>("emulator_import_steam");

export interface EmulatorAppimageInfo {
  appimage: string;
  install_dir: string;
  platform: string;
  display: string;
}

export const emulatorLaunchAppimage = callable<
  [{ platform: string }],
  ApiResponse<EmulatorAppimageInfo>
>("emulator_launch_appimage");

export const steamFinishImport = callable<
  [{ task_id: string; appid: number }],
  ApiResponse<{ appid: number; task_id: string }>
>("steam_finish_import");

/* ── 模拟器管理中心 callable ──────────── */

export interface EmuManagerInfo {
  platform: string; name: string; icon: string; color: string;
  installed: boolean; config_ok: number; config_total: number; bios: string;
}
export const emuManagerList = callable<[], ApiResponse<EmuManagerInfo[]>>("emu_manager_list");
export const emuManagerLaunch = callable<[{ platform: string }], ApiResponse<{ launched: boolean }>>("emu_manager_launch");
export const emuManagerBootstrap = callable<[{ platform: string }], ApiResponse<{ ok: boolean; messages?: string[] }>>("emu_manager_bootstrap");

export const getEmulatorStatus = callable<[{ emulator_id: string }], ApiResponse<EmulatorStatus>>("get_emulator_status");
export const downloadEmulator = callable<[{ emulator_id: string }], ApiResponse<{}>>("download_emulator");
export const uninstallEmulator = callable<[{ emulator_id: string }], ApiResponse<{}>>("uninstall_emulator");

/* ── 运行 / 修复 callable (占位) ────────── */

export const getRuntimeRepairState = callable<[], ApiResponse<RuntimeRepairState>>("get_runtime_repair_state");
export const startRuntimeRepair = callable<[], ApiResponse<{}>>("start_runtime_repair");

/* ── 云存档 callable ─────────────── */

export const cloudSaveUpload = callable<[{ game_id?: string }?], ApiResponse<CloudSaveResult>>("cloud_save_upload");
export const cloudSaveDownload = callable<[{ game_id?: string }?], ApiResponse<CloudSaveResult>>("cloud_save_download");
export const cloudSaveHistory = callable<[], ApiResponse<{ records: CloudSaveRecord[] }>>("cloud_save_history");

/* ── 云存档 自动同步 callable ──────────── */

export type SyncLaunchResult = {
  status: "ok" | "offline" | "cloud_newer" | "conflict" | "first_play";
  files_down?: number;
  files_up?: number;
  message?: string;
  cloud_files?: { name: string; md5: string }[];
  local_files?: { name: string; md5: string }[];
};

export type SyncExitResult = {
  status: "ok" | "offline" | "pending" | "no_change";
  files_up?: number;
  count?: number;
  message?: string;
};

export const cloudSaveSyncOnLaunch = callable<
  [{ game_id: string; appid?: number }],
  ApiResponse<SyncLaunchResult>
>("cloud_save_sync_on_launch");

export const cloudSaveSyncOnExit = callable<
  [{ game_id: string; appid?: number }],
  ApiResponse<SyncExitResult>
>("cloud_save_sync_on_exit");

export const cloudSaveResolveConflict = callable<
  [{ game_id: string; choice: "use_cloud" | "keep_local" }],
  ApiResponse<{ status: string; chosen: string }>
>("cloud_save_resolve_conflict");

/* ── 插件更新 callable (占位) ───────────── */

export const checkPluginUpdate = callable<[], ApiResponse<PluginReleaseState>>("check_plugin_update");
export const startPluginUpdate = callable<[], ApiResponse<{}>>("start_plugin_update");

/* ── Rust crypto 桥接 ───────────────────── */

export const signDownload = callable<
  [{ access_token: string; file_id: string; share_id: string }],
  ApiResponse<{ signature: string; timestamp: string }>
>("sign_download");

/* ── 远程目录 (CSP 代理) ────────────────── */

export const fetchRemoteCatalog = callable<[], ApiResponse<{ items: any[]; total: number; loaded: boolean }>>("fetch_remote_catalog");

export const getShelfGames = callable<[{ limit?: number }], ApiResponse<{ games: CatalogItem[]; total: number; loaded: boolean }>>("get_shelf_games");

export const getCuratedShelf = callable<
  [{ platform: string; limit?: number }],
  ApiResponse<{ games: CatalogItem[]; total: number; platform: string }>
>("get_curated_shelf");

export const getPlatformGames = callable<
  [{ platform: string; page?: number; page_size?: number }],
  ApiResponse<{ games: CatalogItem[]; total: number; page: number; has_more: boolean }>
>("get_platform_games");

/* ── 下载 ── */

export interface DownloadTask {
  task_id: string; gid: string; game_id: string; game_title: string;
  file_name: string; file_size: number; status: string;
  progress: number; speed: number; completed: number; total: number;
  error: string;
  // 自动解压
  extract_task_id?: string;
  extract_status?: string;    // "running" | "complete" | "failed"
  extract_progress?: number;  // 0–100
  extract_file?: string;      // 当前解压文件名
  extract_error?: string;
  extract_output?: string;    // 解压输出目录
  // Steam 导入
  import_status?: string;     // "discovering" | "importing" | "applying" | "complete" | "failed"
  import_error?: string;
  steam_appid?: number;
  steam_exe?: string;
  compat_tool?: string;
}

export const downloadStart = callable<
  [{ share_url: string; file_id?: string; file_name: string; file_size?: number;
     game_id?: string; game_title?: string; access_code?: string }],
  ApiResponse<DownloadTask>
>("download_start");

export const downloadPoll = callable<
  [{ task_id: string }],
  ApiResponse<DownloadTask>
>("download_poll");

export const downloadList = callable<
  [],
  ApiResponse<{ tasks: DownloadTask[] }>
>("download_list");

export const downloadCancel = callable<
  [{ task_id: string }],
  ApiResponse<{ cancelled: boolean }>
>("download_cancel");

export const getPendingArtwork = callable<
  [{ appid: number }],
  ApiResponse<{ grid?: ArtworkAsset; hero?: ArtworkAsset; logo?: ArtworkAsset } | null>
>("get_pending_artwork");

export const importApplied = callable<
  [{ task_id: string }],
  ApiResponse<{ ok: boolean }>
>("import_applied");

export const downloadPause = callable<
  [{ task_id: string }],
  ApiResponse<DownloadTask>
>("download_pause");

export const downloadResume = callable<
  [{ task_id: string }],
  ApiResponse<DownloadTask>
>("download_resume");

/* ── 日志 ── */

export const readLog = callable<
  [{ lines?: number }],
  ApiResponse<{ log: string; total?: number }>
>("read_log");

/* ── 已安装游戏 ── */

export interface InstalledGame {
  game_id: string;
  title: string;
  steam_appid: number;
  install_dir: string;
  size_bytes: number;
  cover_url: string;
  installed_at: string;
}

export const listInstalledGames = callable<[], ApiResponse<{ games: InstalledGame[] }>>("list_installed_games");
export const uninstallGame = callable<[{ game_id: string }], ApiResponse<{ ok: boolean }>>("uninstall_game");

export const PAGE_SIZE = 30;
