/* ═══════════════════════════════════════════
   NewFreedeck — 全局下载轮询器（含 Steam 导入）
   ═══════════════════════════════════════════ */

import { downloadPoll, downloadCancel, downloadStart, downloadList, downloadPause, downloadResume, getPendingArtwork, importApplied, getSettings, deleteArchive, steamFinishImport } from "./api";
import { upsertDownload, removeDownload, getDownloadQueue } from "./downloadStore";
import { applyRomLaunchOptions } from "./romImport";

let _poller: DownloadPoller | null = null;
export function getDownloadPoller(): DownloadPoller {
  if (!_poller) _poller = new DownloadPoller();
  return _poller;
}

class DownloadPoller {
  private _intervals = new Map<string, ReturnType<typeof setInterval>>();
  private _importing = new Map<string, boolean>(); // 防重复导入
  private _doneNotified = new Set<string>();       // 已完成已弹 toast

  /** 开始轮询一个下载任务 */
  addTask(taskId: string, coverUrl?: string) {
    if (this._intervals.has(taskId)) return;
    const id = setInterval(async () => {
      try {
        const poll = await downloadPoll({ task_id: taskId });
        if (poll?.status === "success" && poll.data) {
          const data = poll.data as any;
          upsertDownload(data, coverUrl);

          // 前端创建快捷方式（代替 steamos-add-to-steam）
          if (data.import_status === "need_shortcut" && data.steam_exe && !this._importing.get(taskId)) {
            this._importing.set(taskId, true);
            this._createShortcutAndFinish(data);
          }

          // 触发 Steam 导入
          if (data.import_status === "applying" && data.steam_appid && !this._importing.get(taskId)) {
            this._importing.set(taskId, true);
            this._runSteamImport(data);
          }

          // 完成后处理
          if (this._isAllDone(data)) {
            this._stopPolling(taskId);
            this._handleTaskDone(taskId, data);
          }
        }
      } catch { /* ignore */ }
    }, 1000);
    this._intervals.set(taskId, id);
  }

  async cancelTask(taskId: string) {
    this._stopPolling(taskId);
    try { await downloadCancel({ task_id: taskId }); } catch {}
    removeDownload(taskId);
    this._doneNotified.delete(taskId);
    this._importing.delete(taskId);
  }

  /** 暂停下载 — 调后端 API + 停前端轮询 */
  async pauseTask(taskId: string) {
    this._stopPolling(taskId);
    try {
      const r = await downloadPause({ task_id: taskId });
      if (r?.data) upsertDownload(r.data as any);
    } catch {}
  }

  /** 继续下载 — 调后端 API + 恢复前端轮询 */
  async resumeTask(taskId: string) {
    try {
      const r = await downloadResume({ task_id: taskId });
      if (r?.data) upsertDownload(r.data as any);
    } catch {}
    this.addTask(taskId);
  }

  /** 页面挂载时恢复轮询 + 从后端同步 */
  async resumeAll() {
    try {
      const list = await downloadList();
      if (list?.status === "success" && list.data?.tasks?.length) {
        for (const t of list.data.tasks) {
          if (this._isAllDone(t)) continue;
          const existing = getDownloadQueue().find(q => q.task_id === t.task_id);
          if (!existing) upsertDownload(t as any);
        }
      }
    } catch {}

    const queue = getDownloadQueue();
    for (const task of queue) {
      if (!this._isAllDone(task) && !this._intervals.has(task.task_id)) {
        this.addTask(task.task_id, (task as any).cover_url);
      }
    }
  }

  destroy() {
    for (const id of this._intervals.keys()) clearInterval(this._intervals.get(id));
    this._intervals.clear();
    this._importing.clear();
    this._doneNotified.clear();
  }

  /* ═══ 内部 ═══ */

  private _stopPolling(taskId: string) {
    const id = this._intervals.get(taskId);
    if (id) { clearInterval(id); this._intervals.delete(taskId); }
  }

  private _isAllDone(t: any): boolean {
    const dlDone = t.status === "complete" || t.status === "failed";
    const extDone = !t.extract_task_id || t.extract_status === "complete" || t.extract_status === "failed";
    const hasImport = !!t.game_id;
    const importDone = hasImport
      ? (t.import_status === "complete" || t.import_status === "failed")
      : true;
    return dlDone && extDone && importDone;
  }

  private async _handleTaskDone(taskId: string, data: any) {
    if (this._doneNotified.has(taskId)) return; // 已通知过
    this._doneNotified.add(taskId);

    // 检查是否开启安装后自动删除压缩包
    try {
      const s = await getSettings();
      if (s?.data?.auto_delete_after_install && data?.file_path) {
        await deleteArchive({ file_path: data.file_path });
      }
    } catch {}

    // 1 秒后从队列移除
    setTimeout(() => {
      removeDownload(taskId);
      this._doneNotified.delete(taskId);
      this._importing.delete(taskId);
    }, 1000);
  }

  /* ─── Steam 导入（在 poller 中运行，不依赖任何页面） ─── */

  /** 前端创建 Steam 快捷方式，拿到 appid 后回传后端继续配置 */
  private async _createShortcutAndFinish(task: any) {
    const taskId = task.task_id;
    const exe = String(task.steam_exe || "");
    const gameTitle = String(task.game_title || "");
    const startDir = String(task.start_dir || "");
    const launchOpts = String(task.launch_options || "");

    try {
      const SC = (window as any).SteamClient;
      if (!SC?.Apps) {
        await importApplied({ task_id: taskId });
        return;
      }

      // 创建快捷方式（照搬"打开"按钮的做法）
      const steamAppId = await SC.Apps.AddShortcut(gameTitle, exe, startDir, launchOpts);
      console.log(`[NewFreedeck] _createShortcutAndFinish: AddShortcut appId=${steamAppId} exe=${exe}`);

      // 等待 Steam 识别
      const appStore = (window as any).appStore;
      let gameInfo: any = null;
      for (let i = 0; i < 30; i++) {
        await new Promise(r => setTimeout(r, 100));
        gameInfo = appStore?.GetAppOverviewByAppID(steamAppId);
        if (gameInfo != null) break;
      }

      if (!gameInfo) {
        console.log(`[NewFreedeck] _createShortcutAndFinish: app not found, cleaning up`);
        try { await SC.Apps.RemoveShortcut(steamAppId); } catch {}
        await importApplied({ task_id: taskId });
        return;
      }

      // 设置名称（确保中文标题生效）
      if (gameTitle && SC.Apps.SetShortcutName) {
        try { await SC.Apps.SetShortcutName(steamAppId, gameTitle); } catch {}
      }

      // ROM 游戏：设置启动选项
      if (launchOpts && SC.Apps.SetShortcutLaunchOptions) {
        try { await SC.Apps.SetShortcutLaunchOptions(steamAppId, launchOpts); } catch {}
      }

      // 回传 appid 给后端继续配置（兼容层 + VDF 修改 + 封面）
      const r = await steamFinishImport({ task_id: taskId, appid: steamAppId });
      console.log(`[NewFreedeck] _createShortcutAndFinish: steamFinishImport status=${r?.status}`);

      // 后端已将 import_status 设为 "applying"，立即触发后续导入流程
      if (r?.status === "success") {
        const poll = await downloadPoll({ task_id: taskId });
        if (poll?.status === "success" && poll.data) {
          const data = poll.data as any;
          upsertDownload(data);
          if (data.import_status === "applying" && data.steam_appid) {
            await this._runSteamImport(data);
          }
        }
      }
    } catch (e: any) {
      console.log(`[NewFreedeck] _createShortcutAndFinish error: ${e?.message}`);
      await importApplied({ task_id: taskId });
    }
  }

  private async _runSteamImport(task: any) {
    const taskId = task.task_id;
    const appid = task.steam_appid;
    const gameTitle = task.game_title || "";
    const compatTool = task.compat_tool || "";

    try {
      const SC = (window as any).SteamClient;
      if (!SC?.Apps) {
        await importApplied({ task_id: taskId });
        return;
      }

      // 兼容层
      if (compatTool && SC.Apps.SpecifyCompatTool) {
        try { SC.Apps.SpecifyCompatTool(appid, compatTool); } catch {}
      }

      // 注册 app details
      try {
        await new Promise<void>((resolve) => {
          const t = setTimeout(resolve, 3000);
          try {
            const { unregister } = SC.Apps.RegisterForAppDetails(appid, () => {
              clearTimeout(t); unregister(); resolve();
            });
          } catch { clearTimeout(t); resolve(); }
        });
      } catch {}

      // 设置名称
      if (gameTitle && SC.Apps.SetShortcutName) {
        try { await SC.Apps.SetShortcutName(appid, gameTitle); } catch {}
      }

      // ROM 导入：实时设置启动选项
      const launchOpts = task.launch_options || "";
      if (launchOpts) {
        try {
          await applyRomLaunchOptions(appid, gameTitle, launchOpts);
        } catch {}
      }

      // 等封面（最多 30s）
      const artwork = await this._waitForArtwork(appid);
      if (!artwork) {
        await importApplied({ task_id: taskId });
        return;
      }

      // 上封面
      if (SC.Apps.SetCustomArtworkForApp) {
        for (const [key, assetType] of [["grid", 0], ["hero", 3], ["logo", 4]] as [string, number][]) {
          const asset = (artwork as any)[key];
          if (!asset?.data) continue;
          try {
            const prepared = await this._ensurePng(asset);
            await SC.Apps.ClearCustomArtworkForApp(appid, assetType);
            await new Promise(r => setTimeout(r, 500));
            await SC.Apps.SetCustomArtworkForApp(appid, prepared.data, prepared.format, assetType);
          } catch {}
        }

        // logo 位置
        if ((artwork as any)["logo"]?.data) {
          try {
            const store = (window as any).appDetailsStore;
            const pos = await store?.GetCustomLogoPosition?.(appid);
            if (!pos) {
              await store?.SaveCustomLogoPosition?.(
                { appid, BIsShortcut: () => true },
                { pinnedPosition: "BottomLeft", nWidthPct: 50, nHeightPct: 50 }
              );
            }
          } catch {}
        }
      }

      await importApplied({ task_id: taskId });
    } catch {}
  }

  private async _waitForArtwork(appid: number): Promise<any> {
    for (let i = 0; i < 30; i++) {
      try {
        const r = await getPendingArtwork({ appid });
        if (r?.data && Object.keys(r.data).length > 0) return r.data;
      } catch {}
      await new Promise(r => setTimeout(r, 1000));
    }
    return null;
  }

  private async _ensurePng(asset: { data: string; format: string }) {
    if (asset.format === "png") return asset;
    try {
      const mime = asset.format === "jpg" ? "jpeg" : asset.format;
      const img = await new Promise<HTMLImageElement>((resolve, reject) => {
        const el = new Image();
        el.onload = () => resolve(el);
        el.onerror = reject;
        el.src = `data:image/${mime};base64,${asset.data}`;
      });
      const canvas = document.createElement("canvas");
      canvas.width = img.naturalWidth || img.width;
      canvas.height = img.naturalHeight || img.height;
      const ctx = canvas.getContext("2d");
      if (!ctx) return asset;
      ctx.drawImage(img, 0, 0);
      const data = canvas.toDataURL("image/png").split(",", 2)[1];
      return data ? { data, format: "png" } : asset;
    } catch { return asset; }
  }

}
