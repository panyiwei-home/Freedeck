/* ═══════════════════════════════════════════
   云存档 — 同步任务队列 + toast 通知

   参考 SDH-GameSync 架构：
     - Observable (EventTarget) 事件总线
     - fastq.promise 串行任务队列
     - toaster.toast() 在 Promise 回调中调用, 不依赖 React 挂载

   关键：
     - toast 不经过 React 组件树, 由 Decky API bridge 直发
     - SteamClient 事件注册在 definePlugin() 顶层, 插件 UI 关闭后仍触发
   ═══════════════════════════════════════════ */

import { toaster } from "@decky/api";
import fastq from "fastq";
import type { queueAsPromised } from "fastq";

/* ── Observable 事件总线 ────────────────── */

class Observable {
  private eventTarget = new EventTarget();

  on(eventType: string, handler: (data: any) => void) {
    const listener = (event: Event) => handler((event as CustomEvent).detail);
    this.eventTarget.addEventListener(eventType, listener);
    return { unregister: () => this.eventTarget.removeEventListener(eventType, listener) };
  }

  emit(eventType: string, data?: any) {
    this.eventTarget.dispatchEvent(new CustomEvent(eventType, { detail: data }));
  }
}

/* ── Toast 通知（独立于 React）─────────── */

const PLUGIN_NAME = "NewFreedeck";

const toastIcon = window.SP_REACT.createElement("img", {
  width: "30",
  style: { marginTop: "5px", marginLeft: "10px" },
  src: "",
});

export function toast(msg: string, ms = 3000, clickAction?: () => void) {
  try {
    toaster.toast({
      title: PLUGIN_NAME,
      body: msg,
      duration: ms,
      onClick: clickAction || (() => {}),
    });
  } catch {
    // toaster 可能未就绪（初始化早期）
  }
}

/* ── retry 容错包装 ────────────────────── */

export function retry<T>(handler: () => T, delayMs = 5000): T | undefined {
  try {
    return handler();
  } catch (err) {
    console.warn("[NewFreedeck] retry in", delayMs, "ms:", err);
    setTimeout(() => {
      try {
        handler();
      } catch (err2) {
        console.error("[NewFreedeck] retry failed:", err2);
      }
    }, delayMs);
    return undefined;
  }
}

/* ── SyncTaskQueue ─────────────────────── */

type SyncTask = {
  fn: (appId: number) => Promise<number>;  // 返回 exitCode (0=成功)
  appId: number;
  label: string;
};

async function worker(task: SyncTask): Promise<number> {
  console.log("[NewFreedeck] sync task start:", task.label, "appId=", task.appId);
  try {
    const exitCode = await task.fn(task.appId);
    console.log("[NewFreedeck] sync task done:", task.label, "exit=", exitCode);
    return exitCode;
  } catch (err) {
    console.error("[NewFreedeck] sync task error:", task.label, err);
    return -1;
  }
}

class SyncTaskQueue extends Observable {
  static events = {
    BUSY: "busy",
    FAIL_TOAST_CLICK: "failToastClick",
  };

  private queue: queueAsPromised<SyncTask>;
  private _autoSyncEnabled = false;

  constructor() {
    super();
    this.queue = fastq.promise(worker, 1);
    (this.queue as any).drain = () => {
      this.emit(SyncTaskQueue.events.BUSY, false);
    };
  }

  get busy(): boolean {
    return !this.queue.idle();
  }

  get autoSyncEnabled(): boolean {
    return this._autoSyncEnabled;
  }

  setAutoSyncEnabled(enabled: boolean) {
    this._autoSyncEnabled = enabled;
  }

  /**
   * 添加云存档同步任务。
   * @param fn     后端 callable（返回 ApiResponse<{status,...}>)
   * @param appId   Steam AppID
   * @param label  任务标签（用于日志）
   * @param successToast  成功时的 toast 消息（空字符串则不弹）
   * @param failToast     失败时的 toast 消息
   */
  async addTask(
    fn: (appId: number) => Promise<any>,
    appId: number,
    label: string,
    successToast = "",
    failToast = "",
  ) {
    if (!this._autoSyncEnabled && label.includes("auto")) {
      return;
    }

    this.queue
      .push({
        fn: async (id) => {
          try {
            const r = await fn(id);
            if (r?.status === "success") {
              return 0;
            }
            return 1;
          } catch {
            return -1;
          }
        },
        appId,
        label,
      })
      .then((exitCode: number) => {
        if (exitCode === 0) {
          if (successToast) toast(successToast, 3000);
        } else {
          if (failToast) {
            toast(failToast, 5000, () => {
              this.emit(SyncTaskQueue.events.FAIL_TOAST_CLICK, { appId, label });
            });
          }
        }
      })
      .catch(() => {
        if (failToast) toast(failToast, 5000);
      });
  }
}

/* ── 全局单例 ──────────────────────────── */

export const syncTaskQueue = new SyncTaskQueue();
