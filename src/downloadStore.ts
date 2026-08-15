/* ═══════════════════════════════════════════
   NewFreedeck — 全局下载队列（模块级单例）
   ═══════════════════════════════════════════ */

import type { DownloadTask } from "./api";

type Listener = (queue: DownloadTask[]) => void;
let _queue: DownloadTask[] = [];
let _listeners: Listener[] = [];

export function getDownloadQueue(): DownloadTask[] {
  return _queue;
}

export function setDownloadQueue(queue: DownloadTask[]) {
  _queue = queue;
  for (const fn of _listeners) fn(_queue);
}

/** 添加/更新一个下载任务 */
export function upsertDownload(task: DownloadTask, coverUrl?: string) {
  const enriched = coverUrl ? { ...task, cover_url: coverUrl } as any : task;
  const idx = _queue.findIndex(t => t.task_id === task.task_id);
  if (idx >= 0) {
    _queue = [..._queue];
    _queue[idx] = enriched;
  } else {
    _queue = [enriched, ..._queue];
  }
  // 最多保留 5 条历史
  if (_queue.length > 5) _queue = _queue.slice(0, 5);
  for (const fn of _listeners) fn(_queue);
}

/** 移除一个下载任务 */
export function removeDownload(taskId: string) {
  _queue = _queue.filter(t => t.task_id !== taskId);
  for (const fn of _listeners) fn(_queue);
}

/** 获取当前活动下载（队列中第一个非完成/非失败的） */
export function getActiveDownload(): DownloadTask | null {
  return _queue[0] || null;
}

/** 兼容旧 API */
export function setActiveDownload(task: DownloadTask | null) {
  if (task) {
    upsertDownload(task);
  }
}

export function onDownloadChange(fn: Listener) {
  _listeners.push(fn);
  return () => { _listeners = _listeners.filter(l => l !== fn); };
}
