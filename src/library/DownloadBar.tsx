/* ═══════════════════════════════════════════
   NewFreedeck — 下载进度条（首页置顶）
   ═══════════════════════════════════════════ */

import { Focusable } from "@decky/ui";
import type { DownloadTask } from "../api";

interface DownloadBarProps {
  task: DownloadTask;
  coverUrl: string;
  onCancel: () => void;
  onFocus: () => void;
}

export function DownloadBar({ task, coverUrl, onCancel, onFocus }: DownloadBarProps) {
  const isExtracting = task.extract_status === "running";
  const isImporting = task.import_status === "discovering" || task.import_status === "importing" || task.import_status === "applying";
  const isImportDone = task.import_status === "complete";
  const pct = Math.min(100, Math.max(0,
    isExtracting ? (task.extract_progress || 0) : (task.progress || 0)
  ));
  const speed = (isExtracting || isImporting) ? "" : formatSpeed(task.speed || 0);
  const name = task.game_title || task.file_name || "";
  const isDone = isImportDone || (task.status === "complete" && task.extract_status !== "running" && task.extract_status !== "failed");
  const isFailed = task.status === "failed" || task.extract_status === "failed" || task.import_status === "failed";
  const ringColor = isFailed ? "#c44" : isDone ? "#5a5" : isExtracting ? "#f5a623" : isImporting ? "#67c1f5" : "#fff";
  const circumference = 2 * Math.PI * 19; // r=19

  return (
    <div style={{ padding: "8px 16px 4px", overflow: "visible" }}>
      <style>{CSS}</style>
      <Focusable
        className="nf-dlbar-focus"
        onFocus={onFocus}
        onOKButton={onCancel}
        onOKActionDescription="取消下载"
        style={{
          display: "flex", alignItems: "center", gap: 14,
          padding: "10px 12px", borderRadius: 10,
          background: "rgba(54,159,244,0.12)",
          border: "1px solid rgba(54,159,244,0.3)", cursor: "pointer",
          width: "100%", boxSizing: "border-box",
        } as any}
      >
        {/* 左侧：封面 + 白色环形进度条 */}
        <div className="nf-dlbar-cover-wrap">
          {coverUrl ? (
            <img src={coverUrl} className="nf-dlbar-cover-img" alt="" />
          ) : (
            <div className="nf-dlbar-cover-fb">{name.charAt(0) || "?"}</div>
          )}
          <svg className="nf-dlbar-ring" viewBox="0 0 48 48">
            <circle cx="24" cy="24" r="22" fill="none"
              stroke="rgba(255,255,255,0.15)" strokeWidth="3" />
            <circle cx="24" cy="24" r="22" fill="none"
              stroke={ringColor} strokeWidth="3" strokeLinecap="round"
              strokeDasharray={`${pct * circumference / 100} ${circumference}`}
              transform="rotate(-90 24 24)"
              style={{ transition: "stroke-dasharray 400ms ease" }}
            />
          </svg>
        </div>

        {/* 右侧上：名称，右下：进度 + 速度 */}
        <div style={{ flex: 1, minWidth: 0 }}>
          <div className="nf-dlbar-name">{name || "下载中..."}</div>
          <div className="nf-dlbar-info">
            {isFailed ? "失败"
              : isImportDone ? "已导入 Steam"
              : isImporting ? "导入中"
              : isExtracting ? `解压 ${pct.toFixed(0)}%`
              : isDone ? "已完成"
              : `${pct.toFixed(1)}%  ${speed}`}
          </div>
        </div>
      </Focusable>
    </div>
  );
}

function formatSpeed(bps: number): string {
  if (bps < 1024) return `${bps} B/s`;
  if (bps < 1048576) return `${(bps / 1024).toFixed(1)} KB/s`;
  return `${(bps / 1048576).toFixed(1)} MB/s`;
}

const CSS = `
.nf-dlbar-cover-wrap{position:relative;width:48px;height:48px;flex-shrink:0;border-radius:6px;overflow:hidden;background:rgba(255,255,255,0.05)}
.nf-dlbar-cover-img{width:100%;height:100%;object-fit:cover}
.nf-dlbar-cover-fb{width:100%;height:100%;display:flex;align-items:center;justify-content:center;font-size:20px;color:rgba(255,255,255,0.25)}
.nf-dlbar-ring{position:absolute;inset:-4px;width:56px;height:56px;pointer-events:none}
.nf-dlbar-name{font-size:14px;font-weight:600;color:#f4f8fc;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.nf-dlbar-info{font-size:12px;color:#8b929a;margin-top:2px}
`;
