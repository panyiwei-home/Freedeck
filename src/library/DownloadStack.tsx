/* ═══════════════════════════════════════════
   NewFreedeck — 下载堆叠组件（Stack 动画 + 直操作）
   ═══════════════════════════════════════════ */

import { Focusable, showModal, ConfirmModal } from "@decky/ui";
import { useState, useRef, useEffect } from "react";
import type { DownloadTask } from "../api";
import { getDownloadPoller } from "../downloadPoller";
import { removeDownload, upsertDownload } from "../downloadStore";

interface DownloadStackProps {
  tasks: DownloadTask[];
}

const CARD_H = 104;
const RECT_W = 60, RECT_H = 90, STROKE_W = 2.5, GAP = 4;
const RING_W = RECT_W + GAP * 2, RING_H = RECT_H + GAP * 2;
const PERIMETER = 2 * (RING_W + RING_H);
const SVG_W = RING_W + STROKE_W * 2, SVG_H = RING_H + STROKE_W * 2;
const TOGGLE_H = 20;

export function DownloadStack({ tasks }: DownloadStackProps) {
  const [expanded, setExpanded] = useState(false);
  const [toggleFocused, setToggleFocused] = useState(false);
  const stageRef = useRef<HTMLDivElement>(null);

  const visible = expanded ? tasks.slice(0, 5) : tasks.slice(0, 3);
  const count = tasks.length;
  const toggleExpand = () => setExpanded(v => !v);

  // 展开后自动聚焦第一个下载卡片（等动画完成）
  useEffect(() => {
    if (!expanded || !stageRef.current) return;
    const timer = setTimeout(() => {
      const firstCard = stageRef.current?.querySelector(".nf-dlcard") as HTMLElement | null;
      if (firstCard) {
        firstCard.focus();
      }
    }, 350); // 匹配 CSS transition 时长
    return () => clearTimeout(timer);
  }, [expanded]);

  if (tasks.length === 0) return null;

  return (
    <div style={{ padding: "0 16px 16px", overflow: "visible" }}>
      <style>{CSS}</style>

      <div ref={stageRef} className="nf-dlstack-stage" style={{
        height: expanded
          ? visible.length * CARD_H + (visible.length - 1) * 16 + TOGGLE_H + 16
          : CARD_H + Math.min(count - 1, 2) * 8 + TOGGLE_H,
        transition: "height 350ms cubic-bezier(0.34, 1.56, 0.64, 1)",
      }}>
        {visible.map((task, i) => {
          const top = expanded ? i * (CARD_H + 16) : 0;
          const translateY = expanded ? 0 : i * 8;
          const scale = expanded ? 1 : 1 - i * 0.06;
          const collapsed = !expanded && i > 0;

          return (
            <div
              key={task.task_id}
              className="nf-dlcard-stage-item"
              style={{
                position: "absolute", left: 0, right: 0,
                top: `${top}px`,
                transform: `translateY(${translateY}px) scale(${scale})`,
                transformOrigin: "center top",
                zIndex: expanded ? (visible.length - i) : (3 - i),
                transition: "top 350ms cubic-bezier(0.34, 1.56, 0.64, 1), transform 350ms cubic-bezier(0.34, 1.56, 0.64, 1)",
                opacity: collapsed ? 0 : 1,
                pointerEvents: collapsed ? "none" : "auto",
              }}
            >
              {collapsed ? (
                <CardContent task={task} />
              ) : (
                <DownloadCard task={task} />
              )}
            </div>
          );
        })}

        {count > 1 && (
          <Focusable
            className="nf-dlstack-toggle"
            onFocus={() => setToggleFocused(true)}
            onBlur={() => setToggleFocused(false)}
            onOKButton={toggleExpand}
            onOKActionDescription={expanded ? "收起" : "展开"}
            onClick={toggleExpand}
            style={{
              position: "absolute",
              left: 0, right: 0,
              bottom: 4,
              display: "flex", alignItems: "center", justifyContent: "center",
              padding: "4px 0 0", cursor: "pointer", border: "none",
              background: "transparent", width: "100%",
              height: TOGGLE_H,
              outline: toggleFocused ? "2px solid rgba(255,255,255,0.35)" : "2px solid transparent",
              outlineOffset: "-2px",
            } as any}
          >
            <span className="nf-dlstack-toggle-label">
              {expanded ? "收起" : `${count} 个任务 · 展开`}
            </span>
            <span className={`nf-dlstack-toggle-arrow${expanded ? " is-up" : ""}`} />
          </Focusable>
        )}
      </div>
    </div>
  );
}

/* ── 单张下载卡片（含 Focusable）─────────── */

function DownloadCard({ task }: { task: DownloadTask }) {
  const isExtracting = task.extract_status === "running";
  const isImporting = task.import_status === "discovering" || task.import_status === "importing" || task.import_status === "applying";
  const isImportDone = task.import_status === "complete";
  const isPaused = task.status === "paused";
  const isFailed = task.status === "failed" || task.extract_status === "failed" || task.import_status === "failed";
  const isDone = isImportDone || (task.status === "complete" && task.extract_status !== "running" && !isFailed);
  const isRunning = !isPaused && !isFailed && !isDone;

  const pct = Math.min(100, Math.max(0,
    isExtracting ? (task.extract_progress || 0) : (task.progress || 0)
  ));
  const speed = isRunning && !isExtracting ? formatSpeed(task.speed || 0) : "";
  const name = task.game_title || task.file_name || "";
  const cover = (task as any).cover_url || "";

  const borderColor = isFailed ? "#c44" : isDone ? "#5a5" : isPaused ? "#f5a623" : isExtracting ? "#f5a623" : isImporting ? "#67c1f5" : "#369ff4";
  const dashOffset = PERIMETER * (1 - pct / 100);

  const [focused, setFocused] = useState(false);

  const handleA = () => {
    const poller = getDownloadPoller();
    if (isFailed) {
      poller.addTask(task.task_id);
      upsertDownload({ ...task, status: "downloading", progress: 0, error: "" } as any);
    } else if (isPaused) {
      poller.resumeTask(task.task_id);
    } else if (isRunning) {
      poller.pauseTask(task.task_id);
    }
  };

  const handleX = () => {
    if (isDone || isFailed) {
      removeDownload(task.task_id);
    } else {
      const poller = getDownloadPoller();
      // 弹窗确认是否同时删除已下载文件
      const close = showModal(
        <ConfirmModal
          strTitle="取消下载"
          strDescription={`确定取消 "${name}" 的下载吗？\n已下载的部分文件将被删除。`}
          strOKButtonText="取消下载"
          strCancelButtonText="继续下载"
          onOK={() => {
            poller.cancelTask(task.task_id);
            close.Close();
          }}
          onCancel={() => close.Close()}
        />,
      );
    }
  };

  const aLabel = isFailed ? "重试" : isPaused ? "继续" : isRunning ? "暂停" : isDone ? "" : "";
  const xLabel = isDone || isFailed ? "清除" : (isRunning || isPaused) ? "取消任务" : "";

  return (
    <Focusable
      className="nf-dlcard"
      onFocus={() => setFocused(true)}
      onBlur={() => setFocused(false)}
      style={{
        display: "flex", alignItems: "flex-start",
        padding: 12,
        width: "100%", boxSizing: "border-box",
        outline: focused ? "2px solid rgba(255,255,255,0.35)" : "2px solid transparent",
        outlineOffset: "-2px",
      } as any}
      onOKButton={aLabel ? handleA : undefined}
      onOKActionDescription={aLabel || undefined}
      onSecondaryButton={xLabel ? handleX : undefined}
      onSecondaryActionDescription={xLabel || undefined}
    >
      {/* 封面 + 环形进度 */}
      <div style={{ flexShrink: 0, position: "relative", width: SVG_W, height: SVG_H }}>
        <div style={{
          position: "absolute", left: GAP + STROKE_W, top: GAP + STROKE_W,
          width: RECT_W, height: RECT_H, overflow: "hidden",
          background: "rgba(255,255,255,0.04)", borderRadius: 4,
        }}>
          {cover ? (
            <img src={cover} style={{ width: "100%", height: "100%", objectFit: "cover", display: "block" }} alt="" />
          ) : (
            <div style={{
              width: "100%", height: "100%", display: "flex", alignItems: "center", justifyContent: "center",
              fontSize: 20, color: "rgba(255,255,255,0.12)",
            }}>{name.charAt(0) || "?"}</div>
          )}
        </div>
        <svg style={{ position: "absolute", inset: 0, width: "100%", height: "100%", pointerEvents: "none" }}
          viewBox={`0 0 ${SVG_W} ${SVG_H}`} preserveAspectRatio="none">
          <rect x={STROKE_W / 2} y={STROKE_W / 2} width={RING_W + STROKE_W} height={RING_H + STROKE_W}
            fill="none" stroke="rgba(255,255,255,0.08)" strokeWidth={STROKE_W} />
          {isRunning && (
            <rect x={STROKE_W / 2} y={STROKE_W / 2} width={RING_W + STROKE_W} height={RING_H + STROKE_W}
              fill="none" stroke={borderColor} strokeWidth={STROKE_W}
              strokeDasharray={PERIMETER} strokeDashoffset={dashOffset}
              style={{ transition: "stroke-dashoffset 400ms ease" }} />
          )}
          {(isPaused || isDone || isFailed) && (
            <rect x={STROKE_W / 2} y={STROKE_W / 2} width={RING_W + STROKE_W} height={RING_H + STROKE_W}
              fill="none" stroke={borderColor} strokeWidth={STROKE_W} />
          )}
        </svg>
        {isPaused && (
          <div style={{
            position: "absolute", inset: 0, display: "flex", alignItems: "center", justifyContent: "center",
            background: "rgba(0,0,0,0.4)", borderRadius: 4,
          }}>
            <span style={{ fontSize: 14, fontWeight: 700, color: "#f5a623" }}>⏸</span>
          </div>
        )}
      </div>

      {/* 标题 + 状态 */}
      <div style={{ flex: 1, minWidth: 0, paddingLeft: 12, paddingTop: 8 }}>
        <div style={{
          fontSize: 14, fontWeight: 600, color: "#f4f8fc",
          whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis",
        }}>{name || "下载中..."}</div>
        <div style={{ fontSize: 12, color: "#8b929a", marginTop: 4 }}>
          {isFailed ? (task.error || "失败")
            : isImportDone ? "已导入 Steam"
            : isPaused ? "已暂停"
            : isImporting ? "导入中"
            : isDone ? "已完成"
            : isExtracting ? `解压 ${pct.toFixed(0)}%`
            : `${pct.toFixed(1)}%  ${speed}`}
        </div>
      </div>
    </Focusable>
  );
}

/* ── 非交互卡片内容（折叠态的幽灵卡片） ── */

function CardContent({ task }: { task: DownloadTask }) {
  const name = task.game_title || task.file_name || "";
  const cover = (task as any).cover_url || "";
  return (
    <div style={{
      display: "flex", alignItems: "flex-start",
      padding: 12, borderRadius: 6,
      width: "100%", boxSizing: "border-box",
    }}>
      <div style={{ flexShrink: 0, position: "relative", width: SVG_W, height: SVG_H }}>
        <div style={{
          position: "absolute", left: GAP + STROKE_W, top: GAP + STROKE_W,
          width: RECT_W, height: RECT_H, overflow: "hidden",
          background: "rgba(255,255,255,0.04)", borderRadius: 4,
        }}>
          {cover ? (
            <img src={cover} style={{ width: "100%", height: "100%", objectFit: "cover", display: "block" }} alt="" />
          ) : (
            <div style={{
              width: "100%", height: "100%", display: "flex", alignItems: "center", justifyContent: "center",
              fontSize: 20, color: "rgba(255,255,255,0.12)",
            }}>{name.charAt(0) || "?"}</div>
          )}
        </div>
      </div>
      <div style={{ flex: 1, minWidth: 0, paddingLeft: 12, paddingTop: 8 }}>
        <div style={{
          fontSize: 14, fontWeight: 600, color: "#f4f8fc",
          whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis",
        }}>{name || "下载中..."}</div>
      </div>
    </div>
  );
}

function formatSpeed(bps: number): string {
  if (bps < 1024) return `${bps} B/s`;
  if (bps < 1048576) return `${(bps / 1024).toFixed(1)} KB/s`;
  return `${(bps / 1048576).toFixed(1)} MB/s`;
}

const CSS = `
.nf-dlstack-stage{position:relative;width:100%}

.nf-dlstack-toggle{position:relative}
.nf-dlstack-toggle-label{font-size:12px;color:rgba(255,255,255,0.5);margin-right:4px}
.nf-dlstack-toggle-arrow{display:inline-block;border-top:1.5px solid rgba(255,255,255,0.4);border-right:1.5px solid rgba(255,255,255,0.4);width:6px;height:6px;transform:rotate(135deg);transition:transform 300ms ease}
.nf-dlstack-toggle-arrow.is-up{transform:rotate(-45deg)}

.nf-dlcard{position:relative;background:transparent}
`;