/* ═══════════════════════════════════════════
   NewFreedeck — 安装浮层 (Motion FLIP 动画)
   Flutter StoreGameGroup.selectableVariants 移植
   ═══════════════════════════════════════════ */

import { SteamSpinner, DialogButton, DialogButtonPrimary, Focusable, gamepadLibraryClasses, DropdownItem, Menu, MenuItem, showContextMenu } from "@decky/ui";
import { toaster } from "@decky/api";
import { useState, useEffect, useRef, useCallback, useMemo } from "react";
import type { CatalogItem, DownloadTask } from "../api";
import { downloadStart, downloadPoll, downloadCancel } from "../api";

// ── selectableVariants 辅助函数 ──────────────────

function hasDiscMarker(text: string): boolean {
  return /(disc|disk|cd|dvd|umd|[ABCD一二三四]盘)/i.test(text);
}

function stripDiscMarkers(raw: string): string {
  return raw
    .replace(/(disc|disk|cd|dvd|umd)[\s._-]*[0-9a-d]+/gi, "")
    .replace(/[abcd一二三四]盘/gi, "")
    .replace(/\s+/g, " ")
    .trim();
}

function normalizeFamilyToken(raw: string): string {
  return raw.trim().toLowerCase().replace(/\s+/g, " ").replace(/[/｜|]+/g, " ").trim();
}

function discOrder(v: CatalogItem): number {
  const sources = [
    v.title, v.title_zh, v.title_en, v.version || "",
    v.file_name || "", v.down_url || "",
  ];
  for (const raw of sources) {
    const t = raw.trim();
    if (!t) continue;
    let m = /(?:disc|disk|cd|dvd|umd)[\s._-]*(\d+)/i.exec(t);
    if (m) return parseInt(m[1], 10);
    m = /([ABCDabcd])盘/.exec(t);
    if (m) return m[1].toUpperCase().charCodeAt(0) - 64;
    const cn: Record<string, number> = { "一": 1, "二": 2, "三": 3, "四": 4 };
    m = /([一二三四])盘/.exec(t);
    if (m) return cn[m[1]] || 999;
  }
  return 999;
}

/** 为 PS1 构建 installFamilyKey（标题去盘符 + 版本标签归一化） */
function buildFamilyKey(v: CatalogItem, groupTitleZh: string, groupTitleEn: string): string {
  const versionLabel = v.version || "";
  const combined = [versionLabel, v.title_zh, v.title_en]
    .find(x => x.trim()) || "";
  return [
    normalizeFamilyToken(stripDiscMarkers(groupTitleZh)),
    normalizeFamilyToken(stripDiscMarkers(groupTitleEn)),
    normalizeFamilyToken(stripDiscMarkers(v.title_zh)),
    normalizeFamilyToken(stripDiscMarkers(v.title_en)),
    normalizeFamilyToken(stripDiscMarkers(combined)),
  ].join("|");
}

/** 检查一组变体是否"似多光盘" */
function isDiscFamily(family: CatalogItem[]): boolean {
  if (family.length <= 1) return false;
  if (family.some(v => hasDiscMarker(
    [v.title, v.title_zh, v.title_en, v.version || "", v.file_name || ""].join(" ")
  ))) return true;
  // 归一化版本标签唯一 → 视为光盘组
  const labels = new Set(family.map(v =>
    normalizeFamilyToken(v.version || v.title_zh || v.title_en || "")
  ).filter(Boolean));
  return labels.size === 1;
}

// ── InstallOverlay ──────────────────────────────

interface InstallOverlayProps {
  game: CatalogItem;
  sourceRect: DOMRect;
  onClose: () => void;
  onStartDownload: (task: DownloadTask, coverUrl: string) => void;
}

export function InstallOverlay({ game, sourceRect, onClose, onStartDownload }: InstallOverlayProps) {
  const [phase, setPhase] = useState<"enter" | "active" | "exit">("enter");
  const [downloading, setDownloading] = useState(false);
  const [downloadTask, setDownloadTask] = useState<DownloadTask | null>(null);
  const [downloadError, setDownloadError] = useState("");
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const overlayRef = useRef<HTMLDivElement>(null);
  const installBtnRef = useRef<any>(null);
  const cancelBtnRef = useRef<any>(null);
  const moreBtnRef = useRef<any>(null);
  const dropdownRef = useRef<any>(null);
  const closingRef = useRef(false);

  const allVariants: CatalogItem[] = (game.grouped && game.variants) ? game.variants : [game];
  const platform = (game.platform || "").toLowerCase();
  const installsAsDiscSet = !!(game.multi_disc && game.disc_count && game.disc_count > 1);

  // ── selectableVariants (Flutter 移植) ──
  const selectableVariants: CatalogItem[] = useMemo(() => {
    if (allVariants.length === 0) return [];
    if (installsAsDiscSet) return [allVariants[0]];

    const needsPs1Inference = platform === "ps1" && allVariants.length > 1;
    if (!needsPs1Inference) return allVariants;

    // PS1: 按 familyKey 分组 → 光盘家族只展示一个
    const families = new Map<string, CatalogItem[]>();
    const order: string[] = [];
    for (const v of allVariants) {
      const key = buildFamilyKey(v, game.title_zh, game.title_en);
      if (!families.has(key)) { families.set(key, []); order.push(key); }
      families.get(key)!.push(v);
    }
    const selectable: CatalogItem[] = [];
    for (const key of order) {
      const family = families.get(key)!;
      family.sort((a, b) => discOrder(a) - discOrder(b));
      if (isDiscFamily(family)) {
        selectable.push(family[0]);
      } else {
        selectable.push(...family);
      }
    }
    return selectable.length > 0 ? selectable : allVariants;
  }, [allVariants, installsAsDiscSet, platform, game.title_zh, game.title_en]);

  // ── installVariantsForSelection ──
  function installVariantsForSelection(selected: CatalogItem): CatalogItem[] {
    if (installsAsDiscSet) {
      return [...allVariants].sort((a, b) => discOrder(a) - discOrder(b));
    }
    const needsPs1Inference = platform === "ps1" && allVariants.length > 1;
    if (!needsPs1Inference) return [selected];

    const familyKey = buildFamilyKey(selected, game.title_zh, game.title_en);
    const family = allVariants.filter(v =>
      buildFamilyKey(v, game.title_zh, game.title_en) === familyKey
    );
    return isDiscFamily(family)
      ? family.sort((a, b) => discOrder(a) - discOrder(b))
      : [selected];
  }

  const [selectedIdx, setSelectedIdx] = useState(0);
  const selectedVariant = selectableVariants[selectedIdx] || allVariants[0];
  const focusClass = useMemo(() => {
    const c = gamepadLibraryClasses as Record<string, string> | undefined;
    return c?.["ItemFocusAnim-darkGrey"] || c?.["ItemFocusAnim-darkerGrey"] || "";
  }, []);

  // ── 封面候选链：变体间回退 ──
  const coverUrl: string = useMemo(() => {
    const seen = new Set<string>();
    for (const v of allVariants) {
      const u = (v.cover_url || "").trim();
      if (u && !seen.has(u)) { seen.add(u); return u; }
    }
    return game.cover_url || "";
  }, [allVariants, game.cover_url]);

  const installDiscCount = installVariantsForSelection(selectedVariant).length;

  const activeGame = installsAsDiscSet ? game :
    (installVariantsForSelection(selectedVariant)[0] || selectedVariant);

  const title = game.title_zh || game.title || "";
  const titleEn = game.title_en && game.title_en !== title ? game.title_en : "";
  const server = activeGame.server || "天翼云盘";
  const version = activeGame.version || "";
  const validity = activeGame.validity || "";
  const size = activeGame.size_text || "-";
  const installSize = activeGame.install_size_text || activeGame.size_text || "-";

  const tags: { label: string; value: string }[] = [
    { label: "云盘", value: server },
    { label: "平台", value: platform.toUpperCase() },
  ];
  if (validity) tags.push({ label: "可玩性", value: validity });
  if (version) tags.push({ label: "版本", value: version });

  const [coverErr, setCoverErr] = useState(false);
  const effectiveCover = coverErr ? "" : coverUrl;

  const cardW = sourceRect.width;
  const cardH = sourceRect.height;
  const targetLeft = `calc(25vw - ${cardW / 2}px)`;
  const targetTop = `calc(50vh - ${cardH / 2}px)`;

  const startStyle: React.CSSProperties = {
    position: "fixed", left: sourceRect.left, top: sourceRect.top,
    width: cardW, height: cardH, borderRadius: 0,
    zIndex: 999, transition: "none",
  };
  const targetStyle: React.CSSProperties = {
    position: "fixed", left: targetLeft, top: targetTop,
    width: cardW, height: cardH, borderRadius: 0, zIndex: 999,
    transition: "all 420ms cubic-bezier(0.16, 1, 0.3, 1)",
  };

  useEffect(() => {
    if (phase !== "enter") return;
    const frame = requestAnimationFrame(() => {
      requestAnimationFrame(() => setPhase("active"));
    });
    return () => cancelAnimationFrame(frame);
  }, [phase]);

  // 动画完成后聚焦安装按钮
  useEffect(() => {
    if (phase !== "active") return;
    const t = setTimeout(() => {
      installBtnRef.current?.focus();
    }, 450);
    return () => clearTimeout(t);
  }, [phase]);

  const handleClose = useCallback((e?: CustomEvent) => {
    e?.preventDefault?.();
    e?.stopPropagation?.();
    if (closingRef.current) return;
    if (pollRef.current) { clearInterval(pollRef.current); pollRef.current = null; }
    closingRef.current = true;
    setPhase("exit");
    setTimeout(onClose, 400);
  }, [onClose]);

  const handleInstall = useCallback(async () => {
    if (!activeGame.down_url) {
      toaster.toast({ title: "安装失败", body: "缺少下载链接", duration: 3000 });
      return;
    }
    setDownloading(true);
    setDownloadError("");
    toaster.toast({ title: "开始下载", body: title, duration: 2000 });
    try {
      const r = await downloadStart({
        share_url: activeGame.down_url,
        file_name: activeGame.file_name || activeGame.title || "unknown",
        file_size: activeGame.size_bytes || 0,
        game_id: activeGame.game_id,
        game_title: title,
        access_code: activeGame.pwd || "",
      });
      if (r?.status === "success" && r.data) {
        onStartDownload(r.data, coverUrl);
        handleClose();
        return;
      } else {
        const err = r?.message || "启动下载失败";
        setDownloadError(err);
        setDownloading(false);
        toaster.toast({ title: "启动失败", body: err, duration: 4000 });
      }
    } catch (e: any) {
      const err = e?.message || String(e);
      setDownloadError(err);
      setDownloading(false);
      toaster.toast({ title: "错误", body: err, duration: 4000 });
    }
  }, [activeGame, title, coverUrl, onStartDownload]);

  const onInstallNav = useCallback((e: CustomEvent) => {
    const btn = Number(e?.detail?.button ?? -1);
    // UP → DropdownItem (如果有版本选择)
    if (btn === 1 && selectableVariants.length > 1) { dropdownRef.current?.focus(); e.preventDefault?.(); return; }
    // RIGHT → cancel
    if (btn === 12) { cancelBtnRef.current?.focus(); e.preventDefault?.(); }
  }, [selectableVariants.length]);
  const onCancelNav = useCallback((e: CustomEvent) => {
    const btn = Number(e?.detail?.button ?? -1);
    // UP → DropdownItem (如果有版本选择)
    if (btn === 1 && selectableVariants.length > 1) { dropdownRef.current?.focus(); e.preventDefault?.(); return; }
    // LEFT → install
    if (btn === 11) { installBtnRef.current?.focus(); e.preventDefault?.(); }
    // RIGHT → more
    if (btn === 12) { moreBtnRef.current?.focus(); e.preventDefault?.(); }
  }, [selectableVariants.length]);

  const handleMore = useCallback(() => {
    showContextMenu(
      <>
        <Menu label="游戏操作">
          <MenuItem onClick={() => toaster.toast({ title: "已加入稍后再玩", body: game.title_zh || game.title || "", duration: 2000 })}>
            加入稍后再玩
          </MenuItem>
          <MenuItem disabled>
            报告可玩性
          </MenuItem>
          <MenuItem disabled>
            查看详情
          </MenuItem>
        </Menu>
      </>,
      moreBtnRef.current,
      { bPreferPopTop: true },
    );
  }, [game]);

  const isActive = phase === "active";

  return (
    <div
      ref={overlayRef}
      style={{
        position: "fixed", inset: 0, zIndex: 998,
        background: "rgba(0,0,0,0)",
        transition: "background 350ms ease",
      }}
      className={isActive ? "nf-overlay-on" : ""}
    >
      <style>{OVERLAY_CSS}</style>

      {/* 卡片 — FLIP 动画 */}
      <div style={phase === "exit" ? startStyle : phase === "enter" ? startStyle : targetStyle}>
        <div className="nf-install-card-inner" style={{ borderRadius: 0 }}>
          <div className="nf-install-card-cover">
            {effectiveCover ? (
              <img src={effectiveCover} alt={title}
                onError={() => { setCoverErr(true); }}
                style={{ width: "100%", height: "100%", objectFit: "cover" }} />
            ) : (
              <div className="nf-install-cover-fb">{title.charAt(0)}</div>
            )}
          </div>
        </div>
        <div className="nf-install-backdrop"
          style={effectiveCover ? { backgroundImage: `url(${effectiveCover})` } : undefined} />
      </div>

      {/* 右侧面板 */}
      <div style={{
        position: "fixed", top: 0, left: "50vw", right: "calc(3vw)", bottom: 0,
        display: "flex", flexDirection: "column", justifyContent: "center",
        zIndex: 999, pointerEvents: "none",
        opacity: isActive ? 1 : 0,
        transform: isActive ? "translateX(0)" : "translateX(24px)",
        transition: "opacity 320ms ease 180ms, transform 380ms ease 120ms",
      }}>
        <h2 className="nf-install-title">{title}</h2>
        {titleEn && <div className="nf-install-title-en">{titleEn}</div>}

        <div className="nf-install-tags">
          {tags.map((t, i) => (
            <div key={i} className="nf-install-tag">
              <span className="nf-install-tag-label">{t.label}</span>
              <span className="nf-install-tag-value">{t.value}</span>
            </div>
          ))}
        </div>

        {/* 版本选择 — DropdownItem */}
        {selectableVariants.length > 1 && (
          <div style={{ marginBottom: 10, pointerEvents: "auto" }}>
            <Focusable
              ref={dropdownRef}
              onGamepadDirection={(e: CustomEvent) => {
                const btn = Number(e?.detail?.button ?? -1);
                // DOWN → install button
                if (btn === 2) { installBtnRef.current?.focus(); e.preventDefault?.(); }
              }}
              style={{ display: "block" }}
            >
              <DropdownItem
                label="版本"
                rgOptions={selectableVariants.map((v: CatalogItem) => ({
                  data: v,
                  label: `${v.version || v.title_zh || v.title || ""}${v.size_text ? `  (${v.size_text})` : ""}`,
                }))}
                selectedOption={selectedVariant}
                onChange={(opt: { data: CatalogItem }) => {
                  const idx = selectableVariants.findIndex(
                    (v) => v.game_id === opt.data.game_id && v.version === opt.data.version
                  );
                  if (idx >= 0) setSelectedIdx(idx);
                }}
                strDefaultLabel="选择版本"
                focusable={false}
              />
            </Focusable>
          </div>
        )}

        <div className="nf-install-size-block">
          <div className="nf-install-size-row">
            <span className="nf-install-size-label">下载体积</span>
            <span className="nf-install-size-value">{size}</span>
          </div>
          <div className="nf-install-size-row">
            <span className="nf-install-size-label">安装空间</span>
            <span className="nf-install-size-value">{installSize}</span>
          </div>
        </div>

        {/* 按钮 — Focusable，焦点默认在安装上 */}
        <div style={{ display: "flex", gap: 12, marginTop: 8, pointerEvents: "auto" }}>
          <div style={{ flex: 1 }}>
            <DialogButtonPrimary
              ref={installBtnRef}
              onOKButton={handleInstall}
              onCancelButton={handleClose}
              onGamepadDirection={onInstallNav}
              disabled={downloading}
              style={{ width: "100%" }}
            >
              {downloading ? "下载中..." : installDiscCount > 1 ? `安装全部 ${installDiscCount} 盘` : "安装"}
            </DialogButtonPrimary>
          </div>
          <div style={{ flex: 1 }}>
            <DialogButton
              ref={cancelBtnRef}
              onOKButton={handleClose}
              onCancelButton={handleClose}
              onGamepadDirection={onCancelNav}
              style={{ width: "100%" }}
            >
              取消
            </DialogButton>
          </div>
          <div style={{ flex: "0 0 auto", width: 40 }}>
            <DialogButton
              ref={moreBtnRef}
              onOKButton={handleMore}
              onOKActionDescription="更多操作"
              onGamepadDirection={(e: CustomEvent) => {
                const btn = Number(e?.detail?.button ?? -1);
                // LEFT → cancel
                if (btn === 11) { cancelBtnRef.current?.focus(); e.preventDefault?.(); }
              }}
              style={{ width: "100%", minWidth: 0, padding: "10px 0" }}
            >
              ···
            </DialogButton>
          </div>
        </div>

        {downloadTask && (
          <div style={{ marginTop: 16, padding: "12px 14px",
            background: "rgba(255,255,255,0.03)", borderRadius: 8,
            border: "1px solid rgba(255,255,255,0.05)" }}>
            <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 6 }}>
              <span style={{ fontSize: 12, color: "#8b929a" }}>
                {downloadTask.status === "complete" ? "下载完成" :
                 downloadTask.status === "failed" ? "下载失败" : "下载中"}
              </span>
              <span style={{ fontSize: 12, color: "#67c1f5", fontWeight: 600 }}>
                {formatSpeed(downloadTask.speed)}
              </span>
            </div>
            {/* 进度条 */}
            <div style={{ height: 4, background: "rgba(255,255,255,0.06)", borderRadius: 2, overflow: "hidden" }}>
              <div style={{ height: "100%", width: `${downloadTask.progress}%`,
                background: downloadTask.status === "failed" ? "#c44" :
                  downloadTask.status === "complete" ? "#5a5" : "#67c1f5",
                borderRadius: 2, transition: "width 500ms ease" }} />
            </div>
            <div style={{ display: "flex", justifyContent: "space-between", marginTop: 4 }}>
              <span style={{ fontSize: 11, color: "#6b727a" }}>
                {downloadTask.file_name?.substring(0, 30)}
              </span>
              <span style={{ fontSize: 11, color: "#6b727a" }}>
                {downloadTask.total > 0 ? `${formatBytes(downloadTask.completed)} / ${formatBytes(downloadTask.total)}` : `${downloadTask.progress}%`}
              </span>
            </div>
            {downloadError && (
              <div style={{ fontSize: 11, color: "#c44", marginTop: 4 }}>{downloadError}</div>
            )}
          </div>
        )}
      </div>
    </div>
  );
}

function formatBytes(bytes: number): string {
  if (!bytes || bytes <= 0) return "0 B";
  const units = ["B", "KB", "MB", "GB"];
  let i = 0;
  let v = bytes;
  while (v >= 1024 && i < units.length - 1) { v /= 1024; i++; }
  return `${v.toFixed(i > 0 ? 1 : 0)} ${units[i]}`;
}

function formatSpeed(bytesPerSec: number): string {
  if (!bytesPerSec || bytesPerSec <= 0) return "";
  return `${formatBytes(bytesPerSec)}/s`;
}

const OVERLAY_CSS = `
.nf-overlay-on{background:rgba(0,0,0,0.68)!important;backdrop-filter:blur(16px)}
.nf-install-card-inner{position:relative;width:100%;height:100%;overflow:hidden;z-index:1}
.nf-install-card-cover{width:100%;height:100%}
.nf-install-cover-fb{width:100%;height:100%;display:flex;align-items:center;justify-content:center;font-size:48px;color:rgba(255,255,255,0.1);background:rgba(255,255,255,0.03)}
.nf-install-backdrop{position:absolute;inset:-14px;background-position:center;background-repeat:no-repeat;background-size:cover;filter:blur(64px) saturate(1.5) brightness(1.2);opacity:0.65;z-index:0;border-radius:50%}
.nf-install-title{font-size:24px;font-weight:700;color:#f4f8fc;margin:0 0 2px;letter-spacing:0.01em;line-height:1.25;max-height:60px;overflow:hidden}
.nf-install-title-en{font-size:13px;color:#8b929a;margin-bottom:12px;line-height:1.3}
.nf-install-tags{display:flex;flex-wrap:wrap;gap:6px;margin-bottom:14px}
.nf-install-tag{display:flex;flex-direction:column;gap:1px;padding:5px 10px;background:rgba(255,255,255,0.04);border-radius:6px;border:1px solid rgba(255,255,255,0.06);min-width:0}
.nf-install-tag-label{font-size:9px;color:#6b727a;text-transform:uppercase;letter-spacing:0.06em;line-height:1}
.nf-install-tag-value{font-size:12px;font-weight:600;color:#cac4d0;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.nf-install-size-block{padding:10px 14px;background:rgba(255,255,255,0.03);border-radius:8px;border:1px solid rgba(255,255,255,0.05);margin-bottom:6px}
.nf-install-size-row{display:flex;align-items:center;justify-content:space-between;padding:3px 0}
.nf-install-size-row+.nf-install-size-row{border-top:1px solid rgba(255,255,255,0.04)}
.nf-install-size-label{font-size:12px;color:#8b929a}
.nf-install-size-value{font-size:13px;font-weight:600;color:#cac4d0}
`;
