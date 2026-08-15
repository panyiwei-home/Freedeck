/* ═══════════════════════════════════════════
   NewFreedeck — 游戏卡片 (1:1 复刻老插件)
   ═══════════════════════════════════════════ */

import { Focusable, gamepadLibraryClasses } from "@decky/ui";
import { useState, useCallback, memo, useEffect, useRef } from "react";
import type { CatalogItem } from "../api";
import type { MutableRefObject } from "react";

export function getLibraryFocusClassName(): string {
  const c = gamepadLibraryClasses as Record<string, string> | undefined;
  const bg = c?.["ItemFocusAnim-darkGrey"] || c?.["ItemFocusAnim-darkerGrey"] || "";
  const border = c?.["ItemFocusAnimBorder-darkGrey"] || "";
  const anim = c?.focusAnimation || "";
  return [bg, border, anim, "freedeck-library-grid-card-focus-state"].filter(Boolean).join(" ");
}

export const GameCard = memo(function GameCard(props: {
  item: CatalogItem;
  gridIndex: number;
  columns: number;
  priority: boolean;
  onNavigate: (idx: number, button: number) => boolean;
  onOpen: (item: CatalogItem) => void;
  cardRefs: MutableRefObject<Map<number, HTMLDivElement>>;
  onFocusIndex?: (idx: number) => void;
  onCancel?: (() => void) | null;
  onButtonDown?: ((idx: number, button: number) => boolean) | null;
}) {
  const { item, gridIndex, columns, priority, onNavigate, onOpen, cardRefs, onFocusIndex, onCancel, onButtonDown } = props;
  const [sideLeft, setSideLeft] = useState(false);
  const [imgErr, setImgErr] = useState(false);
  const rootRef = useRef<HTMLDivElement>(null);

  const cover = !imgErr ? (item.cover_url || "") : "";
  const title = item.title_zh || item.title || "";
  const subtitle = title !== item.title_en && item.title_en ? item.title_en : "";
  const size = item.size_text || "";
  const preloadedRef = useRef(false);

  const onFocus = useCallback(() => {
    setSideLeft(gridIndex % columns === columns - 1);
    onFocusIndex?.(gridIndex);
    // 聚焦时预热当前封面到浏览器缓存
    if (!preloadedRef.current && cover) {
      preloadedRef.current = true;
      const link = document.createElement("link");
      link.rel = "preload";
      link.as = "image";
      link.href = cover;
      document.head.appendChild(link);
    }
  }, [columns, gridIndex, onFocusIndex, cover]);

  useEffect(() => {
    if (rootRef.current) cardRefs.current.set(gridIndex, rootRef.current);
    return () => { cardRefs.current.delete(gridIndex); };
  }, [gridIndex, cardRefs]);

  return (
    <div
      ref={rootRef}
      className="freedeck-library-grid-card"
      onFocusCapture={onFocus}
      onMouseEnter={onFocus}
      style={{ position: "relative" }}
    >
      <Focusable
        className="freedeck-library-grid-card-button"
        focusClassName={getLibraryFocusClassName() || undefined}
        onFocus={onFocus}
        onOKButton={() => onOpen(item)}
        onOKActionDescription="选择"
        onCancelButton={onCancel || undefined}
        onCancel={onCancel || undefined}
        onCancelActionDescription={onCancel ? "返回" : undefined}
        onGamepadDirection={(e: CustomEvent) => {
          const btn = Number(e?.detail?.button ?? -1);
          if (btn >= 0 && onNavigate(gridIndex, btn)) {
            e.preventDefault?.();
            e.stopPropagation?.();
          }
        }}
        onButtonDown={(e: CustomEvent) => {
          const btn = Number(e?.detail?.button ?? -1);
          if (btn >= 0 && onButtonDown?.(gridIndex, btn)) {
            e.preventDefault?.();
            e.stopPropagation?.();
          }
        }}
        style={{
          width: "100%", height: "200px", minHeight: "200px", maxHeight: "200px",
          padding: "0", overflow: "visible", borderRadius: "0", border: "none",
          background: "transparent", boxShadow: "none", display: "block",
          cursor: "pointer", textAlign: "left",
          transition: "background 210ms ease",
        } as any}
      >
        <div className="freedeck-library-grid-card-frame">
          {/* 封面 */}
          <div className="freedeck-library-grid-card-cover-wrap">
            {cover ? (
              <img
                className="freedeck-library-grid-card-cover"
                src={cover}
                alt={title}
                loading={priority ? "eager" : "lazy"}
                decoding="async"
                referrerPolicy="no-referrer"
                onError={() => setImgErr(true)}
              />
            ) : null}
          </div>

          {/* 侧边信息 */}
          <div className={`freedeck-library-grid-card-sideinfo${sideLeft ? " is-sideinfo-left" : ""}`}>
            <div className="freedeck-library-grid-card-sideinfo-title">{title || "-"}</div>
            {subtitle && <div className="freedeck-library-grid-card-sideinfo-subtitle">{subtitle}</div>}
            {size && <div className="freedeck-library-grid-card-sideinfo-size">存储空间 {size}</div>}
            {(item as any).multi_disc && (
              <div style={{ fontSize: 11, color: "rgba(236,242,247,.72)", marginTop: 2 }}>
                {(item as any).disc_count} 光盘
              </div>
            )}
            {(item as any).grouped && !(item as any).multi_disc && (
              <div style={{ fontSize: 11, color: "rgba(236,242,247,.72)", marginTop: 2 }}>
                {(item as any).variant_count} 个版本
              </div>
            )}
          </div>
        </div>
      </Focusable>
    </div>
  );
});

export const LoadMoreCard = memo(function LoadMoreCard(props: {
  loading: boolean;
  onOpen: () => void;
}) {
  const focusClass = getLibraryFocusClassName();
  return (
    <div className="freedeck-library-grid-card freedeck-library-load-more-card" style={{ position: "relative" }}>
      <Focusable
        className="freedeck-library-grid-card-button"
        focusClassName={focusClass || undefined}
        onOKButton={props.onOpen}
        onOKActionDescription="加载更多"
        style={{
          width: "100%", height: "200px", minHeight: "200px", maxHeight: "200px",
          padding: "0", overflow: "visible", borderRadius: "0",
          background: "transparent", display: "block",
          pointerEvents: props.loading ? "none" : "auto",
          opacity: props.loading ? 0.92 : 1,
        } as any}
      >
        <div className="freedeck-library-grid-card-frame freedeck-library-load-more-card-frame">
          <div className="freedeck-library-load-more-card-inner">
            <div className="freedeck-library-load-more-card-title">
              {props.loading ? "加载中..." : "加载更多"}
            </div>
            <div className="freedeck-library-load-more-card-subtitle">
              继续获取更多资源
            </div>
          </div>
        </div>
      </Focusable>
    </div>
  );
});
