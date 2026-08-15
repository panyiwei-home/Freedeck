/* ═══════════════════════════════════════════
   NewFreedeck — 轮播图模块 (CarouselBanner)
   基于 @decky/ui Carousel + Focusable 手柄导航
   ═══════════════════════════════════════════ */

import { Carousel, Focusable, gamepadLibraryClasses } from "@decky/ui";
import { useMemo, useRef, useCallback } from "react";
import type { ReactNode } from "react";

/* ── 类型 ────────────────────────────── */

export interface CarouselBannerItem {
  id: string;
  imageUrl: string;
  title: string;
  subtitle?: string;
  onClick?: () => void;
}

interface CarouselBannerProps {
  items: CarouselBannerItem[];
  height?: number;
}

/* ── 焦点样式 ─────────────────────────── */

function useFocusClass(): string {
  return useMemo(() => {
    const c = gamepadLibraryClasses as Record<string, string> | undefined;
    return c?.["ItemFocusAnim-darkGrey"] || c?.["ItemFocusAnim-darkerGrey"] || "";
  }, []);
}

/* ═══════════════════════════════════════════
   CarouselBanner
   ═══════════════════════════════════════════ */

export function CarouselBanner({ items, height = 200 }: CarouselBannerProps) {
  const focusClass = useFocusClass();
  const containerRef = useRef<HTMLDivElement>(null);

  /* ── 占位状态 ── */
  if (items.length === 0) {
    return (
      <div style={{
        width: "100%", height,
        background: "rgba(255,255,255,0.03)",
        border: "1px solid rgba(255,255,255,0.06)",
        borderRadius: 8,
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        color: "#6b727a",
        fontSize: 13,
        marginBottom: 16,
        boxSizing: "border-box",
        userSelect: "none",
      }}>
        即将推出更多精选内容
      </div>
    );
  }

  /* ── 轮播项渲染 ── */
  const renderItem = useCallback((id: number): ReactNode => {
    const item = items[id];
    if (!item) return null;

    return (
      <Focusable
        key={item.id}
        className="nf-carousel-item"
        focusClassName={focusClass || undefined}
        onOKButton={() => item.onClick?.()}
        onOKActionDescription={item.title}
        onActivate={() => item.onClick?.()}
        style={{
          width: "100%",
          height: height - 8,
          borderRadius: 8,
          overflow: "hidden",
          position: "relative",
          display: "block",
          background: "rgba(255,255,255,0.03)",
          border: "1px solid rgba(255,255,255,0.06)",
          boxSizing: "border-box",
        } as any}
      >
        {/* 背景图 */}
        <div style={{
          position: "absolute", inset: 0,
          backgroundImage: `url(${item.imageUrl})`,
          backgroundSize: "cover",
          backgroundPosition: "center",
          filter: "brightness(0.7)",
        }} />
        {/* 底部渐变 + 标题 */}
        <div style={{
          position: "absolute", inset: 0,
          background: "linear-gradient(to top, rgba(0,0,0,0.7) 0%, transparent 50%)",
        }} />
        <div style={{
          position: "absolute", bottom: 0, left: 0, right: 0,
          padding: "12px 14px",
        }}>
          <div style={{ fontSize: 15, fontWeight: 700, color: "#f4f8fc", lineHeight: 1.2 }}>
            {item.title}
          </div>
          {item.subtitle ? (
            <div style={{ fontSize: 11, color: "#8b929a", marginTop: 2 }}>{item.subtitle}</div>
          ) : null}
        </div>
      </Focusable>
    );
  }, [items, focusClass, height]);

  /* ── 列宽计算：铺满容器，左右留少量边距 ── */
  const getColumnWidth = useCallback(() => {
    const el = containerRef.current;
    const w = el ? el.clientWidth : 800;
    // 卡片宽度 ≈ 容器宽度 - 左右边距，保留一点间隙
    return Math.max(200, w - 32);
  }, []);

  return (
    <div ref={containerRef} style={{ width: "100%", marginBottom: 16, overflow: "visible" }}>
      <style>{CAROUSEL_CSS}</style>
      <Carousel
        name="freedeck-carousel"
        nNumItems={items.length}
        nHeight={height}
        nItemHeight={height - 8}
        nItemMarginX={8}
        autoFocus={false}
        enableBumperPaging={false}
        scrollToAlignment="center"
        fnGetId={(id: number) => id}
        fnGetColumnWidth={getColumnWidth}
        fnDoesItemTakeFocus={() => true}
        fnItemRenderer={renderItem}
      />
    </div>
  );
}

/* ── CSS ─────────────────────────────── */

const CAROUSEL_CSS = `
.nf-carousel-item:focus {
  outline: none;
}
`;