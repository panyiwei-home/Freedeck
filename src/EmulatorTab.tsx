/* ═══════════════════════════════════════════
   NewFreedeck — EmulatorTab
   目标：1:1 贴近老插件的模拟器总览与平台列表交互
   ═══════════════════════════════════════════ */

import { Focusable, SteamSpinner } from "@decky/ui";
import { useState, useEffect, useRef, useCallback, useMemo } from "react";
import { getPlatformGames } from "./api";
import { GameCard, getLibraryFocusClassName } from "./library/GameCard";
import { focusNavigableElementWithRetry } from "./library/focus";
import type { CatalogItem } from "./api";

type EmulatorMode = "switch" | "ps1" | "ps2" | "ps3" | "psp" | "3ds" | "gba" | "nds" | "fc" | "md";

const SERVE_BASE = "http://127.0.0.1:20065/emulator_icons";
const LIBRARY_GRID_COLUMNS = 5;
const COLLECTION_GRID_COLUMNS = 4;
const COLLECTION_OVERVIEW_REQUEST_PAGE_SIZE = 9;
const PAGE_SIZE = 30;
const GRID_CARD_W = 133;
const GRID_GAP = 33;

interface PlatformDef {
  id: EmulatorMode;
  title: string;
  icon: string;
  surface: {
    bottom: string;
    mid: string;
    top: string;
  };
}

interface EmulatorCollectionEntry {
  mode: EmulatorMode;
  title: string;
  total: number;
  samples: CatalogItem[];
  loading?: boolean;
  loadError?: string;
}

const PLATFORMS: PlatformDef[] = [
  { id: "switch", title: "Switch", icon: "switch.webp", surface: { bottom: "#e74a2f", mid: "#8f1f28", top: "#1d1319" } },
  { id: "ps1", title: "PS1", icon: "ps1.webp", surface: { bottom: "#7f8cff", mid: "#384a98", top: "#171726" } },
  { id: "ps2", title: "PS2", icon: "ps2.webp", surface: { bottom: "#3b83ff", mid: "#1747a0", top: "#121828" } },
  { id: "ps3", title: "PS3", icon: "ps3.webp", surface: { bottom: "#7f6cff", mid: "#4936a3", top: "#171523" } },
  { id: "psp", title: "PSP", icon: "psp.webp", surface: { bottom: "#f39a30", mid: "#9a4e16", top: "#231711" } },
  { id: "3ds", title: "3DS", icon: "3ds-alt.webp", surface: { bottom: "#db7a34", mid: "#8c4219", top: "#25150f" } },
  { id: "gba", title: "GBA", icon: "gba.webp", surface: { bottom: "#8db43f", mid: "#4c6d1e", top: "#151d12" } },
  { id: "nds", title: "NDS", icon: "nds.webp", surface: { bottom: "#57b198", mid: "#2d6d5d", top: "#121d19" } },
  { id: "fc", title: "FC", icon: "fc.webp", surface: { bottom: "#f08a35", mid: "#a2471f", top: "#23130f" } },
  { id: "md", title: "MD", icon: "md.webp", surface: { bottom: "#4d96e8", mid: "#2352a2", top: "#10192a" } },
];

function buildInitialEntries(): EmulatorCollectionEntry[] {
  return PLATFORMS.map((platform) => ({
    mode: platform.id,
    title: platform.title,
    total: 0,
    samples: [],
    loading: true,
    loadError: "",
  }));
}

function getCollectionPlatform(mode: EmulatorMode): PlatformDef {
  return PLATFORMS.find((platform) => platform.id === mode) || PLATFORMS[0];
}

function getCollectionGridVirtualPosition(index: number): { row: number; column: number } {
  const safeIndex = Math.max(0, index);
  return {
    row: Math.floor(safeIndex / COLLECTION_GRID_COLUMNS),
    column: safeIndex % COLLECTION_GRID_COLUMNS,
  };
}

function getFixedGridRowBounds(index: number, total: number, columns: number): { rowStart: number; rowEnd: number } {
  const safeIndex = Math.max(0, Math.min(index, Math.max(total - 1, 0)));
  const rowStart = Math.floor(safeIndex / columns) * columns;
  const rowEnd = Math.min(rowStart + columns - 1, Math.max(total - 1, 0));
  return { rowStart, rowEnd };
}

function findFixedGridVerticalTargetIndex(
  currentIndex: number,
  total: number,
  columns: number,
  direction: "up" | "down",
): number {
  const candidate = currentIndex + (direction === "up" ? -columns : columns);
  if (candidate < 0 || candidate >= total) return -1;
  return candidate;
}

function getEntryStatusText(entry: EmulatorCollectionEntry): string {
  if (entry.loadError) return "读取失败";
  if (entry.loading) return "目录准备中";
  if (entry.total > 0) return `${entry.total} 款`;
  return "空目录";
}

function getCoverUrl(item: CatalogItem | null | undefined): string {
  return String(item?.cover_url || "").trim();
}

function CollectionCard(props: {
  entry: EmulatorCollectionEntry;
  buttonRef?: ((node: HTMLDivElement | null) => void) | null;
  onNavigate: (mode: EmulatorMode, button: number) => boolean;
  onOpen: (entry: EmulatorCollectionEntry) => void;
}) {
  const platform = useMemo(() => getCollectionPlatform(props.entry.mode), [props.entry.mode]);
  const focusClassName = useMemo(() => getLibraryFocusClassName(), []);
  const sampleTiles = useMemo(() => {
    return props.entry.samples.slice(0, COLLECTION_OVERVIEW_REQUEST_PAGE_SIZE).map((sample, index) => ({
      key: `${props.entry.mode}:${sample.game_id || sample.title || index}`,
      url: getCoverUrl(sample),
      opacity: Math.max(0.48, 0.94 - index * 0.04),
    }));
  }, [props.entry.mode, props.entry.samples]);

  return (
    <div className="freedeck-library-collection-card" style={{ position: "relative" }}>
      <Focusable
        ref={props.buttonRef || undefined}
        className="freedeck-library-collection-card-button"
        focusClassName={focusClassName || undefined}
        onClick={() => props.onOpen(props.entry)}
        onOKButton={() => props.onOpen(props.entry)}
        onOKActionDescription="进入列表"
        onGamepadDirection={(event) => {
          const handled = props.onNavigate(props.entry.mode, Number(event?.detail?.button || 0));
          if (!handled) return;
          event?.preventDefault?.();
          event?.stopPropagation?.();
        }}
        style={{
          width: "100%",
          height: "184px",
          minHeight: "184px",
          maxHeight: "184px",
          padding: "0",
          overflow: "visible",
          borderRadius: "0",
          border: "none",
          background: "transparent",
          boxShadow: "none",
          display: "block",
          cursor: "pointer",
        }}
      >
        <div className="freedeck-library-collection-card-frame">
          <div className="freedeck-library-collection-card-backdrop" />
          <div
            className="freedeck-library-collection-card-surface"
            style={{
              background: `linear-gradient(to top, ${platform.surface.bottom} 0%, ${platform.surface.mid} 52%, ${platform.surface.top} 100%)`,
            }}
          >
            <div className="freedeck-library-collection-card-mosaic" aria-hidden="true">
              {sampleTiles.length > 0
                ? sampleTiles.map((tile) => (
                    <div
                      key={tile.key}
                      className={`freedeck-library-collection-card-mosaic-tile${tile.url ? "" : " is-static"}`}
                      style={{ opacity: tile.opacity }}
                    >
                      {tile.url ? (
                        <img
                          className="freedeck-library-collection-card-mosaic-image"
                          src={tile.url}
                          alt=""
                          loading="eager"
                          decoding="async"
                          referrerPolicy="no-referrer"
                        />
                      ) : null}
                    </div>
                  ))
                : Array.from({ length: COLLECTION_OVERVIEW_REQUEST_PAGE_SIZE }, (_, index) => (
                    <div
                      key={`${props.entry.mode}:placeholder:${index}`}
                      className="freedeck-library-collection-card-mosaic-tile is-static"
                      style={{ opacity: Math.max(0.42, 0.9 - index * 0.04) }}
                    />
                  ))}
            </div>
            <div className="freedeck-library-collection-card-scrim" />
            <div
              className={`freedeck-library-collection-card-platform-badge${props.entry.mode === "fc" ? " is-fc-badge" : ""}`}
              aria-hidden="true"
            >
              <img
                className={`freedeck-library-collection-card-platform-image${props.entry.mode === "fc" ? " is-fc-platform-image" : ""}`}
                src={`${SERVE_BASE}/${platform.icon}`}
                referrerPolicy="no-referrer"
              />
            </div>
            <div className="freedeck-library-collection-card-content">
              <div className="freedeck-library-collection-card-title">{props.entry.title}</div>
              <div className="freedeck-library-collection-card-count">{getEntryStatusText(props.entry)}</div>
            </div>
          </div>
        </div>
      </Focusable>
    </div>
  );
}

function PlatformGameGrid(props: {
  platform: PlatformDef;
  onBack: () => void;
  onOpen: (item: CatalogItem, el: HTMLElement) => void;
}) {
  const [games, setGames] = useState<CatalogItem[]>([]);
  const [loading, setLoading] = useState(true);
  const [loadingMore, setLoadingMore] = useState(false);
  const [page, setPage] = useState(1);
  const [hasMore, setHasMore] = useState(false);
  const [errorMessage, setErrorMessage] = useState("");
  const cardRefs = useRef<Map<number, HTMLDivElement>>(new Map());
  const backButtonRef = useRef<HTMLDivElement | null>(null);
  const loadMoreRef = useRef<HTMLDivElement | null>(null);
  const initialFocusPendingRef = useRef(true);
  const lastFocusedCardIndexRef = useRef(0);
  const pendingRestoreIndexRef = useRef<number | null>(null);
  const LOAD_MORE_IDX = -1;

  const focusBackButton = useCallback(() => {
    return focusNavigableElementWithRetry(() => backButtonRef.current);
  }, []);

  const focusCard = useCallback((idx: number) => {
    return focusNavigableElementWithRetry(() => {
      const wrapper = cardRefs.current.get(idx);
      return wrapper?.querySelector(".freedeck-library-grid-card-button") as HTMLElement | null;
    });
  }, []);

  const focusLoadMore = useCallback(() => {
    return focusNavigableElementWithRetry(() => loadMoreRef.current);
  }, []);

  const restoreFocusedCard = useCallback((idx: number | null) => {
    if (idx == null || games.length <= 0) return false;
    const safe = Math.max(0, Math.min(idx, games.length - 1));
    lastFocusedCardIndexRef.current = safe;
    return focusCard(safe);
  }, [focusCard, games.length]);

  const handleCardFocus = useCallback((idx: number) => {
    lastFocusedCardIndexRef.current = idx;
  }, []);

  useEffect(() => {
    initialFocusPendingRef.current = true;
    setLoading(true);
    setLoadingMore(false);
    setErrorMessage("");
    setGames([]);
    setHasMore(false);
    setPage(1);

    getPlatformGames({ platform: props.platform.id, page: 1, page_size: PAGE_SIZE })
      .then((result) => {
        if (result?.status !== "success" || !result.data) {
          setErrorMessage(String(result?.message || `读取 ${props.platform.title} 目录失败`));
          return;
        }
        setGames(result.data.games as CatalogItem[]);
        setHasMore(Boolean(result.data.has_more));
        setPage(1);
      })
      .catch((error) => {
        setErrorMessage(String(error || `读取 ${props.platform.title} 目录失败`));
      })
      .finally(() => {
        setLoading(false);
      });
  }, [props.platform.id, props.platform.title]);

  useEffect(() => {
    if (loading) return;
    if (!initialFocusPendingRef.current) return;
    initialFocusPendingRef.current = false;
    if (games.length > 0) {
      focusCard(0);
      return;
    }
    focusBackButton();
  }, [focusBackButton, focusCard, games.length, loading]);

  useEffect(() => {
    if (loadingMore) return;
    if (pendingRestoreIndexRef.current == null) return;
    const restoreIndex = pendingRestoreIndexRef.current;
    pendingRestoreIndexRef.current = null;
    restoreFocusedCard(restoreIndex);
  }, [games.length, loadingMore, restoreFocusedCard]);

  const loadMore = useCallback(async () => {
    if (loadingMore || !hasMore) return;
    setLoadingMore(true);
    const next = page + 1;
    try {
      const result = await getPlatformGames({ platform: props.platform.id, page: next, page_size: PAGE_SIZE });
      if (result?.status === "success" && result.data?.games?.length) {
        setGames((prev) => [...prev, ...(result.data!.games as CatalogItem[])]);
        setHasMore(Boolean(result.data.has_more));
        setPage(next);
      }
    } catch {
      // ignore
    } finally {
      setLoadingMore(false);
    }
  }, [hasMore, loadingMore, page, props.platform.id]);

  const handleLoadMore = useCallback(() => {
    if (loadingMore || !hasMore) return;
    // 加载后焦点跳到第一张新卡片
    pendingRestoreIndexRef.current = games.length;
    void loadMore();
  }, [games.length, hasMore, loadMore, loadingMore]);

  const onNavigateGrid = useCallback((idx: number, button: number) => {
    if (idx === LOAD_MORE_IDX) {
      if (button === 9) {
        return restoreFocusedCard(lastFocusedCardIndexRef.current);
      }
      return true;
    }

    const { rowStart, rowEnd } = getFixedGridRowBounds(idx, games.length, LIBRARY_GRID_COLUMNS);
    if (button === 11) {
      if (idx > rowStart) return focusCard(idx - 1);
      return true;
    }
    if (button === 12) {
      if (idx < rowEnd && idx + 1 < games.length) return focusCard(idx + 1);
      return true;
    }
    if (button === 9) {
      const targetIndex = findFixedGridVerticalTargetIndex(idx, games.length, LIBRARY_GRID_COLUMNS, "up");
      if (targetIndex >= 0) return focusCard(targetIndex);
      return focusBackButton();
    }
    if (button === 10) {
      const targetIndex = findFixedGridVerticalTargetIndex(idx, games.length, LIBRARY_GRID_COLUMNS, "down");
      if (targetIndex >= 0) return focusCard(targetIndex);
      if (hasMore && !loadingMore) {
        lastFocusedCardIndexRef.current = idx;
        return focusLoadMore();
      }
      return true;
    }
    return false;
  }, [focusBackButton, focusCard, focusLoadMore, games.length, hasMore, loadingMore, restoreFocusedCard]);

  const onCardOpen = useCallback((item: CatalogItem) => {
    for (const [idx, wrapper] of cardRefs.current) {
      if (wrapper && games[idx]?.game_id === item.game_id) {
        props.onOpen(item, wrapper);
        return;
      }
    }
  }, [games, props]);

  if (loading) {
    return (
      <div className="freedeck-native-library">
        <div className="freedeck-library-grid-stage">
          <div style={{ display: "flex", justifyContent: "center", padding: "48px 0 72px" }}>
            <SteamSpinner style={{ width: 40, height: 40 }} />
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="freedeck-native-library">
      <div className="freedeck-library-grid-stage">
        <div className="nf-emulator-header-row">
          <Focusable
            ref={backButtonRef as any}
            className="nf-emulator-back-button"
            onClick={props.onBack}
            onOKButton={props.onBack}
            onCancelButton={props.onBack}
            onCancel={props.onBack}
            onOKActionDescription="返回平台列表"
            onCancelActionDescription="返回"
            onGamepadDirection={(event) => {
              const button = Number(event?.detail?.button || 0);
              if (button === 10) {
                if (games.length > 0) {
                  focusCard(Math.max(0, Math.min(lastFocusedCardIndexRef.current, games.length - 1)));
                  event?.preventDefault?.();
                  event?.stopPropagation?.();
                }
                return;
              }
              if (button === 11 || button === 12) {
                event?.preventDefault?.();
                event?.stopPropagation?.();
              }
            }}
          >
            ← 返回
          </Focusable>
          <div className="freedeck-library-platform-section-title">{props.platform.title} 游戏</div>
        </div>

        {errorMessage ? (
          <div className="freedeck-library-empty">{`加载失败：${errorMessage}`}</div>
        ) : games.length === 0 ? (
          <div className="freedeck-library-empty">当前没有可展示的模拟器目录。</div>
        ) : (
          <div className="freedeck-library-grid">
            {games.map((item, index) => (
              <GameCard
                key={item.game_id || `${props.platform.id}:${index}`}
                item={item}
                gridIndex={index}
                columns={LIBRARY_GRID_COLUMNS}
                priority={index < 20}
                onNavigate={onNavigateGrid}
                onOpen={onCardOpen}
                cardRefs={cardRefs}
                onFocusIndex={handleCardFocus}
                onCancel={props.onBack}
              />
            ))}
            {hasMore ? (
              <div
                ref={(node) => {
                  loadMoreRef.current = node;
                  if (node) cardRefs.current.set(LOAD_MORE_IDX, node);
                }}
                className="freedeck-library-grid-card freedeck-library-load-more-card"
                style={{ position: "relative" }}
              >
                <Focusable
                  className="freedeck-library-grid-card-button"
                  focusClassName={getLibraryFocusClassName() || undefined}
                  onClick={loadingMore ? undefined : handleLoadMore}
                  onOKButton={loadingMore ? undefined : handleLoadMore}
                  onCancelButton={props.onBack}
                  onCancel={props.onBack}
                  onOKActionDescription={loadingMore ? undefined : "加载更多"}
                  onCancelActionDescription="返回"
                  onGamepadDirection={(event) => {
                    const handled = onNavigateGrid(LOAD_MORE_IDX, Number(event?.detail?.button || 0));
                    if (!handled) return;
                    event?.preventDefault?.();
                    event?.stopPropagation?.();
                  }}
                  style={{
                    width: "100%",
                    height: "200px",
                    minHeight: "200px",
                    maxHeight: "200px",
                    padding: "0",
                    overflow: "visible",
                    borderRadius: "0",
                    background: "transparent",
                    display: "block",
                    pointerEvents: loadingMore ? "none" : "auto",
                    opacity: loadingMore ? 0.92 : 1,
                  }}
                >
                  <div className="freedeck-library-grid-card-frame freedeck-library-load-more-card-frame">
                    <div className="freedeck-library-load-more-card-inner">
                      <div className="freedeck-library-load-more-card-title">{loadingMore ? "加载中..." : "加载更多"}</div>
                      <div className="freedeck-library-load-more-card-subtitle">继续获取更多资源</div>
                    </div>
                  </div>
                </Focusable>
              </div>
            ) : null}
          </div>
        )}
      </div>
    </div>
  );
}

const EMULATOR_TAB_CSS = `
.freedeck-native-library {
  width: 100%;
}
.freedeck-native-library .freedeck-library-grid-stage {
  width: 100%;
  max-width: 806px;
  margin: 0 auto;
  padding: 0 4px 20px;
  box-sizing: border-box;
  display: flex;
  flex-direction: column;
  gap: 16px;
}
.freedeck-native-library .freedeck-library-collection-stage {
  width: 100%;
  max-width: 752px;
  margin: 0 auto;
  display: flex;
  flex-direction: column;
  gap: 0;
}
.freedeck-native-library .freedeck-library-collection-grid {
  display: grid;
  grid-template-columns: repeat(${COLLECTION_GRID_COLUMNS}, 176px);
  column-gap: 16px;
  row-gap: 16px;
  width: 100%;
  max-width: 100%;
  margin: 0;
  padding: 0 0 20px;
  justify-content: center;
}
.freedeck-native-library .freedeck-library-collection-card {
  width: 176px;
  height: 176px;
  min-width: 0;
  position: relative;
  isolation: isolate;
}
.freedeck-native-library .freedeck-library-collection-card > div {
  width: 176px !important;
  height: 176px !important;
  min-height: 176px !important;
  max-height: 176px !important;
  padding: 0 !important;
  overflow: visible !important;
}
.freedeck-native-library .freedeck-library-collection-card-button {
  position: relative;
  z-index: 1;
  transform: none;
  transition: none;
  filter: brightness(1);
}
.freedeck-native-library .freedeck-library-collection-card-button.freedeck-library-grid-card-focus-state {
  z-index: 8;
}
.freedeck-native-library .freedeck-library-collection-card-button.freedeck-library-grid-card-focus-state .freedeck-library-collection-card-surface {
  box-shadow:
    0 16px 34px rgba(4, 11, 20, 0.42),
    inset 0 0 0 2px rgba(255,255,255,0.2);
}
.freedeck-native-library .freedeck-library-collection-card-frame {
  position: relative;
  width: 100%;
  height: 176px;
  overflow: visible;
}
.freedeck-native-library .freedeck-library-collection-card-backdrop {
  display: none;
}
.freedeck-native-library .freedeck-library-collection-card-surface {
  position: relative;
  width: 100%;
  height: 100%;
  overflow: hidden;
  box-shadow: 0 12px 18px rgba(0, 0, 0, 0.2), inset 0 0 0 1px rgba(255,255,255,0.08);
}
.freedeck-native-library .freedeck-library-collection-card-mosaic {
  position: absolute;
  left: 50%;
  top: 44.5%;
  width: 188px;
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  column-gap: 10px;
  row-gap: 16px;
  align-content: start;
  justify-content: center;
  justify-items: center;
  overflow: visible;
  transform-origin: center center;
  transform: translate3d(-50%, -50%, 0) perspective(760px) rotateX(62deg) rotateZ(-35deg) scale(1.52);
  pointer-events: none;
}
.freedeck-native-library .freedeck-library-collection-card-mosaic::after {
  content: "";
  position: absolute;
  inset: -8px;
  background: linear-gradient(180deg, rgba(8, 12, 18, 0.08), rgba(8, 12, 18, 0.18));
  mix-blend-mode: multiply;
  pointer-events: none;
}
.freedeck-native-library .freedeck-library-collection-card-mosaic-tile {
  position: relative;
  width: 100%;
  aspect-ratio: 0.68 / 1;
  overflow: hidden;
  box-shadow:
    0 12px 18px rgba(0, 0, 0, 0.32),
    0 4px 8px rgba(0, 0, 0, 0.18),
    inset 0 0 0 1px rgba(255,255,255,0.14);
  filter: brightness(0.97) contrast(0.94);
}
.freedeck-native-library .freedeck-library-collection-card-mosaic-tile.is-static {
  background:
    linear-gradient(180deg, rgba(255,255,255,0.2), rgba(255,255,255,0.04)),
    linear-gradient(135deg, rgba(255,255,255,0.08), rgba(10,16,22,0.06));
}
.freedeck-native-library .freedeck-library-collection-card-mosaic-image {
  width: 100%;
  height: 100%;
  display: block;
  object-fit: cover;
  object-position: center top;
  transform: translateZ(0);
  backface-visibility: hidden;
}
.freedeck-native-library .freedeck-library-collection-card-mosaic-tile::after {
  content: "";
  position: absolute;
  inset: 0;
  background: linear-gradient(180deg, rgba(255, 255, 255, 0.08), rgba(6, 10, 14, 0.16));
  pointer-events: none;
}
.freedeck-native-library .freedeck-library-collection-card-scrim {
  position: absolute;
  inset: 0;
  background:
    linear-gradient(180deg, rgba(9, 12, 16, 0.1) 0%, rgba(9, 12, 16, 0.18) 24%, rgba(9, 12, 16, 0.44) 58%, rgba(9, 12, 16, 0.76) 100%),
    linear-gradient(145deg, rgba(255,255,255,0.08), transparent 28%);
  pointer-events: none;
}
.freedeck-native-library .freedeck-library-collection-card-platform-badge {
  position: absolute;
  inset: 0;
  display: flex;
  align-items: center;
  justify-content: center;
  opacity: 0.96;
  filter: saturate(0.84) brightness(0.92) contrast(0.96) drop-shadow(0 12px 22px rgba(0, 0, 0, 0.34));
  pointer-events: none;
}
.freedeck-native-library .freedeck-library-collection-card-platform-image {
  width: 86%;
  height: 54%;
  object-fit: contain;
  object-position: center center;
  display: block;
}
.freedeck-native-library .freedeck-library-collection-card-platform-badge.is-fc-badge {
  filter: saturate(0.9) brightness(0.95) contrast(0.98) drop-shadow(0 14px 24px rgba(0, 0, 0, 0.32));
}
.freedeck-native-library .freedeck-library-collection-card-platform-image.is-fc-platform-image {
  width: 100%;
  height: 76%;
  transform: translateY(-2px);
}
.freedeck-native-library .freedeck-library-collection-card-content {
  position: absolute;
  z-index: 100;
  inset: 0;
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: flex-end;
  padding: 14px 14px 16px;
  text-align: center;
  color: #fff7ee;
  text-shadow: 0 2px 10px rgba(0, 0, 0, 0.38);
  pointer-events: none;
}
.freedeck-native-library .freedeck-library-collection-card-title {
  font-size: 18px;
  line-height: 1.08;
  font-weight: 900;
  letter-spacing: 0.08em;
  margin-bottom: 4px;
}
.freedeck-native-library .freedeck-library-collection-card-count {
  font-size: 16px;
  line-height: 1;
  font-weight: 600;
  letter-spacing: 0.16em;
}
.freedeck-native-library .freedeck-library-grid {
  display: grid;
  grid-template-columns: repeat(${LIBRARY_GRID_COLUMNS}, ${GRID_CARD_W}px);
  column-gap: ${GRID_GAP}px;
  row-gap: 37px;
  width: 100%;
  max-width: 100%;
  margin: 0;
  padding: 0 0 20px;
  align-items: start;
  justify-items: center;
  justify-content: center;
}
.freedeck-native-library .freedeck-library-grid-card {
  width: ${GRID_CARD_W}px;
  height: 200px;
  min-width: 0;
  align-self: start;
  display: block;
}
.freedeck-native-library .freedeck-library-grid-card > div {
  width: ${GRID_CARD_W}px !important;
  height: 200px !important;
  min-height: 200px !important;
  max-height: 200px !important;
  padding: 0 !important;
  overflow: visible !important;
}
.freedeck-native-library .freedeck-library-empty {
  width: 100%;
  padding: 48px 20px 60px;
  text-align: center;
  color: #8b929a;
  font-size: 13px;
  box-sizing: border-box;
}
.nf-emulator-header-row {
  display: flex;
  align-items: center;
  gap: 12px;
  width: 100%;
  max-width: 806px;
  margin: 0 auto;
  padding: 2px 4px 0;
  box-sizing: border-box;
}
.nf-emulator-back-button {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  min-height: 32px;
  padding: 0 10px;
  border-radius: 999px;
  background: rgba(18, 27, 39, 0.76);
  border: 1px solid rgba(255,255,255,0.1);
  color: #cdd5de;
  font-size: 13px;
  box-shadow: inset 0 1px 0 rgba(255,255,255,0.06);
}
.nf-emulator-back-button:focus-within {
  border-color: rgba(111, 187, 255, 0.72);
  box-shadow: 0 0 0 2px rgba(54,159,244,0.24), inset 0 1px 0 rgba(255,255,255,0.12);
}
@media (max-width: 780px) {
  .freedeck-native-library .freedeck-library-collection-stage {
    max-width: 560px;
  }
  .freedeck-native-library .freedeck-library-collection-grid {
    grid-template-columns: repeat(3, 176px);
  }
}
`;

// 模块级缓存：组件销毁/重建时保持不变，避免切 tab 回来重复加载
let _sCachedEntries: EmulatorCollectionEntry[] | null = null;

export function EmulatorTab(props: {
  onOpen: (item: CatalogItem, el: HTMLElement) => void;
  autoFocusFirst?: boolean;
  onAutoFocused?: () => void;
}) {
  const [entries, setEntries] = useState<EmulatorCollectionEntry[]>(
    () => _sCachedEntries || buildInitialEntries(),
  );
  const [loadingCollections, setLoadingCollections] = useState(
    () => _sCachedEntries === null,
  );
  const [currentPlatformMode, setCurrentPlatformMode] = useState<EmulatorMode | null>(null);
  const collectionButtonRefs = useRef<Record<string, HTMLDivElement | null>>({});
  const pendingCollectionFocusModeRef = useRef<EmulatorMode | null>(null);

  const currentPlatform = useMemo(
    () => (currentPlatformMode ? getCollectionPlatform(currentPlatformMode) : null),
    [currentPlatformMode],
  );

  const focusCollectionCard = useCallback((mode: EmulatorMode | null) => {
    if (!mode) return false;
    return focusNavigableElementWithRetry(() => collectionButtonRefs.current[mode] || null);
  }, []);

  const loadCollections = useCallback(async () => {
    setLoadingCollections(true);
    setEntries(buildInitialEntries());

    await Promise.all(PLATFORMS.map(async (platform) => {
      try {
        const result = await getPlatformGames({
          platform: platform.id,
          page: 1,
          page_size: COLLECTION_OVERVIEW_REQUEST_PAGE_SIZE,
        });
        setEntries((prev) => prev.map((entry) => {
          if (entry.mode !== platform.id) return entry;
          if (result?.status !== "success" || !result.data) {
            return {
              ...entry,
              loading: false,
              loadError: String(result?.message || `读取 ${platform.title} 目录失败`),
            };
          }
          return {
            ...entry,
            loading: false,
            loadError: "",
            total: Number(result.data.total || 0),
            samples: Array.isArray(result.data.games) ? (result.data.games as CatalogItem[]) : [],
          };
        }));
      } catch (error) {
        setEntries((prev) => prev.map((entry) => (
          entry.mode === platform.id
            ? { ...entry, loading: false, loadError: String(error || `读取 ${platform.title} 目录失败`) }
            : entry
        )));
      }
    }));

    setLoadingCollections(false);
  }, []);

  // 加载完成后写模块缓存
  useEffect(() => {
    if (!loadingCollections) _sCachedEntries = entries;
  }, [entries, loadingCollections]);

  useEffect(() => {
    void loadCollections();
  }, [loadCollections]);

  // Tab 切换后聚焦第一个集合卡片
  const { autoFocusFirst, onAutoFocused } = props;
  useEffect(() => {
    if (!autoFocusFirst || loadingCollections || entries.length === 0) return;
    const t = setTimeout(() => {
      if (focusCollectionCard(entries[0].mode)) {
        onAutoFocused?.();
      }
    }, 120);
    return () => clearTimeout(t);
  }, [autoFocusFirst, loadingCollections, entries, focusCollectionCard, onAutoFocused]);

  useEffect(() => {
    if (currentPlatformMode !== null) return;
    if (!pendingCollectionFocusModeRef.current) return;
    const mode = pendingCollectionFocusModeRef.current;
    pendingCollectionFocusModeRef.current = null;
    focusCollectionCard(mode);
  }, [currentPlatformMode, entries, focusCollectionCard]);

  const onNavigateCollections = useCallback((mode: EmulatorMode, button: number) => {
    const currentIndex = Math.max(0, entries.findIndex((entry) => entry.mode === mode));
    const currentPosition = getCollectionGridVirtualPosition(currentIndex);
    let nextIndex = -1;

    if (button === 11) {
      let bestColumn = -1;
      entries.forEach((_entry, index) => {
        const candidate = getCollectionGridVirtualPosition(index);
        if (candidate.row !== currentPosition.row || candidate.column >= currentPosition.column) return;
        if (candidate.column > bestColumn) {
          bestColumn = candidate.column;
          nextIndex = index;
        }
      });
      if (nextIndex < 0) return true;
    } else if (button === 12) {
      let bestColumn = Number.POSITIVE_INFINITY;
      entries.forEach((_entry, index) => {
        const candidate = getCollectionGridVirtualPosition(index);
        if (candidate.row !== currentPosition.row || candidate.column <= currentPosition.column) return;
        if (candidate.column < bestColumn) {
          bestColumn = candidate.column;
          nextIndex = index;
        }
      });
      if (nextIndex < 0) return true;
    } else if (button === 9) {
      if (currentPosition.row <= 0) return false;
      let bestDistance = Number.POSITIVE_INFINITY;
      let bestColumn = Number.POSITIVE_INFINITY;
      entries.forEach((_entry, index) => {
        const candidate = getCollectionGridVirtualPosition(index);
        if (candidate.row !== currentPosition.row - 1) return;
        const distance = Math.abs(candidate.column - currentPosition.column);
        if (distance < bestDistance || (distance === bestDistance && candidate.column < bestColumn)) {
          bestDistance = distance;
          bestColumn = candidate.column;
          nextIndex = index;
        }
      });
      if (nextIndex < 0) return true;
    } else if (button === 10) {
      let bestDistance = Number.POSITIVE_INFINITY;
      let bestColumn = Number.POSITIVE_INFINITY;
      entries.forEach((_entry, index) => {
        const candidate = getCollectionGridVirtualPosition(index);
        if (candidate.row !== currentPosition.row + 1) return;
        const distance = Math.abs(candidate.column - currentPosition.column);
        if (distance < bestDistance || (distance === bestDistance && candidate.column < bestColumn)) {
          bestDistance = distance;
          bestColumn = candidate.column;
          nextIndex = index;
        }
      });
      if (nextIndex < 0) return true;
    }

    if (nextIndex < 0 || nextIndex >= entries.length) return false;
    return focusCollectionCard(entries[nextIndex]?.mode || null);
  }, [entries, focusCollectionCard]);

  const openCollection = useCallback((entry: EmulatorCollectionEntry) => {
    pendingCollectionFocusModeRef.current = entry.mode;
    setCurrentPlatformMode(entry.mode);
  }, []);

  const handleBackToCollections = useCallback(() => {
    pendingCollectionFocusModeRef.current = currentPlatformMode;
    setCurrentPlatformMode(null);
  }, [currentPlatformMode]);

  return (
    <div className="freedeck-native-library">
      <style>{EMULATOR_TAB_CSS}</style>
      {currentPlatform ? (
        <PlatformGameGrid
          platform={currentPlatform}
          onBack={handleBackToCollections}
          onOpen={props.onOpen}
        />
      ) : (
        <div className="freedeck-library-grid-stage">
          {loadingCollections && entries.every((entry) => entry.loading) ? (
            <div style={{ display: "flex", justifyContent: "center", padding: "48px 0 72px" }}>
              <SteamSpinner style={{ width: 40, height: 40 }} />
            </div>
          ) : (
            <div className="freedeck-library-collection-stage">
              <div className="freedeck-library-collection-grid">
                {entries.map((entry) => (
                  <CollectionCard
                    key={entry.mode}
                    entry={entry}
                    buttonRef={(node) => {
                      collectionButtonRefs.current[entry.mode] = node;
                    }}
                    onNavigate={onNavigateCollections}
                    onOpen={openCollection}
                  />
                ))}
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
