/* ═══════════════════════════════════════════
   NewFreedeck — 游戏库 (Tab 缓存 + 分页加载)
   ═══════════════════════════════════════════ */

import { Tabs, SteamSpinner, Focusable, TextField } from "@decky/ui";
import { useState, useEffect, useMemo, useRef, useCallback } from "react";
import { getShelfGames, getPlatformGames, getCuratedShelf, listCatalog } from "./api";
import { ShelfView } from "./library/ShelfView";
import { GameCard } from "./library/GameCard";
import { InstallOverlay } from "./library/InstallOverlay";
import { EmulatorTab } from "./EmulatorTab";
import { CarouselBanner } from "./library/CarouselBanner";
import type { CarouselBannerItem } from "./library/CarouselBanner";
import { focusNavigableElement, focusNavigableElementWithRetry } from "./library/focus";
import { LIBRARY_CSS } from "./styles";
import type { CatalogItem, DownloadTask } from "./api";
import { upsertDownload } from "./downloadStore";
import { getDownloadPoller } from "./downloadPoller";


const TAB_ORDER = ["home", "pc", "emulator", "pool", "later"];
const TAB_META: Record<string, string> = {
  home: "首页", pc: "PC游戏", emulator: "模拟器游戏", pool: "资源池", later: "稍后再玩",
};

/* ═══════════════════════════════════════════
   LibraryPage
   ═══════════════════════════════════════════ */

export default function LibraryPage() {
  const [tab, setTab] = useState("home");
  const [games, setGames] = useState<CatalogItem[]>([]);
  const [loading, setLoading] = useState(true);
  const [loadingMore, setLoadingMore] = useState(false);
  const [page, setPage] = useState(1);
  const [hasMore, setHasMore] = useState(false);
  const cacheRef = useRef<Map<string, CatalogItem[]>>(new Map());

  const [selectedGame, setSelectedGame] = useState<CatalogItem | null>(null);
  const [sourceRect, setSourceRect] = useState<DOMRect | null>(null);

  const lastFocusedCardRef = useRef<HTMLElement | null>(null);

  const handleOpen = useCallback((item: CatalogItem, el: HTMLElement) => {
    // 保存当前聚焦的卡片，关闭安装页后恢复
    lastFocusedCardRef.current = el.closest(".freedeck-library-grid-card") as HTMLElement | null;
    setSourceRect(el.getBoundingClientRect()); setSelectedGame(item);
  }, []);
  const handleClose = useCallback(() => {
    setSelectedGame(null); setSourceRect(null);
    // 恢复焦点到之前选中的卡片
    const restoreEl = lastFocusedCardRef.current;
    if (restoreEl) {
      lastFocusedCardRef.current = null;
      setTimeout(() => focusNavigableElement(restoreEl), 80);
    }
  }, []);

  const handleStartDownload = useCallback(async (task: DownloadTask, coverUrl: string) => {
    setSelectedGame(null); setSourceRect(null);
    const initial = { ...task, cover_url: coverUrl } as any;
    upsertDownload(initial, coverUrl);
    getDownloadPoller().addTask(task.task_id, coverUrl);
  }, []);

  const handleCancelDownload = useCallback(async (taskId: string) => {
    await getDownloadPoller().cancelTask(taskId);
  }, []);

  // 页面挂载时恢复进行中的下载轮询
  useEffect(() => {
    getDownloadPoller().resumeAll();
  }, []);

  const PAGE_SIZE = 25;

  const loadHome = useCallback(async () => {
    const cached = cacheRef.current.get("home");
    if (cached && cached.length > 0) { setGames(cached); setPage(1); setLoading(false); return; }
    setLoading(true);
    try {
      const r = await getShelfGames({ limit: 24 });
      if (r?.status === "success" && r.data?.games?.length) {
        cacheRef.current.set("home", r.data.games as CatalogItem[]);
        setGames(r.data.games as CatalogItem[]);
      }
    } catch {}
    setLoading(false);
  }, []);

  const loadPcFirst = useCallback(async () => {
    const cached = cacheRef.current.get("pc");
    if (cached && cached.length > 0) { setGames(cached); setHasMore(true); setPage(1); setLoading(false); return; }

    setLoading(true);
    try {
      const r = await getPlatformGames({ platform: "pc", page: 1, page_size: PAGE_SIZE });
      if (r?.status === "success" && r.data?.games?.length) {
        cacheRef.current.set("pc", r.data.games as CatalogItem[]);
        setGames(r.data.games as CatalogItem[]);
        setHasMore(r.data.has_more ?? false);
        setPage(1);
      }
    } catch {}
    setLoading(false);
  }, []);

  const loadPcMore = useCallback(async () => {
    if (loadingMore || !hasMore) return;
    setLoadingMore(true);
    const next = page + 1;
    try {
      const r = await getPlatformGames({ platform: "pc", page: next, page_size: PAGE_SIZE });
      if (r?.status === "success" && r.data?.games?.length) {
        setGames(prev => {
          const merged = [...prev, ...(r.data!.games as CatalogItem[])];
          cacheRef.current.set("pc", merged);
          return merged;
        });
        setHasMore(r.data.has_more ?? false);
        setPage(next);
      }
    } catch {}
    setLoadingMore(false);
  }, [page, hasMore, loadingMore]);

  // ── Tab 切换 ──
  useEffect(() => {
    if (tab === "home") loadHome();
    else if (tab === "pc") loadPcFirst();
    else setLoading(false);
  }, [tab, loadHome, loadPcFirst]);

  // 后台预热
  useEffect(() => {
    loadHome();
    setTimeout(() => {
      getPlatformGames({ platform: "pc", page: 1, page_size: PAGE_SIZE }).then(r => {
        if (r?.status === "success" && r.data?.games?.length)
          cacheRef.current.set("pc", r.data.games as CatalogItem[]);
      }).catch(() => {});
    }, 2000);
  }, []); // eslint-disable-line

  const spinner = <div style={{ display:"flex",justifyContent:"center",padding:60 }}><SteamSpinner style={{width:40,height:40}} /></div>;
  const empty = <div style={{ display:"flex",justifyContent:"center",padding:60,color:"#8b929a",fontSize:13 }}>敬请期待</div>;
  const showSpinner = loading && games.length === 0;

  // Tab 切换后聚焦第一个内容元素
  const tabFocusIndexRef = useRef<number>(0);
  const handleShowTab = useCallback((id: string) => {
    if (tab !== id) tabFocusIndexRef.current = 0; // 切 tab 时重置
    setTab(id);
  }, [tab]);

  const tabs = TAB_ORDER.map(id => ({
    id, title: TAB_META[id],
    content: showSpinner ? spinner
      : id === "home" ? <HomeTab games={games} onOpen={handleOpen} initialFocus={tabFocusIndexRef.current === 0} onInitialFocused={() => { tabFocusIndexRef.current = 1; }} />
      : id === "pc"   ? <GridTab games={games} hasMore={hasMore} loadingMore={loadingMore} onLoadMore={loadPcMore} onOpen={handleOpen} autoFocusFirst={tabFocusIndexRef.current === 0} onAutoFocused={() => { tabFocusIndexRef.current = 1; }} />
      : id === "emulator" ? <EmulatorTab onOpen={handleOpen} autoFocusFirst={tabFocusIndexRef.current === 0} onAutoFocused={() => { tabFocusIndexRef.current = 1; }} />
      : id === "pool" ? <SearchPool onOpen={handleOpen} /> : empty,
  }));

  return (
    <div className="nf-library-root" style={{
      width: "100%", height: "100vh", display: "flex", flexDirection: "column",
      background: "#0e141b", color: "#cac4d0",
      paddingTop: 40, boxSizing: "border-box",
    }}>
      <style>{LIBRARY_CSS}{NF_CSS}</style>

      <Tabs tabs={tabs} activeTab={tab} onShowTab={handleShowTab} autoFocusContents={false} />

      {loading && games.length > 0 && (
        <div style={{ position:"fixed",bottom:0,left:0,right:0,height:2,zIndex:100,
          background:"linear-gradient(90deg,transparent,#369ff4,transparent)",
          animation:"nf-loading-bar 1.2s ease-in-out infinite" }} />
      )}

      {selectedGame && sourceRect && (
        <InstallOverlay game={selectedGame} sourceRect={sourceRect} onClose={handleClose}
          onStartDownload={handleStartDownload} />
      )}

      {/* 书架区 */}
    </div>
  );
}

/* ═══════════════════════════════════════════
   HomeTab
   ═══════════════════════════════════════════ */

// 模拟器平台列表（用于首页书架）
const EMU_SHELVES = [
  { id: "gba", title: "GBA 精选" },
  { id: "ps1", title: "PS1 精选" },
  { id: "ps2", title: "PS2 精选" },
  { id: "psp", title: "PSP 精选" },
  { id: "nds", title: "NDS 精选" },
  { id: "3ds", title: "3DS 精选" },
  { id: "fc",  title: "FC 精选" },
  { id: "md",  title: "MD 精选" },
];

type ShelfRow = { title: string; items: CatalogItem[]; cardRefs: Map<number, HTMLDivElement> };

/* ── 搜索组件 ────────────────────────── */

const SEARCH_PLATFORMS = [
  { id: "all", label: "全部" },
  { id: "pc", label: "PC" },
  { id: "ps1", label: "PS1" },
  { id: "ps2", label: "PS2" },
  { id: "ps3", label: "PS3" },
  { id: "psp", label: "PSP" },
  { id: "switch", label: "Switch" },
  { id: "3ds", label: "3DS" },
  { id: "gba", label: "GBA" },
  { id: "nds", label: "NDS" },
  { id: "fc", label: "FC" },
  { id: "md", label: "MD" },
];

function SearchPool({ onOpen }: { onOpen: (item: CatalogItem, el: HTMLElement) => void }) {
  const [query, setQuery] = useState("");
  const [platform, setPlatform] = useState("all");
  const [results, setResults] = useState<CatalogItem[]>([]);
  const [loading, setLoading] = useState(false);
  const searchTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const searchRequestRef = useRef(0);
  const resultsContainerRef = useRef<HTMLDivElement>(null);
  const cardRefs = useRef<Map<number, HTMLDivElement>>(new Map());
  const columns = 4;
  const cardWidth = 133 + 12;

  // 执行搜索
  const doSearch = useCallback(async (q: string, plat: string) => {
    const requestId = ++searchRequestRef.current;
    if (!q.trim()) {
      setResults([]);
      setLoading(false);
      return;
    }
    setLoading(true);
    try {
      const r = await listCatalog({
        query: q.trim(),
        platform: plat === "all" ? "" : plat,
        page: 1,
        page_size: 50,
      });
      if (requestId !== searchRequestRef.current) return;
      setResults(
        r?.status === "success" && Array.isArray(r.data?.items)
          ? r.data.items as CatalogItem[]
          : [],
      );
    } catch {
      if (requestId === searchRequestRef.current) setResults([]);
    } finally {
      if (requestId === searchRequestRef.current) setLoading(false);
    }
  }, []);

  // 输入防抖
  const onInput = useCallback((val: string) => {
    setQuery(val);
    searchRequestRef.current += 1;
    if (searchTimerRef.current) clearTimeout(searchTimerRef.current);
    if (!val.trim()) {
      setResults([]);
      setLoading(false);
      return;
    }
    searchTimerRef.current = setTimeout(() => doSearch(val, platform), 400);
  }, [platform, doSearch]);

  // 平台切换重新搜索
  useEffect(() => {
    if (searchTimerRef.current) {
      clearTimeout(searchTimerRef.current);
      searchTimerRef.current = null;
    }
    void doSearch(query, platform);
  }, [platform]); // eslint-disable-line react-hooks/exhaustive-deps

  useEffect(() => () => {
    searchRequestRef.current += 1;
    if (searchTimerRef.current) clearTimeout(searchTimerRef.current);
  }, []);

  const focusCard = useCallback((index: number) => {
    return focusNavigableElementWithRetry(() => {
      const wrapper = cardRefs.current.get(index);
      return wrapper?.querySelector(".freedeck-library-grid-card-button") as HTMLElement | null;
    });
  }, []);

  const onNavigate = useCallback((index: number, button: number) => {
    const total = results.length;
    const rowStart = Math.floor(index / columns) * columns;
    const rowEnd = Math.min(rowStart + columns - 1, total - 1);

    if (button === 11) {
      return index > rowStart ? focusCard(index - 1) : true;
    }
    if (button === 12) {
      return index < rowEnd ? focusCard(index + 1) : true;
    }
    if (button === 9) {
      const target = index - columns;
      return target >= 0 ? focusCard(target) : false;
    }
    if (button === 10) {
      const target = index + columns;
      return target < total ? focusCard(target) : true;
    }
    return false;
  }, [focusCard, results.length]);

  const onCardOpen = useCallback((item: CatalogItem) => {
    for (const [index, element] of cardRefs.current) {
      if (results[index]?.game_id === item.game_id) {
        onOpen(item, element);
        return;
      }
    }
  }, [onOpen, results]);

  return (
    <div style={{ height: "100%", display: "flex", flexDirection: "column", overflow: "hidden" }}>
      {/* 搜索栏 — TextField */}
      <div style={{ padding: "12px 0", flexShrink: 0 }}>
        <TextField
          label=""
          description="输入游戏名称搜索"
          value={query}
          onChange={(e: any) => onInput(e?.target?.value || "")}
          bShowClearAction
          bAlwaysShowClearAction
        />
      </div>

      {/* 平台 Tab 栏 — Focusable + 高亮 */}
      <div style={{
        display: "flex", gap: 4, padding: "0 0 10px", flexShrink: 0, overflowX: "auto",
      }}>
        {SEARCH_PLATFORMS.map(p => (
          <Focusable
            key={p.id}
            onOKButton={() => setPlatform(p.id)}
            onOKActionDescription={p.label}
            focusClassName={platform === p.id ? "nf-search-platform-active" : ""}
            style={{
              padding: "4px 12px", borderRadius: 16, cursor: "pointer",
              fontSize: 12, fontWeight: platform === p.id ? 700 : 400,
              background: platform === p.id ? "rgba(54,159,244,0.2)" : "rgba(255,255,255,0.04)",
              color: platform === p.id ? "#369ff4" : "#8b929a",
              border: platform === p.id ? "1px solid rgba(54,159,244,0.3)" : "1px solid rgba(255,255,255,0.06)",
              whiteSpace: "nowrap",
            } as any}
          >{p.label}
          </Focusable>
        ))}
      </div>

      {/* 结果列表 */}
      <div ref={resultsContainerRef} style={{ flex: 1, overflowY: "auto", paddingBottom: 40 }}>
        {loading ? (
          <div style={{ display: "flex", justifyContent: "center", padding: 40 }}>
            <SteamSpinner style={{ width: 32, height: 32 }} />
          </div>
        ) : !query.trim() ? (
          <div style={{ textAlign: "center", padding: 60, color: "#8b929a", fontSize: 13 }}>
            输入游戏名称开始搜索
          </div>
        ) : results.length === 0 ? (
          <div style={{ textAlign: "center", padding: 60, color: "#8b929a", fontSize: 13 }}>
            未找到匹配的游戏
          </div>
        ) : (
          <div style={{
            display: "grid",
            gridTemplateColumns: `repeat(${columns}, ${cardWidth}px)`,
            justifyContent: "space-between",
            gap: "12px 0",
          }}>
            {results.map((item, i) => (
              <GameCard
                key={item.game_id || i}
                item={item}
                gridIndex={i}
                columns={columns}
                priority={i < 8}
                onNavigate={onNavigate}
                onOpen={onCardOpen}
                cardRefs={cardRefs}
              />
            ))}
          </div>
        )}
      </div>
    </div>
  );
}

function HomeTab({ games, onOpen, initialFocus, onInitialFocused }: {
  games: CatalogItem[];
  onOpen: (item: CatalogItem, el: HTMLElement) => void;
  initialFocus: boolean;
  onInitialFocused: () => void;
}) {
  const [emuShelves, setEmuShelves] = useState<{ platform: string; title: string; items: CatalogItem[] }[]>([]);
  const shuffled = useMemo(() => [...games].sort(() => Math.random() - 0.5), [games]);
  const shelfRegistry = useRef<Map<number, ShelfRow>>(new Map());

  const allShelves = useMemo(() => {
    const list: { id: number; title: string; items: CatalogItem[] }[] = [
      { id: 0, title: "智能推荐", items: shuffled },
    ];
    emuShelves.forEach((s, i) => {
      list.push({ id: i + 1, title: s.title, items: s.items });
    });
    return list;
  }, [shuffled, emuShelves]);

  const focusAt = useCallback((rowId: number, col: number) => {
    const row = shelfRegistry.current.get(rowId);
    if (!row) return false;
    const safe = Math.max(0, Math.min(col, row.items.length - 1));
    const wrapper = row.cardRefs.get(safe);
    if (!wrapper) return false;
    return focusNavigableElementWithRetry(
      () => wrapper.querySelector(".freedeck-library-grid-card-button") as HTMLElement | null,
    );
  }, []);

  // 首次加载自动聚焦第一行第一张卡片
  useEffect(() => {
    if (!initialFocus) return;
    const t = setTimeout(() => {
      focusAt(0, 0);
      onInitialFocused();
    }, 100);
    return () => clearTimeout(t);
  }, [focusAt, initialFocus, onInitialFocused]);

  const makeOnNavigate = useCallback((rowId: number, count: number) => {
    return (idx: number, btn: number) => {
      if (btn === 11) { // LEFT: 行内左移，行首则跳到上一行末尾
        if (idx > 0) return focusAt(rowId, idx - 1);
        if (rowId > 0) {
          const prevCount = shelfRegistry.current.get(rowId - 1)?.items.length ?? 0;
          if (prevCount > 0) return focusAt(rowId - 1, prevCount - 1);
        }
        return false;
      }
      if (btn === 12) { // RIGHT: 行内右移，行尾则跳到下一行开头
        if (idx + 1 < count) return focusAt(rowId, idx + 1);
        const nextRow = shelfRegistry.current.get(rowId + 1);
        if (nextRow && nextRow.items.length > 0) return focusAt(rowId + 1, 0);
        return false;
      }
      if (btn === 9) { // UP
        if (rowId === 0) return false;
        const targetItems = shelfRegistry.current.get(rowId - 1)?.items.length ?? 0;
        if (targetItems === 0) return false;
        return focusAt(rowId - 1, Math.min(idx, targetItems - 1));
      }
      if (btn === 10) { // DOWN
        const target = shelfRegistry.current.get(rowId + 1);
        if (!target || target.items.length === 0) return false;
        return focusAt(rowId + 1, Math.min(idx, target.items.length - 1));
      }
      return false;
    };
  }, [focusAt]);

  const makeOnCardOpen = useCallback((rowId: number) => {
    return (item: CatalogItem) => {
      const row = shelfRegistry.current.get(rowId);
      if (!row) return;
      for (const [idx, wrapper] of row.cardRefs) {
        if (row.items[idx]?.game_id === item.game_id) { onOpen(item, wrapper); return; }
      }
    };
  }, [onOpen]);

  useEffect(() => {
    let alive = true;
    const results: typeof emuShelves = [];
    const fetchBatch = async (batch: typeof EMU_SHELVES) => {
      await Promise.all(batch.map(async ({ id, title }) => {
        try {
          const r = await getCuratedShelf({ platform: id, limit: 20 });
          if (r?.status === "success" && r.data?.games?.length && alive) {
            results.push({ platform: id, title, items: r.data.games as CatalogItem[] });
            setEmuShelves([...results]);
          }
        } catch {}
      }));
    };
    (async () => {
      await fetchBatch(EMU_SHELVES.slice(0, 3)); if (!alive) return;
      await new Promise(r => setTimeout(r, 200));
      await fetchBatch(EMU_SHELVES.slice(3, 6)); if (!alive) return;
      await new Promise(r => setTimeout(r, 200));
      await fetchBatch(EMU_SHELVES.slice(6));
    })();
    return () => { alive = false; };
  }, []);

  return (
    <div style={{ paddingBottom: 40 }}>

      {allShelves.map(shelf => (
        <HomeShelfRow
          key={shelf.id} rowId={shelf.id} title={shelf.title} items={shelf.items}
          registry={shelfRegistry}
          onNavigate={makeOnNavigate(shelf.id, shelf.items.length)}
          onCardOpen={makeOnCardOpen(shelf.id)}
        />
      ))}
    </div>
  );
}

/* ── 单个书架行（注册到全局 registry） ── */
function HomeShelfRow({ rowId, title, items, registry, onNavigate, onCardOpen }: {
  rowId: number; title: string; items: CatalogItem[];
  registry: React.MutableRefObject<Map<number, ShelfRow>>;
  onNavigate: (idx: number, btn: number) => boolean;
  onCardOpen: (item: CatalogItem) => void;
}) {
  const cardRefs = useRef<Map<number, HTMLDivElement>>(new Map());

  // 注册/更新到全局 registry
  useEffect(() => {
    registry.current.set(rowId, { title, items, cardRefs: cardRefs.current });
    return () => { registry.current.delete(rowId); };
  }, [rowId, title, items, registry]);

  return (
    <ShelfView title={title} items={items}
      cardRefs={cardRefs} onNavigate={onNavigate} onOpen={onCardOpen} />
  );
}

/* ═══════════════════════════════════════════
   GridTab — PC 宫格 + 加载更多
   ═══════════════════════════════════════════ */

const GRID_COLS = 5;
const GRID_CARD_W = 133;
const GRID_GAP = 33;

function GridTab({ games, hasMore, loadingMore, onLoadMore, onOpen, autoFocusFirst, onAutoFocused }: {
  games: CatalogItem[]; hasMore: boolean; loadingMore: boolean;
  onLoadMore: () => void; onOpen: (item: CatalogItem, el: HTMLElement) => void;
  autoFocusFirst: boolean; onAutoFocused: () => void;
}) {
  const cardRefs = useRef<Map<number, HTMLDivElement>>(new Map());
  const lastFocusedCardIndexRef = useRef(0);
  const pendingRestoreIndexRef = useRef<number | null>(null);
  const gameCount = games.length;
  const LOAD_MORE_IDX = -1;                   // 固定哨兵索引，不受 games 增长影响
  const loadMoreActive = hasMore && !loadingMore;
  const total = gameCount + (loadMoreActive ? 1 : 0);

  // 清理超出范围的卡片 ref（games 增长后旧哨兵已无用）
  useEffect(() => {
    const map = cardRefs.current;
    for (const key of map.keys()) {
      if (key !== LOAD_MORE_IDX && key >= gameCount) map.delete(key);
    }
  }, [gameCount]);

  // 聚焦辅助：rAF 延迟确保 Decky 内部焦点释放后再设置
  const focusEl = useCallback((idx: number) => {
    return focusNavigableElementWithRetry(() => {
      const wrapper = cardRefs.current.get(idx);
      return wrapper?.querySelector(".freedeck-library-grid-card-button") as HTMLElement | null;
    });
  }, []);

  const handleCardFocus = useCallback((idx: number) => {
    lastFocusedCardIndexRef.current = idx;
  }, []);

  const restoreFocusedCard = useCallback((idx: number | null) => {
    if (idx == null || gameCount <= 0) return false;
    const safe = Math.max(0, Math.min(idx, gameCount - 1));
    lastFocusedCardIndexRef.current = safe;
    return focusEl(safe);
  }, [focusEl, gameCount]);

  // Tab 切换后聚焦第一个游戏卡片
  useEffect(() => {
    if (!autoFocusFirst || gameCount === 0 || loadingMore) return;
    const t = setTimeout(() => {
      if (focusEl(0)) onAutoFocused();
    }, 100);
    return () => clearTimeout(t);
  }, [autoFocusFirst, gameCount, loadingMore, focusEl, onAutoFocused]);

  const handleLoadMore = useCallback(() => {
    if (loadingMore || !hasMore) return;
    // 加载后焦点跳到第一张新卡片
    pendingRestoreIndexRef.current = gameCount;
    onLoadMore();
  }, [gameCount, hasMore, loadingMore, onLoadMore]);

  useEffect(() => {
    if (loadingMore) return;
    if (pendingRestoreIndexRef.current == null) return;
    const restoreIndex = pendingRestoreIndexRef.current;
    pendingRestoreIndexRef.current = null;
    restoreFocusedCard(restoreIndex);
  }, [games.length, loadingMore, restoreFocusedCard]);

  // 统一导航（游戏卡片 + 加载更多按钮为同一焦点组）
  const onNavigate = useCallback((idx: number, btn: number) => {
    // ── 当前焦点在「加载更多」按钮上 ──
    if (idx === LOAD_MORE_IDX) {
      if (btn === 9) { // UP → 跳回最后一行最后一个卡片
        const lastCard = gameCount - 1;
        if (lastCard >= 0) { restoreFocusedCard(lastCard); return true; }
        return false;
      }
      // LEFT(11) / RIGHT(12) / DOWN(10): 焦点停留
      return false;
    }

    // ── 当前焦点在游戏卡片上 ──
    const row = Math.floor(idx / GRID_COLS);
    const col = idx % GRID_COLS;
    let next = -1;

    if (btn === 11) { // LEFT
      if (col > 0) next = idx - 1;
    } else if (btn === 12) { // RIGHT
      if (col < GRID_COLS - 1 && idx + 1 < gameCount) next = idx + 1;
    } else if (btn === 9) { // UP
      if (row > 0) next = idx - GRID_COLS;
    } else if (btn === 10) { // DOWN
      const below = idx + GRID_COLS;
      if (below < gameCount) {
        next = below;
      } else if (loadMoreActive) {
        lastFocusedCardIndexRef.current = idx;
        focusEl(LOAD_MORE_IDX);
        return true;
      }
    }

    if (next >= 0 && next < gameCount) {
      focusEl(next);
      return true;
    }
    return false;
  }, [gameCount, loadMoreActive, focusEl]);

  const onCardOpen = useCallback((item: CatalogItem) => {
    for (const [idx, el] of cardRefs.current)
      if (el && games[idx]?.game_id === item.game_id) { onOpen(item, el); return; }
  }, [games, onOpen]);

  const gridPadding = useMemo(() => {
    const gridW = GRID_COLS * GRID_CARD_W + (GRID_COLS - 1) * GRID_GAP;
    return Math.max(0, (800 - gridW) / 2 - 16);
  }, []);

  if (total === 0) return <div style={{ display:"flex",justifyContent:"center",padding:60,color:"#8b929a",fontSize:13 }}>暂无游戏</div>;

  return (
    <div style={{
      display: "grid",
      gridTemplateColumns: `repeat(${GRID_COLS}, ${GRID_CARD_W}px)`,
      columnGap: GRID_GAP, rowGap: 37, justifyContent: "center",
      padding: `20px ${gridPadding}px 40px`,
    }}>
      {games.map((item, i) => (
        <GameCard key={item.game_id || i} item={item} gridIndex={i}
          columns={GRID_COLS} priority={i < 20}
          onNavigate={onNavigate} onOpen={onCardOpen} cardRefs={cardRefs}
          onFocusIndex={handleCardFocus} />
      ))}
      {hasMore && (
        <div
          ref={(el) => { if (el) cardRefs.current.set(LOAD_MORE_IDX, el); }}
          className="freedeck-library-load-more-card"
          style={{ width: GRID_CARD_W, height: 200 }}
        >
          <Focusable
            className="freedeck-library-grid-card-button"
            onOKButton={loadingMore ? undefined : handleLoadMore}
            onOKActionDescription={loadingMore ? undefined : "加载更多"}
            onGamepadDirection={(e: CustomEvent) => {
              const btn = Number(e?.detail?.button ?? -1);
              // UP → 回到最后一行最后一个卡片
              if (btn === 9) {
                const last = gameCount - 1;
                if (last >= 0) {
                  restoreFocusedCard(last);
                  e.preventDefault?.();
                }
              }
              // LEFT/RIGHT/DOWN: 焦点停留
            }}
            style={{
              width: "100%", height: 200, minHeight: 200, maxHeight: 200,
              padding: 0, borderRadius: 0, border: "none",
              background: "transparent", display: "block",
              pointerEvents: loadingMore ? "none" : "auto",
              opacity: loadingMore ? 0.6 : 1,
        } as any}
          >
            <div className="freedeck-library-grid-card-frame freedeck-library-load-more-card-frame"
              style={{ width: "100%", height: "100%" }}>
              <div className="freedeck-library-load-more-card-inner">
                <div className="freedeck-library-load-more-card-title">
                  {loadingMore ? "加载中..." : "加载更多"}
                </div>
                <div className="freedeck-library-load-more-card-subtitle">
                  {loadingMore ? "请稍候" : "继续获取更多资源"}
                </div>
              </div>
            </div>
          </Focusable>
        </div>
      )}
    </div>
  );
}

const NF_CSS = `
.nf-library-root .Panel.Focusable[class*="tabs"],
.nf-library-root .tabpanel, .nf-library-root [class*="TabContent"],
.nf-library-root [class*="tabcontent"],
.nf-library-root .Panel.Focusable[class*="tabs"] > div,
.nf-library-root .Panel.Focusable[class*="tabs"] > div > div {
  overflow: auto !important; overflow-x: hidden !important; flex: 1 !important;
}
.nf-library-root [class*="tabs"] .Panel.Focusable,
.nf-library-root .tabpanel .Panel.Focusable { overflow: auto !important; }
@keyframes nf-loading-bar {
  0% { opacity:0; transform:translateX(-100%); }
  50% { opacity:1; }
  100% { opacity:0; transform:translateX(100%); }
}
`;
