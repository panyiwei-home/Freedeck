/* ═══════════════════════════════════════════
   NewFreedeck — ShelfView 可复用书架组件
   ═══════════════════════════════════════════ */

import { useRef } from "react";
import { GameCard } from "./GameCard";
import type { CatalogItem } from "../api";

export interface ShelfViewProps {
  title: string;
  items: CatalogItem[];
  cardRefs: React.MutableRefObject<Map<number, HTMLDivElement>>;
  onNavigate: (idx: number, btn: number) => boolean;
  onOpen: (item: CatalogItem) => void;
  gap?: number;
  priorityCount?: number;
}

export function ShelfView({
  title,
  items,
  cardRefs,
  onNavigate,
  onOpen,
  gap = 33,
  priorityCount = 8,
}: ShelfViewProps) {
  const scrollRef = useRef<HTMLDivElement>(null);

  return (
    <div style={{ padding: "12px 0", overflow: "visible" }}>
      <div style={{ padding: "0 16px 8px" }}>
        <h3 style={{ fontSize: 16, fontWeight: 700, color: "#f4f8fc", margin: 0 }}>{title}</h3>
      </div>

      <div
        ref={scrollRef}
        style={{
          display: "flex",
          flexWrap: "nowrap",
          gap,
          overflowX: "auto",
          overflowY: "visible",
          scrollbarWidth: "none",
          padding: "6px 16px 30px",
        }}
      >
        {items.map((item, i) => (
          <GameCard
            key={item.game_id || i}
            item={item}
            gridIndex={i}
            columns={items.length}
            priority={i < priorityCount}
            onNavigate={onNavigate}
            onOpen={onOpen}
            cardRefs={cardRefs}
          />
        ))}
        <div style={{ minWidth: 16, flexShrink: 0 }} />
      </div>
    </div>
  );
}
