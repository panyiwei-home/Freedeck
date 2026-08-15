# 游戏库首页轮播图 — 设计文档

**日期**: 2026-06-06
**状态**: 已确认
**范围**: 在 LibraryPage 首页 Tab 顶部新增 Carousel 轮播图模块

---

## 模块接口

```typescript
// src/library/CarouselBanner.tsx

interface CarouselBannerItem {
  id: string;
  imageUrl: string;         // 轮播背景图
  title: string;            // 标题（显示在轮播项底部）
  subtitle?: string;        // 副标题
  onClick?: () => void;     // A键点击回调
}

interface CarouselBannerProps {
  items: CarouselBannerItem[];   // 轮播项列表（空数组=占位状态）
  height?: number;                // 默认 200
}
```

## 页面结构

```
LibraryPage
├── Tabs (首页/PC/模拟器/资源池/稍后再玩)
├── CarouselBanner ← 只在 home tab 显示
├── HomeTab (书架区，ShelfView × N)
└── InstallOverlay
```

## 手柄导航

```
Tabs 标签栏
    ↓
CarouselBanner (← → 切换轮播项)
    ↓
书架区 ShelfView (↓ → 游戏卡片行)
```

## 使用的 @decky/ui 组件

| 组件 | 用途 |
|------|------|
| `Carousel` | 核心轮播容器 |
| `Focusable` | 包裹每个轮播项，提供手柄导航能力 |
| `FocusRing` | 获焦时显示焦点环 |

## Carousel 关键参数

| 参数 | 值 | 说明 |
|------|-----|------|
| `autoFocus` | `false` | 不抢占初始焦点 |
| `scrollToAlignment` | `"center"` | 选中项居中 |
| `fnDoesItemTakeFocus` | `() => true` | 每项可获焦 |
| `fnGetColumnWidth` | 返回卡片宽度 | 根据容器动态计算 |
| `nHeight` | props.height | 轮播区高度 |
| `nItemHeight` | props.height - 8 | 卡片高度 |
| `nItemMarginX` | 8 | 项间距 |

## 占位状态

`items.length === 0` 时渲染:
- 高度保持 `props.height`
- 灰色背景 `rgba(255,255,255,0.03)`
- 居中文字 "即将推出更多精选内容"
- border: `1px solid rgba(255,255,255,0.06)`
- 不可获焦（不参与手柄导航）

## 后续扩展

- `items` 数据源：后端新增 `getCarouselItems` callable
- A键交互：点击轮播项跳转到对应平台/游戏详情
- 自动轮播：可选的 `autoRotateInterval` prop