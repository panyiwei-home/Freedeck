# InstallOverlay "更多"操作按钮 — 技术文档

**日期**: 2026-06-06
**状态**: 前端已实现，后端待接入
**文件**: `src/library/InstallOverlay.tsx`

---

## 架构

```
安装弹窗按钮栏
┌──────────┬──────────┬──────┐
│   安装   │   取消   │ ··· │
│ (flex:1) │ (flex:1) │ 40px │
└──────────┴──────────┴──────┘
                           │
                    点击后 showContextMenu
                           │
                    ┌───────┴────────┐
                    │  Menu 弹窗     │
                    │ ────────────── │
                    │ 加入稍后再玩    │
                    │ 报告可玩性(禁)  │
                    │ 查看详情 (禁)   │
                    └────────────────┘
```

---

## 使用的 @decky/ui 组件

### 1. `showContextMenu` — 弹出上下文菜单

```typescript
import { showContextMenu } from "@decky/ui";

showContextMenu(
  children: ReactNode,          // 菜单内容（Menu + MenuItem）
  parent?: EventTarget,         // 菜单定位锚点（ref.current）
  options?: ContextMenuPositionOptions
): ContextMenuInstance          // { Hide(), Show() }
```

**常用定位选项**:
| 选项 | 说明 |
|------|------|
| `bPreferPopTop` | 菜单优先出现在锚点上方 |
| `bPreferPopLeft` | 菜单优先出现在锚点左侧 |
| `bForcePopup` | 强制在独立窗口打开 |
| `bMatchWidth` | 匹配锚点宽度 |

### 2. `Menu` — 菜单容器

```typescript
import { Menu } from "@decky/ui";

<Menu
  label="游戏操作"     // 菜单标题
  onCancel?() => void   // 关闭回调
  cancelText?: string   // 取消按钮文本
>
  {/* MenuItem 子项 */}
</Menu>
```

### 3. `MenuItem` — 菜单项

```typescript
import { MenuItem } from "@decky/ui";

<MenuItem
  onClick?(evt: Event): void       // 点击回调
  disabled?: boolean               // 禁用状态（灰色不可选）
  tone?: 'positive' | 'emphasis' | 'destructive'  // 色调
  bPlayAudio?: boolean             // 播放音效
  selected?: boolean               // 预选中
>
  菜单项文本
</MenuItem>
```

### 4. `MenuSeparator` — 分隔线

```typescript
import { MenuSeparator } from "@decky/ui";

<MenuSeparator />
```

---

## 当前实现代码

```tsx
// ref
const moreBtnRef = useRef<any>(null);

// 更多按钮
<DialogButton
  ref={moreBtnRef}
  onOKButton={handleMore}
  onOKActionDescription="更多操作"
  onGamepadDirection={(e) => {
    if (e.detail.button === 11) { cancelBtnRef.current?.focus(); }
  }}
  style={{ width: "100%", minWidth: 0, padding: "10px 0" }}
>
  ···
</DialogButton>

// 点击处理
const handleMore = useCallback(() => {
  showContextMenu(
    <>
      <Menu label="游戏操作">
        <MenuItem onClick={() => toaster.toast({ title: "已加入稍后再玩" })}>
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
```

---

## 手柄导航

```
安装 ← → 取消 ← → ···
  ↓             ↓
  Dropdown (版本选择)
```

**方向键**:
- 安装按钮: → 取消 | ↑ Dropdown
- 取消按钮: ← 安装 | → 更多 | ↑ Dropdown
- 更多按钮: ← 取消

---

## 后端接入点（待实现）

### Callable 接口建议

```python
# main.py — 新增 callable

# 加入稍后再玩
async def game_add_to_playlist(self, payload=None, game_id: str = ""):
    """将游戏加入稍后再玩列表"""
    ...

# 报告可玩性
async def game_report_playability(self, payload=None, game_id: str = "", status: str = ""):
    """status: "playable" | "not_playable" | "perfect" """
    ...

# 获取游戏详情
async def game_get_details(self, payload=None, game_id: str = ""):
    """返回游戏详细信息"""
    ...
```

### 前端 API 绑定

```typescript
// src/api.ts
export const gameAddToPlaylist = callable<
  [{ game_id: string }],
  ApiResponse<{ ok: boolean }>
>("game_add_to_playlist");

export const gameReportPlayability = callable<
  [{ game_id: string; status: string }],
  ApiResponse<{ ok: boolean }>
>("game_report_playability");
```

---

## MenuItem 可替换为确认弹窗（ConfirmModal）

对于"报告可玩性"等需要用户选择的操作，可以改为嵌套弹窗：

```tsx
<MenuItem onClick={() => {
  showModal(
    <ConfirmModal
      strTitle="报告可玩性"
      strDescription="该游戏在 Steam Deck 上的运行情况？"
      strOKButtonText="完美运行"
      strCancelButtonText="取消"
      strMiddleButtonText="可玩（有小问题）"
      onOK={() => { /* 完美 */ }}
      onMiddleButton={() => { /* 可玩 */ }}
      onCancel={() => {}}
    />
  );
}}>
  报告可玩性
</MenuItem>
```