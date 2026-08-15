/* ═══════════════════════════════════════════
   QuickAccessEntry — Steam 菜单 QAM 入口

   显示在 Steam Deck 的 Steam 按钮弹出菜单中。
   当设置中的 "显示 Freedeck 游戏库入口" 开关打开时，
   渲染"游戏库"和"设置"两个按钮。
   ═══════════════════════════════════════════ */

import {
  PanelSection,
  PanelSectionRow,
  ButtonItem,
} from "@decky/ui";
import { routerHook } from "@decky/api";
import { LIBRARY_ROUTE, SETTINGS_ROUTE } from "./routes";
import { getSettings } from "./api";

/* ── 导航工具 ──────────────────────────── */

function navigateTo(path: string) {
  // 关闭 QAM
  try {
    (window as any).SteamClient?.UI?.OpenQuickAccessMenu?.();
  } catch {}
  // 导航到目标路由
  routerHook.navigateTo(path);
}

/* ── QAM 入口组件 ──────────────────────── */

export default function QuickAccessEntry() {
  return (
    <PanelSection title="Freedeck">
      <PanelSectionRow>
        <ButtonItem
          onClick={() => navigateTo(LIBRARY_ROUTE)}
          onOKActionDescription="打开游戏库"
        >
          游戏库
        </ButtonItem>
      </PanelSectionRow>
      <PanelSectionRow>
        <ButtonItem
          onClick={() => navigateTo(SETTINGS_ROUTE)}
          onOKActionDescription="打开设置"
        >
          设置
        </ButtonItem>
      </PanelSectionRow>
    </PanelSection>
  );
}
