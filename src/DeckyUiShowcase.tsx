/* ═══════════════════════════════════════════
   DeckyUiShowcase — @decky/ui 组件展示页

   展示所有可用 UI 组件，方便预览和测试。
   无需注入/patcher，仅展示静态组件效果。
   ═══════════════════════════════════════════ */

import {
  PanelSection,
  PanelSectionRow,
  ButtonItem,
  ToggleField,
  Toggle,
  Field,
  Focusable,
  FocusRing,
  SidebarNavigation,
  SliderField,
  TextField,
  Spinner,
  SteamSpinner,
  ProgressBar,
  ProgressBarWithInfo,
  ProgressBarItem,
  Dropdown,
  DropdownItem,
  DialogButton,
  DialogCheckbox,
  ScrollPanel,
  ScrollPanelGroup,
  ControlsList,
  Tabs,
  showModal,
  showContextMenu,
  Menu,
  MenuItem,
} from "@decky/ui";
import { useState } from "react";
import type { CSSProperties } from "react";

/* ── 组件展示区 ──────────────────────── */

function Section({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <PanelSection title={title}>
      <PanelSectionRow>
        <div style={{ display: "flex", flexDirection: "column", gap: 8, width: "100%" }}>
          {children}
        </div>
      </PanelSectionRow>
    </PanelSection>
  );
}

/* ═══════════════════════════════════════════
   PanelSection / PanelSectionRow
   ═══════════════════════════════════════════ */

function PanelDemo() {
  return (
    <PanelSection title="PanelSection 示例">
      <PanelSectionRow>
        <div style={{ padding: "8px 0", fontSize: 14 }}>PanelSectionRow 内容行</div>
      </PanelSectionRow>
      <PanelSectionRow>
        <div style={{ padding: "8px 0", fontSize: 14 }}>PanelSectionRow 第二行</div>
      </PanelSectionRow>
    </PanelSection>
  );
}

/* ═══════════════════════════════════════════
   ButtonItem / DialogButton
   ═══════════════════════════════════════════ */

function ButtonDemo() {
  const [count, setCount] = useState(0);
  return (
    <Section title="ButtonItem / DialogButton">
      <ButtonItem onClick={() => setCount(c => c + 1)}>
        点击计数: {count}
      </ButtonItem>
      <ButtonItem disabled>禁用的 ButtonItem</ButtonItem>
      <div style={{ display: "flex", gap: 8, marginTop: 4 }}>
        <DialogButton onClick={() => {}}>DialogButton</DialogButton>
        <DialogButton disabled onClick={() => {}}>禁用</DialogButton>
      </div>
    </Section>
  );
}

/* ═══════════════════════════════════════════
   ToggleField / Toggle
   ═══════════════════════════════════════════ */

function ToggleDemo() {
  const [checked1, setChecked1] = useState(false);
  const [checked2, setChecked2] = useState(true);
  return (
    <Section title="ToggleField / Toggle">
      <ToggleField
        label="ToggleField 开关"
        description={checked1 ? "已开启" : "已关闭"}
        checked={checked1}
        onChange={setChecked1}
      />
      <ToggleField label="禁用的 ToggleField" checked={false} onChange={() => {}} disabled />
      <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
        <span style={{ fontSize: 14 }}>纯 Toggle:</span>
        <Toggle value={checked2} onChange={setChecked2} />
        <span style={{ fontSize: 12, color: "#8b929a" }}>{checked2 ? "ON" : "OFF"}</span>
      </div>
    </Section>
  );
}

/* ═══════════════════════════════════════════
   Field / Focusable / FocusRing
   ═══════════════════════════════════════════ */

function FieldDemo() {
  const [focusCount, setFocusCount] = useState(0);
  return (
    <Section title="Field / Focusable / FocusRing">
      <Field label="Field 标签" description="Field 的 description 文本">
        <Focusable style={{ display: "flex", padding: 4 }}>
          <span style={{ fontSize: 14 }}>Focusable 内容</span>
        </Focusable>
      </Field>
      <Field label="可聚焦区域" highlightOnFocus={true}>
        <Focusable
          onActivate={() => setFocusCount(c => c + 1)}
          onOKActionDescription="激活"
          style={{ padding: 8, background: "rgba(255,255,255,0.05)", borderRadius: 4 }}
        >
          <span>点击 OK 触发 ({focusCount} 次)</span>
        </Focusable>
      </Field>
      <FocusRing>
        <div style={{ padding: 4 }}>FocusRing 包裹的内容</div>
      </FocusRing>
    </Section>
  );
}

/* ═══════════════════════════════════════════
   SliderField
   ═══════════════════════════════════════════ */

function SliderDemo() {
  const [sliderVal, setSliderVal] = useState(50);
  const [slider2, setSlider2] = useState(30);
  return (
    <Section title="SliderField">
      <SliderField
        label="音量"
        value={sliderVal}
        min={0}
        max={100}
        step={5}
        showValue
        valueSuffix="%"
        onChange={setSliderVal}
      />
      <SliderField
        label="亮度 (禁用)"
        value={slider2}
        min={0}
        max={100}
        disabled
        onChange={setSlider2}
      />
    </Section>
  );
}

/* ═══════════════════════════════════════════
   TextField
   ═══════════════════════════════════════════ */

function TextFieldDemo() {
  const [text, setText] = useState("");
  return (
    <Section title="TextField">
      <TextField label="输入框" description="请输入文本" value={text} onChange={e => setText(e.target.value)} />
      <TextField label="密码框" bIsPassword description="输入密码" value="" onChange={() => {}} />
      <TextField label="数字框" mustBeNumeric description="只能输入数字" value="123" onChange={() => {}} />
      <TextField label="禁用" value="不可编辑" onChange={() => {}} disabled />
    </Section>
  );
}

/* ═══════════════════════════════════════════
   Spinner / SteamSpinner
   ═══════════════════════════════════════════ */

function SpinnerDemo() {
  return (
    <Section title="Spinner / SteamSpinner">
      <div style={{ display: "flex", alignItems: "center", gap: 16 }}>
        <Spinner style={{ width: 32, height: 32 }} />
        <span style={{ fontSize: 13, color: "#8b929a" }}>SVG 旋转加载</span>
      </div>
      <div style={{ position: "relative", height: 80, background: "rgba(0,0,0,0.3)", borderRadius: 8, overflow: "hidden" }}>
        <SteamSpinner background="transparent" />
      </div>
    </Section>
  );
}

/* ═══════════════════════════════════════════
   ProgressBar
   ═══════════════════════════════════════════ */

function ProgressDemo() {
  return (
    <Section title="ProgressBar">
      <ProgressBar nProgress={0} />
      <ProgressBar nProgress={45} />
      <ProgressBar nProgress={100} />
      <ProgressBar indeterminate />
      <div style={{ marginTop: 4 }}>
        <ProgressBarWithInfo
          nProgress={67}
          sOperationText="正在下载..."
          sTimeRemaining="剩余 2 分钟"
        />
      </div>
    </Section>
  );
}

/* ═══════════════════════════════════════════
   Dropdown
   ═══════════════════════════════════════════ */

function DropdownDemo() {
  const [selected, setSelected] = useState("option1");
  return (
    <Section title="Dropdown / DropdownItem">
      <Dropdown
        rgOptions={[
          { data: "option1", label: "选项一" },
          { data: "option2", label: "选项二" },
          { data: "option3", label: "选项三" },
        ]}
        selectedOption={selected}
        onChange={(d: any) => setSelected(d.data)}
        strDefaultLabel="请选择..."
      />
      <Dropdown
        rgOptions={[
          { data: "a", label: "禁用示例" },
        ]}
        selectedOption="a"
        onChange={() => {}}
        disabled
      />
    </Section>
  );
}

/* ═══════════════════════════════════════════
   DialogCheckbox
   ═══════════════════════════════════════════ */

function CheckboxDemo() {
  const [cb, setCb] = useState(false);
  return (
    <Section title="DialogCheckbox">
      <DialogCheckbox
        label="同意用户协议"
        description="请仔细阅读协议内容"
        checked={cb}
        onChange={setCb}
      />
      <DialogCheckbox
        label="禁用复选框"
        checked={true}
        onChange={() => {}}
        disabled
      />
    </Section>
  );
}

/* ═══════════════════════════════════════════
   Tabs
   ═══════════════════════════════════════════ */

function TabsDemo() {
  const [activeTab, setActiveTab] = useState("tab1");
  return (
    <Section title="Tabs">
      <Tabs
        activeTab={activeTab}
        onShowTab={setActiveTab}
        tabs={[
          { id: "tab1", title: "标签一", content: <div style={{ padding: 12, fontSize: 14 }}>标签一 内容</div> },
          { id: "tab2", title: "标签二", content: <div style={{ padding: 12, fontSize: 14 }}>标签二 内容</div> },
          { id: "tab3", title: "数量+", content: <div style={{ padding: 12, fontSize: 14 }}>标签三 内容带附加信息</div>, renderTabAddon: () => <span style={{ fontSize: 10, color: "#6fb7ff" }}>NEW</span> },
        ]}
      />
    </Section>
  );
}

/* ═══════════════════════════════════════════
   ControlsList
   ═══════════════════════════════════════════ */

function ControlsListDemo() {
  return (
    <Section title="ControlsList">
      <ControlsList alignItems="left" spacing="standard">
        <DialogButton onClick={() => {}} onOKActionDescription="确定">确定</DialogButton>
        <DialogButton onClick={() => {}} onCancelActionDescription="取消">取消</DialogButton>
        <DialogButton onClick={() => {}} onSecondaryActionDescription="更多">更多</DialogButton>
      </ControlsList>
    </Section>
  );
}

/* ═══════════════════════════════════════════
   ScrollPanel
   ═══════════════════════════════════════════ */

function ScrollPanelDemo() {
  return (
    <Section title="ScrollPanel / ScrollPanelGroup">
      <div style={{ height: 100, border: "1px solid rgba(255,255,255,0.1)", borderRadius: 8 }}>
        <ScrollPanel>
          <ScrollPanelGroup>
            {Array.from({ length: 10 }, (_, i) => (
              <div key={i} style={{ padding: "6px 12px", fontSize: 13 }}>
                滚动项 {i + 1}
              </div>
            ))}
          </ScrollPanelGroup>
        </ScrollPanel>
      </div>
    </Section>
  );
}

/* ═══════════════════════════════════════════
   Modal / Menu
   ═══════════════════════════════════════════ */

function ModalMenuDemo() {
  const showDemoModal = () => {
    showModal(
      <div style={{ padding: 24 }}>
        <h3 style={{ margin: "0 0 12px" }}>测试弹窗</h3>
        <p style={{ fontSize: 14, color: "#8b929a" }}>这是一个 showModal 弹窗示例</p>
      </div>,
      undefined,
      { strTitle: "弹窗标题" }
    );
  };

  const showDemoMenu = (e: MouseEvent) => {
    showContextMenu(
      <Menu label="测试菜单">
        <MenuItem onSelected={() => {}}>菜单项 1</MenuItem>
        <MenuItem onSelected={() => {}}>菜单项 2</MenuItem>
        <MenuItem onSelected={() => {}} disabled>禁用项</MenuItem>
        <MenuItem onSelected={() => {}} tone="destructive">删除</MenuItem>
      </Menu>,
      e.target as EventTarget
    );
  };

  return (
    <Section title="Modal / ContextMenu">
      <ButtonItem onClick={showDemoModal}>打开弹窗 (showModal)</ButtonItem>
      <ButtonItem onClick={(e: any) => showDemoMenu(e)}>打开右键菜单 (showContextMenu)</ButtonItem>
    </Section>
  );
}

/* ═══════════════════════════════════════════
   SidebarNavigation 展示
   ═══════════════════════════════════════════ */

function SidebarDemo() {
  return (
    <Section title="SidebarNavigation">
      <div style={{ height: 300, border: "1px solid rgba(255,255,255,0.1)", borderRadius: 8, overflow: "hidden" }}>
        <SidebarNavigation
          title="侧边栏"
          pages={[
            { title: "页面 A", content: <div style={{ padding: 16, fontSize: 14 }}>页面 A 的内容</div> },
            { title: "页面 B", content: <div style={{ padding: 16, fontSize: 14 }}>页面 B 的内容</div> },
            'separator',
            { title: "页面 C", content: <div style={{ padding: 16, fontSize: 14 }}>页面 C 的内容</div> },
          ]}
        />
      </div>
    </Section>
  );
}

/* ═══════════════════════════════════════════
   主页面：所有组件一览
   ═══════════════════════════════════════════ */

export default function DeckyUiShowcase() {
  return (
    <div style={{
      height: "100%",
      maxHeight: "calc(100vh - 40px)",
      boxSizing: "border-box",
      overflowY: "auto",
      padding: "0 0 80px 0",
    }}>
      <h2 style={{ margin: "16px 0 8px", padding: "0 16px" }}>@decky/ui 组件展示</h2>

      <PanelDemo />
      <ButtonDemo />
      <ToggleDemo />
      <FieldDemo />
      <SliderDemo />
      <TextFieldDemo />
      <SpinnerDemo />
      <ProgressDemo />
      <DropdownDemo />
      <CheckboxDemo />
      <TabsDemo />
      <ModalMenuDemo />
      <ControlsListDemo />
      <ScrollPanelDemo />
      <SidebarDemo />
    </div>
  );
}
