/* ═══════════════════════════════════════════
   NewFreedeck — QR 登录页面 (useReducer)
   ═══════════════════════════════════════════ */

import {
  ButtonItem,
  PanelSection,
  PanelSectionRow,
  Navigation,
  Router,
  SteamSpinner,
} from "@decky/ui";
import { useReducer, useEffect, useRef, useCallback } from "react";
import { qrStart, qrStatus, qrStop } from "./api";
import { setLoginState } from "./loginState";
import { HOME_ROUTE } from "./routes";

// ── 状态机 ──────────────────────────────

type QrStage = "init" | "running" | "completed" | "failed";

type QrState = {
  stage: QrStage;
  message: string;
  qrDataUrl: string;
  sessionId: string;
  mounted: boolean;
};

type QrAction =
  | { type: "RESET" }
  | { type: "RUNNING"; sessionId: string; message: string; qrDataUrl: string }
  | { type: "POLL_MSG"; message: string }
  | { type: "COMPLETED"; message: string }
  | { type: "FAILED"; message: string }
  | { type: "UNMOUNT" };

function qrReducer(state: QrState, action: QrAction): QrState {
  switch (action.type) {
    case "RESET":
      return { stage: "init", message: "", qrDataUrl: "", sessionId: "", mounted: true };
    case "RUNNING":
      return { ...state, stage: "running", sessionId: action.sessionId, message: action.message, qrDataUrl: action.qrDataUrl };
    case "POLL_MSG":
      return { ...state, message: action.message };
    case "COMPLETED":
      return { ...state, stage: "completed", message: action.message };
    case "FAILED":
      return { ...state, stage: "failed", message: action.message };
    case "UNMOUNT":
      return { ...state, mounted: false };
    default:
      return state;
  }
}

// ── 组件 ────────────────────────────────

export default function QrLoginPage() {
  const [state, dispatch] = useReducer(qrReducer, {
    stage: "init", message: "", qrDataUrl: "", sessionId: "", mounted: true,
  });
  const timerRef = useRef(0);
  const sidRef = useRef("");  // 可变引用 — 供异步回调读取最新值

  const stopTimer = () => { if (timerRef.current) { clearInterval(timerRef.current); timerRef.current = 0; } };

  const navigateHome = () => { try { Navigation.Navigate(HOME_ROUTE); } catch {} };

  const doPoll = useCallback(async () => {
    const sid = sidRef.current;
    if (!sid) return;
    try {
      const r = await qrStatus(sid);
      const d = r?.data; if (!d) return;
      if (d.stage === "completed") {
        setLoginState({
          logged_in: true,
          account_name: d.user_account || "",
          message: d.message || "ok",
        });
        dispatch({ type: "COMPLETED", message: d.message || "" });
        stopTimer();
        setTimeout(navigateHome, 1200);
        return;
      }
      if (d.stage === "failed" || d.stage === "stopped") {
        dispatch({ type: "FAILED", message: d.message || "登录失败" });
        stopTimer();
        return;
      }
      dispatch({ type: "POLL_MSG", message: d.message || "" });
    } catch {}
  }, []);

  const doStart = useCallback(async () => {
    dispatch({ type: "RESET" }); stopTimer();
    try {
      const r = await qrStart();
      if (r?.status !== "success" || !r.data) {
        dispatch({ type: "FAILED", message: r?.message || "启动失败" });
        return;
      }
      if (r.data.stage === "completed") {
        setLoginState({
          logged_in: true,
          account_name: r.data.user_account || "",
          message: r.data.message || "ok",
        });
        dispatch({ type: "COMPLETED", message: r.data.message || "已登录" });
        setTimeout(navigateHome, 1200);
        return;
      }
      if (!r.data.session_id) {
        dispatch({ type: "FAILED", message: r?.message || r.data.message || "启动失败" });
        return;
      }
      sidRef.current = r.data.session_id;
      dispatch({ type: "RUNNING", sessionId: r.data.session_id, message: r.data.message || "", qrDataUrl: r.data.qr_data_url || "" });
      setTimeout(() => doPoll(), 200);
    } catch (e: any) {
      dispatch({ type: "FAILED", message: String(e) });
    }
  }, [doPoll]);

  // 启动 / 清理
  useEffect(() => {
    try { Router.CloseSideMenus?.(); } catch {}
    doStart();
    return () => { dispatch({ type: "UNMOUNT" }); stopTimer(); };
  }, []);

  // 轮询
  useEffect(() => {
    if (state.stage !== "running") return;
    stopTimer();
    timerRef.current = window.setInterval(doPoll, 1600);
    return stopTimer;
  }, [state.stage, doPoll]);

  // 卸载时停止远程会话
  useEffect(() => () => {
    if (sidRef.current) qrStop(sidRef.current).catch(() => {});
  }, []);

  // ── 渲染 ──────────────────────────────

  if (state.stage === "failed") return (
    <div style={{ paddingTop: 40 }}>
      <PanelSection title="登录失败">
        <PanelSectionRow><div style={{ color: "#e03e3e", fontSize: 13, padding: "10px 0" }}>{state.message}</div></PanelSectionRow>
        <PanelSectionRow><ButtonItem onClick={doStart}>重试</ButtonItem></PanelSectionRow>
      </PanelSection>
    </div>
  );

  if (state.stage === "completed") return (
    <div style={{ paddingTop: 40 }}>
      <PanelSection title="登录">
        <PanelSectionRow><div style={{ color: "#59bf40", fontSize: 14, padding: "10px 0" }}>登录成功，跳转中…</div></PanelSectionRow>
      </PanelSection>
    </div>
  );

  if (state.stage === "init") return (
    <div style={{ height: "100%", display: "flex", flexDirection: "column", alignItems: "center", justifyContent: "center", paddingTop: 40 }}>
      <SteamSpinner style={{ width: 40, height: 40 }} />
      <div style={{ color: "#8b929a", fontSize: 13, marginTop: 12 }}>正在连接…</div>
    </div>
  );

  return (
    <div style={{ height: "100%", display: "flex", flexDirection: "column", paddingTop: 40 }}>
      <div style={{ flex: 1, display: "flex", flexWrap: "wrap", alignItems: "center", justifyContent: "center", gap: 24, padding: 16 }}>
        <div style={{ flex: "0 0 auto" }}>
          {state.qrDataUrl
            ? <img src={state.qrDataUrl} alt="QR" style={{ width: "min(56vw, 260px)", aspectRatio: "1", borderRadius: 16, background: "#fff" }} />
            : <SteamSpinner style={{ width: 40, height: 40 }} />}
        </div>
        <div style={{ flex: "1 1 200px", maxWidth: 320 }}>
          <div style={{ fontSize: 22, fontWeight: 500, marginBottom: 6, color: "#fff" }}>扫码登录</div>
          <div style={{ fontSize: 13, color: "#8b929a", marginBottom: 6 }}>使用天翼云盘 App 扫描二维码</div>
          <div style={{ fontSize: 15, color: state.message ? "#f6c26b" : "transparent", marginBottom: 8, minHeight: 20 }}>{state.message || "\u00A0"}</div>
          <ButtonItem onClick={doStart}>刷新</ButtonItem>
        </div>
      </div>
    </div>
  );
}
