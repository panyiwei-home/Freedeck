(function () {
  "use strict";

  const statusEl = document.getElementById("status");
  const metaEl = document.getElementById("meta");
  const stageBadgeEl = document.getElementById("stageBadge");
  const resultTitleEl = document.getElementById("resultTitle");
  const resultHintEl = document.getElementById("resultHint");
  const diagnosticsEl = document.getElementById("diagnostics");
  const qrImageEl = document.getElementById("qrImage");
  const qrPlaceholderEl = document.getElementById("qrPlaceholder");

  const refreshQrBtn = document.getElementById("refreshQrBtn");

  const OFFICIAL_LOGIN_URL = "https://cloud.189.cn/web/login.html";
  const POLL_INTERVAL_MS = 1600;

  let pollTimer = 0;
  let currentSessionId = "";
  let currentStage = "idle";
  let lastReason = "";
  let completedOnce = false;
  let stoppingSession = false;

  const STAGE_META = {
    idle: { label: "未开始", tone: "pending" },
    running: { label: "进行中", tone: "pending" },
    completed: { label: "已完成", tone: "success" },
    failed: { label: "失败", tone: "failed" },
    stopped: { label: "已停止", tone: "failed" },
  };

  function setStatus(text, tone) {
    if (!statusEl) return;
    statusEl.textContent = String(text || "");
    statusEl.classList.remove("warn", "danger");
    if (tone === "warn") statusEl.classList.add("warn");
    if (tone === "danger") statusEl.classList.add("danger");
  }

  function setResult(stage, title, hint) {
    currentStage = String(stage || "idle");
    const meta = STAGE_META[currentStage] || STAGE_META.idle;

    if (stageBadgeEl) {
      stageBadgeEl.textContent = meta.label;
      stageBadgeEl.classList.remove("pending", "success", "failed");
      stageBadgeEl.classList.add(meta.tone);
    }
    if (resultTitleEl) resultTitleEl.textContent = String(title || "");
    if (resultHintEl) resultHintEl.textContent = String(hint || "");
    renderMeta();
  }

  function renderMeta() {
    if (!metaEl) return;
    const lines = [];
    lines.push("登录模式：后端二维码轮询");
    lines.push("官方地址：" + OFFICIAL_LOGIN_URL);
    lines.push("当前阶段：" + ((STAGE_META[currentStage] || {}).label || currentStage));
    if (currentSessionId) {
      lines.push("会话 ID：" + currentSessionId);
    }
    if (lastReason) {
      lines.push("最近 reason：" + lastReason);
    }
    metaEl.textContent = lines.join("\n");
  }

  function renderDiagnostics(data) {
    if (!diagnosticsEl) return;
    if (!data || typeof data !== "object") {
      diagnosticsEl.textContent = "(暂无)";
      return;
    }
    try {
      const text = JSON.stringify(data, null, 2);
      diagnosticsEl.textContent = text.length > 10000 ? text.slice(0, 10000) + "\n...(截断)" : text;
    } catch (_error) {
      diagnosticsEl.textContent = "(诊断序列化失败)";
    }
  }

  function gotoLibrary(delayMs) {
    window.setTimeout(function () {
      window.location.href = "/tianyi/library";
    }, Math.max(0, Number(delayMs || 0)));
  }

  function stopPolling() {
    if (!pollTimer) return;
    window.clearInterval(pollTimer);
    pollTimer = 0;
  }

  function startPolling() {
    stopPolling();
    if (document.hidden) return;
    pollTimer = window.setInterval(function () {
      pollQrStatus();
    }, POLL_INTERVAL_MS);
  }

  async function stopQrSession(reason) {
    const sessionId = String(currentSessionId || "").trim();
    if (!sessionId || stoppingSession) return;
    stoppingSession = true;
    try {
      await apiPost("/api/tianyi/login/qr/stop", {
        session_id: sessionId,
        reason: String(reason || ""),
      });
    } catch (_error) {
      // 页面关闭/切后台时失败可忽略。
    } finally {
      stoppingSession = false;
    }
  }

  function updateQrImage(imageUrl, sessionId) {
    const safeUrl = String(imageUrl || "").trim();
    if (!qrImageEl || !qrPlaceholderEl) return;
    if (!safeUrl) {
      qrImageEl.hidden = true;
      qrImageEl.removeAttribute("src");
      qrPlaceholderEl.hidden = false;
      qrPlaceholderEl.textContent = "二维码准备中...";
      return;
    }

    const separator = safeUrl.indexOf("?") >= 0 ? "&" : "?";
    const fullUrl = safeUrl + separator + "session_id=" + encodeURIComponent(sessionId || "");
    qrImageEl.src = fullUrl;
    qrImageEl.hidden = false;
    qrPlaceholderEl.hidden = true;
  }

  async function apiGet(path) {
    const resp = await fetch(path, {
      method: "GET",
      headers: { Accept: "application/json" },
    });
    return resp.json();
  }

  async function apiPost(path, body) {
    const resp = await fetch(path, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Accept: "application/json",
      },
      body: JSON.stringify(body || {}),
    });
    return resp.json();
  }

  async function checkLoginFast() {
    try {
      const result = await apiPost("/api/tianyi/login/check", {});
      if (result.status !== "success") {
        return { loggedIn: false, account: "", message: String(result.message || "登录态检查失败") };
      }
      const data = result.data || {};
      return {
        loggedIn: Boolean(data.logged_in),
        account: String(data.user_account || ""),
        message: String(data.message || ""),
      };
    } catch (error) {
      return { loggedIn: false, account: "", message: "登录态检查异常：" + String(error) };
    }
  }

  function applyQrState(state) {
    if (!state || typeof state !== "object") return false;

    const stage = String(state.stage || "idle");
    const reason = String(state.reason || "");
    const message = String(state.message || "");
    const account = String(state.user_account || "");
    const imageUrl = String(state.image_url || "");
    const sessionId = String(state.session_id || "");
    const diagnostics = state.diagnostics && typeof state.diagnostics === "object" ? state.diagnostics : {};

    if (sessionId) currentSessionId = sessionId;
    lastReason = reason;

    updateQrImage(imageUrl, currentSessionId);
    renderDiagnostics(
      Object.assign({}, diagnostics, {
        stage,
        reason,
        session_id: currentSessionId || "",
      })
    );

    if (stage === "completed") {
      const text = account ? `登录成功：${account}` : "登录成功";
      setResult("completed", text, "登录态已写入插件，正在返回游戏列表...");
      setStatus(message || text, "");
      stopPolling();
      if (!completedOnce) {
        completedOnce = true;
        gotoLibrary(1000);
      }
      return true;
    }

    if (stage === "failed") {
      const hint = message || "二维码登录失败，请刷新二维码后重试。";
      setResult("failed", "登录失败", hint);
      setStatus(hint, "danger");
      stopPolling();
      return true;
    }

    if (stage === "stopped") {
      setResult("stopped", "已停止登录", message || "可刷新二维码重新开始。");
      setStatus(message || "二维码登录已停止", "warn");
      stopPolling();
      return true;
    }

    const runningHint = message || "等待扫码";
    setResult("running", "请扫码登录", runningHint);
    setStatus(runningHint, reason === "waiting_scan" ? "warn" : "");
    return false;
  }

  async function startQrSession() {
    setResult("running", "正在生成二维码", "请稍候...");
    setStatus("正在请求二维码登录会话...", "");
    stopPolling();

    try {
      const result = await apiPost("/api/tianyi/login/qr/start", {});
      if (result.status !== "success" || !result.data) {
        const msg = String(result.message || "二维码会话启动失败");
        setResult("failed", "启动失败", msg);
        setStatus(msg, "danger");
        if (result.diagnostics) renderDiagnostics(result.diagnostics);
        return;
      }
      completedOnce = false;
      const terminal = applyQrState(result.data);
      if (!terminal) {
        startPolling();
        await pollQrStatus();
      }
    } catch (error) {
      const msg = "启动二维码登录异常：" + String(error);
      setResult("failed", "启动异常", msg);
      setStatus(msg, "danger");
    }
  }

  async function pollQrStatus() {
    if (!currentSessionId) return;
    try {
      const result = await apiGet("/api/tianyi/login/qr/status?session_id=" + encodeURIComponent(currentSessionId));
      if (result.status !== "success" || !result.data) return;
      applyQrState(result.data);
    } catch (_error) {
      // 下次轮询继续尝试。
    }
  }

  if (refreshQrBtn) {
    refreshQrBtn.addEventListener("click", function () {
      startQrSession();
    });
  }

  document.addEventListener("visibilitychange", function () {
    if (document.hidden) {
      stopPolling();
      void stopQrSession("document_hidden");
      return;
    }
    if (currentStage === "running") {
      if (currentSessionId) {
        startPolling();
        void pollQrStatus();
      } else {
        void startQrSession();
      }
    }
  });

  window.addEventListener("pagehide", function () {
    stopPolling();
    void stopQrSession("page_hide");
  });

  window.addEventListener("beforeunload", function () {
    stopPolling();
    void stopQrSession("before_unload");
  });

  async function bootstrap() {
    renderMeta();
    setResult("running", "正在检查已有登录态", "每次进入页面都会自动尝试获取登录态。");
    setStatus("正在检查本地登录态...", "");
    updateQrImage("", "");
    renderDiagnostics(null);

    const quick = await checkLoginFast();
    if (quick.loggedIn) {
      const text = quick.account ? `已登录：${quick.account}` : "已登录";
      setResult("completed", text, "正在返回游戏列表...");
      setStatus(quick.message || "登录态有效", "");
      renderDiagnostics({ source: "fast_check", user_account: quick.account || "" });
      gotoLibrary(900);
      return;
    }

    setStatus("未检测到有效登录态，正在启动二维码会话...", "warn");
    await startQrSession();
  }

  bootstrap();
})();
