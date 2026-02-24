const { createApp } = window.Vue;

function buildUrl(path, query = {}) {
  const url = new URL(path, window.location.origin);
  Object.entries(query).forEach(([key, value]) => {
    if (value === undefined || value === null || value === "") return;
    url.searchParams.set(key, String(value));
  });
  return url.toString();
}

async function apiGet(path, query = {}) {
  const resp = await fetch(buildUrl(path, query), {
    method: "GET",
    headers: { Accept: "application/json" },
  });
  return resp.json();
}

async function apiPost(path, body = {}) {
  const resp = await fetch(path, {
    method: "POST",
    headers: { "Content-Type": "application/json", Accept: "application/json" },
    body: JSON.stringify(body),
  });
  return resp.json();
}

createApp({
  data() {
    return {
      settings: {
        download_dir: "",
        install_dir: "",
        split_count: 16,
        page_size: 50,
      },
      list: { total: 0, page: 1, page_size: 50, items: [] },
      query: "",
      activeQuery: "",
      showResults: false,
      hintText: "",
      message: "",
      coverCache: {},
      coverMetaCache: {},
      coverPending: {},
      coverMissCache: {},
      focusedIndex: 0,
      gamepadTimer: null,
      gamepadPressed: {},
      gamepadRepeatState: {},
      gamepadPollMs: 160,
      installDialog: {
        visible: false,
        loading: false,
        submitting: false,
        plan: null,
        game: null,
      },
    };
  },
  computed: {
    totalPages() {
      const total = Number(this.list.total || 0);
      const pageSize = Number(this.list.page_size || 50);
      if (total <= 0 || pageSize <= 0) return 1;
      return Math.max(1, Math.ceil(total / pageSize));
    },
    modalCanConfirm() {
      if (!this.installDialog.plan) return false;
      if (this.installDialog.loading || this.installDialog.submitting) return false;
      return this.storageEnough(this.installDialog.plan);
    },
  },
  mounted() {
    this.bootstrap();
    window.addEventListener("keydown", this.onGlobalKeyDown, { passive: false });
    window.addEventListener("gamepadconnected", this.onGamepadConnected);
    window.addEventListener("gamepaddisconnected", this.onGamepadDisconnected);
    document.addEventListener("visibilitychange", this.onVisibilityChange);
    window.addEventListener("focus", this.onWindowFocus);
    window.addEventListener("blur", this.onWindowBlur);
    this.syncGamepadPolling();
    this.setFocusIndex(0, { scroll: false });
  },
  beforeUnmount() {
    window.removeEventListener("keydown", this.onGlobalKeyDown);
    window.removeEventListener("gamepadconnected", this.onGamepadConnected);
    window.removeEventListener("gamepaddisconnected", this.onGamepadDisconnected);
    document.removeEventListener("visibilitychange", this.onVisibilityChange);
    window.removeEventListener("focus", this.onWindowFocus);
    window.removeEventListener("blur", this.onWindowBlur);
    this.stopGamepadPolling();
  },
  methods: {
    onVisibilityChange() {
      this.syncGamepadPolling();
    },
    onWindowFocus() {
      this.syncGamepadPolling();
    },
    onWindowBlur() {
      this.syncGamepadPolling();
    },
    syncGamepadPolling() {
      const shouldRun = !document.hidden && document.hasFocus();
      if (!shouldRun) {
        this.stopGamepadPolling();
        return;
      }
      this.startGamepadPolling();
    },
    flash(text) {
      this.message = String(text || "");
      if (!this.message) return;
      setTimeout(() => {
        this.message = "";
      }, 2400);
    },
    formatShareAttempt(item) {
      if (!item || typeof item !== "object") return "";
      const profile = String(item.profile || item.step || "").trim();
      const method = String(item.method || "").trim().toUpperCase();
      const host = String(item.host || "").trim();
      const endpoint = String(item.endpoint || "").trim().replace("/api/open/share/", "");
      const status = Number(item.status || 0);
      const reason = String(item.message || "").trim();
      const tags = [profile, method, host].filter(Boolean).join(" ");
      const endpointPart = endpoint ? ` ${endpoint}` : "";
      const statusPart = Number.isFinite(status) && status > 0 ? ` status=${status}` : "";
      const reasonPart = reason ? ` ${reason}` : "";
      return `${tags}${endpointPart}${statusPart}${reasonPart}`.trim();
    },
    summarizeShareAttempts(diagnostics) {
      const attempts = Array.isArray(diagnostics && diagnostics.attempts) ? diagnostics.attempts : [];
      if (!attempts.length) return "";
      const failed = attempts.filter((item) => item && item.ok === false);
      const head = failed[0] || attempts[0] || {};
      const tail = failed[failed.length - 1] || attempts[attempts.length - 1] || {};
      const headText = this.formatShareAttempt(head);
      const tailText = this.formatShareAttempt(tail);
      const failedCount = failed.length;
      let text = `已尝试 ${attempts.length} 条链路，失败 ${failedCount} 条`;
      if (headText) text += `；首个关键尝试：${headText}`;
      if (tailText && tailText !== headText) text += `；最后关键尝试：${tailText}`;
      return text;
    },
    buildPrepareInstallError(result) {
      const base = String((result && result.message) || "安装准备失败").trim();
      const diagnostics = (result && result.diagnostics) || {};
      const attempts = Array.isArray(diagnostics.attempts) ? diagnostics.attempts : [];
      if (!attempts.length) return base;
      const headline = base.includes("shareId") ? base : `${base}（未获取shareId）`;
      const summary = this.summarizeShareAttempts(diagnostics);
      return summary ? `${headline}。${summary}` : headline;
    },
    normalizePath(path) {
      return String(path || "")
        .trim()
        .replace(/\\/g, "/")
        .replace(/\/+$/, "")
        .toLowerCase();
    },
    toNum(value) {
      const num = Number(value || 0);
      return Number.isFinite(num) ? Math.max(0, num) : 0;
    },
    samePathOrNest(pathA, pathB) {
      if (!pathA || !pathB) return false;
      if (pathA === pathB) return true;
      return pathA.startsWith(`${pathB}/`) || pathB.startsWith(`${pathA}/`);
    },
    isSameStorage(plan) {
      if (!plan || typeof plan !== "object") return false;
      const downloadDir = this.normalizePath(plan.download_dir);
      const installDir = this.normalizePath(plan.install_dir);
      if (this.samePathOrNest(downloadDir, installDir)) return true;

      const freeDownload = this.toNum(plan.free_download_bytes);
      const freeInstall = this.toNum(plan.free_install_bytes);
      if (freeDownload > 0 && freeInstall > 0) {
        const diff = Math.abs(freeDownload - freeInstall);
        if (diff <= 64 * 1024 * 1024) return true;
      }
      return false;
    },
    totalRequiredBytes(plan) {
      return this.toNum(plan && plan.required_download_bytes) + this.toNum(plan && plan.required_install_bytes);
    },
    totalRequiredFormula(plan) {
      const packText = this.formatBytes(this.toNum(plan && plan.required_download_bytes));
      const gameText = this.formatBytes(this.toNum(plan && plan.required_install_bytes));
      return `压缩包（${packText}）+游戏本体（${gameText}）`;
    },
    totalFreeBytes(plan) {
      if (!plan || typeof plan !== "object") return 0;
      const freeDownload = this.toNum(plan.free_download_bytes);
      const freeInstall = this.toNum(plan.free_install_bytes);
      if (freeDownload > 0 && freeInstall > 0) {
        return Math.min(freeDownload, freeInstall);
      }
      return Math.max(freeDownload, freeInstall);
    },
    combinedSpaceOk(plan) {
      const need = this.totalRequiredBytes(plan);
      const free = this.totalFreeBytes(plan);
      if (need <= 0) return Boolean(plan && plan.can_install);
      return free >= need;
    },
    storageEnough(plan) {
      if (!plan || typeof plan !== "object") return false;
      if (this.isSameStorage(plan)) return this.combinedSpaceOk(plan);
      return this.downloadSpaceOk(plan) && this.installSpaceOk(plan);
    },
    freeSpaceLabel(plan) {
      if (!plan || typeof plan !== "object") return "-";
      if (this.isSameStorage(plan)) {
        return this.formatBytes(this.totalFreeBytes(plan));
      }
      const freeDownload = this.formatBytes(this.toNum(plan.free_download_bytes));
      const freeInstall = this.formatBytes(this.toNum(plan.free_install_bytes));
      return `下载盘 ${freeDownload} / 安装盘 ${freeInstall}`;
    },
    storageChipLabel(plan) {
      return this.storageEnough(plan) ? "充足" : "不足";
    },
    storageChipClass(plan) {
      return this.storageEnough(plan) ? "chip-good" : "chip-bad";
    },
    protonTierFor(item) {
      const key = this.getCoverCacheKey(item);
      if (!key) return "";
      const meta = this.coverMetaCache[key];
      if (!meta || typeof meta !== "object") return "";
      return String(meta.proton_tier || "").trim();
    },
    protonTierLabel(tierRaw) {
      const tier = String(tierRaw || "").trim().toLowerCase();
      if (!tier) return "未知";
      if (["platinum", "gold", "native"].includes(tier)) return "绿标";
      if (["silver", "bronze"].includes(tier)) return "黄标";
      if (["borked", "unsupported"].includes(tier)) return "红标";
      return tier.toUpperCase();
    },
    protonTierClass(tierRaw) {
      const tier = String(tierRaw || "").trim().toLowerCase();
      if (["platinum", "gold", "native"].includes(tier)) return "tier-green";
      if (["silver", "bronze"].includes(tier)) return "tier-yellow";
      if (["borked", "unsupported"].includes(tier)) return "tier-red";
      return "tier-neutral";
    },
    protonBadgeClass(tierRaw) {
      const tier = String(tierRaw || "").trim().toLowerCase();
      if (["platinum", "gold", "native"].includes(tier)) return "tier-good";
      if (["silver", "bronze"].includes(tier)) return "tier-mid";
      if (["borked", "unsupported"].includes(tier)) return "tier-bad";
      return "tier-unknown";
    },
    protonBadgeMark(tierRaw) {
      const tier = String(tierRaw || "").trim().toLowerCase();
      if (["platinum", "gold", "native"].includes(tier)) return "✓";
      if (["silver", "bronze"].includes(tier)) return "!";
      if (["borked", "unsupported"].includes(tier)) return "×";
      return "?";
    },
    downloadSpaceOk(plan) {
      if (!plan || typeof plan !== "object") return false;
      const free = this.toNum(plan.free_download_bytes);
      const need = this.toNum(plan.required_download_bytes);
      if (free > 0 || need > 0) return free >= need;
      return Boolean(plan.download_dir_ok);
    },
    installSpaceOk(plan) {
      if (!plan || typeof plan !== "object") return false;
      const free = this.toNum(plan.free_install_bytes);
      const need = this.toNum(plan.required_install_bytes);
      if (free > 0 || need > 0) return free >= need;
      return Boolean(plan.install_dir_ok);
    },
    async bootstrap() {
      try {
        const result = await apiGet("/api/tianyi/state");
        if (result.status !== "success") return;
        const data = result.data || {};
        this.settings = Object.assign({}, this.settings, data.settings || {});
        if (!this.settings.page_size) this.settings.page_size = 50;
        if (!this.settings.split_count) this.settings.split_count = 16;
        if (!this.settings.install_dir) this.settings.install_dir = this.settings.download_dir || "";
      } catch (_error) {
        // 状态读取失败不阻断搜索流程。
      }
    },
    getFocusableCount() {
      if (this.installDialog.visible) {
        return 2;
      }
      const cardCount = this.showResults ? Number(this.list.items.length || 0) : 0;
      return 2 + Math.max(0, cardCount);
    },
    getCardColumns() {
      const cards = Array.from(document.querySelectorAll(".game-card"));
      if (cards.length <= 1) return 1;
      const firstTop = cards[0].getBoundingClientRect().top;
      let columns = 0;
      for (const card of cards) {
        const top = card.getBoundingClientRect().top;
        if (Math.abs(top - firstTop) <= 6) {
          columns += 1;
        }
      }
      return Math.max(1, columns || 1);
    },
    focusElementByIndex(index, options = {}) {
      const scroll = options.scroll !== false;
      let el = null;
      if (this.installDialog.visible) {
        el = index <= 0 ? document.getElementById("install-cancel") : document.getElementById("install-confirm");
      } else if (index === 0) {
        el = document.getElementById("search-input");
      } else if (index === 1) {
        el = document.getElementById("search-button");
      } else if (index >= 2) {
        const cardIndex = index - 2;
        el = document.querySelector(`.game-card[data-card-index="${cardIndex}"]`);
      }
      if (!el || typeof el.focus !== "function") return;
      try {
        el.focus({ preventScroll: true });
      } catch (_error) {
        el.focus();
      }
      if (scroll && !this.installDialog.visible && index >= 2 && typeof el.scrollIntoView === "function") {
        el.scrollIntoView({ block: "nearest", inline: "nearest", behavior: "smooth" });
      }
    },
    setFocusIndex(index, options = {}) {
      const total = this.getFocusableCount();
      if (total <= 0) return;
      const next = Math.max(0, Math.min(total - 1, Number(index || 0)));
      this.focusedIndex = next;
      this.$nextTick(() => this.focusElementByIndex(next, options));
    },
    moveFocus(direction) {
      if (this.installDialog.visible) {
        if (["left", "up"].includes(direction)) this.setFocusIndex(0, { scroll: false });
        if (["right", "down"].includes(direction)) this.setFocusIndex(1, { scroll: false });
        return;
      }

      const total = this.getFocusableCount();
      if (total <= 0) return;

      const cardStart = 2;
      const hasCards = total > cardStart;
      const columns = this.getCardColumns();
      let next = this.focusedIndex;

      if (direction === "left") {
        if (next > cardStart) {
          next -= 1;
        } else if (next === 1) {
          next = 0;
        }
      } else if (direction === "right") {
        if (next === 0) {
          next = 1;
        } else if (next >= cardStart && next < total - 1) {
          next += 1;
        }
      } else if (direction === "up") {
        if (next >= cardStart) {
          const up = next - columns;
          next = up >= cardStart ? up : 0;
        } else if (next === 1) {
          next = 0;
        }
      } else if (direction === "down") {
        if (next === 0) {
          next = 1;
        } else if (next === 1 && hasCards) {
          next = cardStart;
        } else if (next >= cardStart) {
          next = Math.min(total - 1, next + columns);
        }
      }

      this.setFocusIndex(next);
    },
    async activateFocused() {
      if (this.installDialog.visible) {
        if (this.focusedIndex <= 0) {
          this.closeInstallDialog();
          return;
        }
        await this.confirmInstall();
        return;
      }
      const index = this.focusedIndex;
      if (index <= 1) {
        await this.reloadList(1);
        return;
      }
      const cardIndex = index - 2;
      const item = this.list.items[cardIndex];
      if (item) {
        await this.openInstallConfirm(item);
      }
    },
    handleBackAction() {
      if (this.installDialog.visible) {
        this.closeInstallDialog();
        return;
      }
      if (this.focusedIndex !== 0) {
        this.setFocusIndex(0, { scroll: false });
        return;
      }
      if (this.showResults) {
        this.showResults = false;
        this.hintText = "";
      }
      this.setFocusIndex(0, { scroll: false });
    },
    onGlobalKeyDown(event) {
      const key = String(event.key || "");
      if (["ArrowUp", "ArrowDown", "ArrowLeft", "ArrowRight", "Enter", "Escape"].includes(key)) {
        event.preventDefault();
      }

      if (key === "ArrowUp") this.moveFocus("up");
      if (key === "ArrowDown") this.moveFocus("down");
      if (key === "ArrowLeft") this.moveFocus("left");
      if (key === "ArrowRight") this.moveFocus("right");
      if (key === "Enter") this.activateFocused();
      if (key === "Escape") this.handleBackAction();
    },
    pressEdge(name, pressed, callback) {
      const wasPressed = Boolean(this.gamepadPressed[name]);
      if (pressed && !wasPressed) {
        callback();
      }
      this.gamepadPressed[name] = Boolean(pressed);
    },
    repeatDirectional(name, pressed, callback, nowTs) {
      const repeatDelay = 260;
      const repeatInterval = 110;
      const state = this.gamepadRepeatState[name] || { active: false, nextAt: 0 };
      if (!pressed) {
        state.active = false;
        state.nextAt = 0;
        this.gamepadRepeatState[name] = state;
        return;
      }
      if (!state.active) {
        callback();
        state.active = true;
        state.nextAt = nowTs + repeatDelay;
        this.gamepadRepeatState[name] = state;
        return;
      }
      if (nowTs >= state.nextAt) {
        callback();
        state.nextAt = nowTs + repeatInterval;
      }
      this.gamepadRepeatState[name] = state;
    },
    readPrimaryGamepad() {
      if (!navigator.getGamepads) return null;
      const pads = navigator.getGamepads();
      if (!pads) return null;
      for (const pad of pads) {
        if (pad) return pad;
      }
      return null;
    },
    pollGamepad() {
      const pad = this.readPrimaryGamepad();
      if (!pad) return;

      const buttons = pad.buttons || [];
      const axes = pad.axes || [];
      const axisX = Number(axes[0] || 0);
      const axisY = Number(axes[1] || 0);
      const deadzone = 0.56;
      const nowTs = Date.now();

      const up = Boolean(buttons[12] && buttons[12].pressed) || axisY <= -deadzone;
      const down = Boolean(buttons[13] && buttons[13].pressed) || axisY >= deadzone;
      const left = Boolean(buttons[14] && buttons[14].pressed) || axisX <= -deadzone;
      const right = Boolean(buttons[15] && buttons[15].pressed) || axisX >= deadzone;
      const a = Boolean(buttons[0] && buttons[0].pressed);
      const b = Boolean(buttons[1] && buttons[1].pressed);

      this.repeatDirectional("up", up, () => this.moveFocus("up"), nowTs);
      this.repeatDirectional("down", down, () => this.moveFocus("down"), nowTs);
      this.repeatDirectional("left", left, () => this.moveFocus("left"), nowTs);
      this.repeatDirectional("right", right, () => this.moveFocus("right"), nowTs);
      this.pressEdge("a", a, () => this.activateFocused());
      this.pressEdge("b", b, () => this.handleBackAction());
    },
    startGamepadPolling() {
      if (this.gamepadTimer) return;
      if (document.hidden || !document.hasFocus()) return;
      this.gamepadTimer = window.setInterval(() => this.pollGamepad(), this.gamepadPollMs);
    },
    stopGamepadPolling() {
      if (!this.gamepadTimer) return;
      window.clearInterval(this.gamepadTimer);
      this.gamepadTimer = null;
      this.gamepadPressed = {};
      this.gamepadRepeatState = {};
    },
    onGamepadConnected() {
      this.startGamepadPolling();
    },
    onGamepadDisconnected() {
      const pad = this.readPrimaryGamepad();
      if (!pad) {
        this.gamepadPressed = {};
        this.gamepadRepeatState = {};
      }
    },
    async reloadList(page = 1) {
      const q = this.query.trim();
      if (!q) {
        this.showResults = false;
        this.activeQuery = "";
        this.list = { total: 0, page: 1, page_size: this.settings.page_size || 50, items: [] };
        this.hintText = "";
        this.setFocusIndex(0, { scroll: false });
        return;
      }

      this.showResults = true;
      this.activeQuery = q;
      this.hintText = "";

      const result = await apiGet("/api/tianyi/catalog", {
        q,
        page,
        page_size: this.settings.page_size,
      });
      if (result.status !== "success") {
        this.flash(result.message || "搜索失败");
        this.hintText = "";
        this.setFocusIndex(0, { scroll: false });
        return;
      }

      this.list = Object.assign({ total: 0, page: 1, page_size: 50, items: [] }, result.data || {});
      this.hintText = `已显示“${q}”的搜索结果`;
      if (!this.list.items || this.list.items.length === 0) {
        this.setFocusIndex(0, { scroll: false });
      } else {
        this.setFocusIndex(2, { scroll: true });
        void this.prefetchCovers(this.list.items);
      }
    },
    getCoverCacheKey(item) {
      return String((item && item.game_id) || (item && item.title) || "").trim();
    },
    extractBuiltInCover(item) {
      const candidateKeys = ["cover_url", "cover", "image_url", "image", "pic_url", "pic", "thumbnail", "poster"];
      for (const key of candidateKeys) {
        const value = String((item && item[key]) || "").trim();
        if (!value) continue;
        if (value.startsWith("http://") || value.startsWith("https://") || value.startsWith("/")) {
          return value;
        }
      }
      return "";
    },
    buildSteamCoverCandidates(appIdRaw) {
      const appId = Number(appIdRaw || 0);
      if (!Number.isFinite(appId) || appId <= 0) return [];
      const id = Math.floor(appId);
      return [
        `https://shared.fastly.steamstatic.com/store_item_assets/steam/apps/${id}/library_600x900_2x.jpg`,
        `https://shared.fastly.steamstatic.com/store_item_assets/steam/apps/${id}/library_600x900.jpg`,
        `https://cdn.cloudflare.steamstatic.com/steam/apps/${id}/library_600x900_2x.jpg`,
        `https://cdn.cloudflare.steamstatic.com/steam/apps/${id}/library_600x900.jpg`,
        `https://shared.fastly.steamstatic.com/store_item_assets/steam/apps/${id}/capsule_616x353.jpg`,
      ];
    },
    collectCoverCandidates(item) {
      const candidates = [];
      const seen = new Set();
      const push = (value) => {
        const url = String(value || "").trim();
        if (!url) return;
        if (
          !url.startsWith("http://")
          && !url.startsWith("https://")
          && !url.startsWith("/")
          && !url.startsWith("data:image/")
        ) {
          return;
        }
        if (seen.has(url)) return;
        seen.add(url);
        candidates.push(url);
      };

      const cacheKey = this.getCoverCacheKey(item);
      const meta = (cacheKey && this.coverMetaCache[cacheKey] && typeof this.coverMetaCache[cacheKey] === "object")
        ? this.coverMetaCache[cacheKey]
        : null;
      const appId = Number((meta && meta.app_id) || (item && item.app_id) || 0);

      push(meta && meta.square_cover_url);
      this.buildSteamCoverCandidates(appId).forEach((url) => push(url));
      push(meta && meta.cover_url);
      push(this.extractBuiltInCover(item));
      push(cacheKey && this.coverCache[cacheKey]);

      return candidates;
    },
    async prefetchCovers(items) {
      const queue = Array.isArray(items) ? items.filter(Boolean) : [];
      if (!queue.length) return;
      const workerCount = Math.max(1, Math.min(4, queue.length));
      let cursor = 0;
      const worker = async () => {
        while (cursor < queue.length) {
          const current = queue[cursor];
          cursor += 1;
          await this.fetchCoverForItem(current);
        }
      };
      const workers = Array.from({ length: workerCount }, () => worker());
      await Promise.all(workers);
    },
    async fetchCoverForItem(item, options = {}) {
      const cacheKey = this.getCoverCacheKey(item);
      if (!cacheKey) return;
      const force = Boolean(options && options.force);
      if (this.coverPending[cacheKey]) return;
      if (!force && this.coverMissCache[cacheKey]) return;
      if (force && this.coverMissCache[cacheKey]) delete this.coverMissCache[cacheKey];

      const builtInCover = this.extractBuiltInCover(item);
      if (builtInCover) {
        this.coverCache[cacheKey] = builtInCover;
        return;
      }

      this.coverPending[cacheKey] = true;
      try {
        const result = await apiGet("/api/tianyi/catalog/cover", {
          game_id: String((item && item.game_id) || ""),
          title: String((item && item.title) || ""),
          categories: String((item && item.categories) || ""),
        });
        const data = (result && result.data && typeof result.data === "object") ? result.data : {};
        const coverUrl = String(data.cover_url || "").trim();
        const squareCoverUrl = String(data.square_cover_url || "").trim();
        const protonTier = String(data.protondb_tier || "").trim();
        const appId = Number(data.app_id || 0);
        this.coverMetaCache[cacheKey] = {
          app_id: Number.isFinite(appId) ? appId : 0,
          proton_tier: protonTier,
          cover_url: coverUrl,
          square_cover_url: squareCoverUrl,
        };

        if (result && result.status === "success") {
          const preferred = this.collectCoverCandidates(item)[0] || "";
          if (preferred) {
            this.coverCache[cacheKey] = preferred;
          }
          if (preferred || coverUrl || squareCoverUrl || protonTier || appId > 0) {
            return;
          }
        }
        this.coverMissCache[cacheKey] = true;
      } catch (_error) {
        this.coverMissCache[cacheKey] = true;
      } finally {
        delete this.coverPending[cacheKey];
      }
    },
    coverFor(item) {
      const cacheKey = this.getCoverCacheKey(item);
      const candidates = this.collectCoverCandidates(item);
      if (candidates.length > 0) {
        const first = candidates[0];
        if (cacheKey) {
          this.coverCache[cacheKey] = first;
          if (!this.coverPending[cacheKey] && !this.coverMissCache[cacheKey]) {
            const meta = this.coverMetaCache[cacheKey];
            if (!meta || !meta.square_cover_url) {
              void this.fetchCoverForItem(item);
            }
          }
        }
        return first;
      }

      const placeholder = this.buildPlaceholderCover(item);
      if (cacheKey) {
        this.coverCache[cacheKey] = placeholder;
        if (!this.coverPending[cacheKey] && !this.coverMissCache[cacheKey]) {
          void this.fetchCoverForItem(item);
        }
      }
      return placeholder;
    },
    onCoverError(event, item) {
      if (!event || !event.target) return;
      const img = event.target;
      const attemptedRaw = String(img.dataset.coverAttempts || "").trim();
      const attempted = attemptedRaw ? attemptedRaw.split("||").filter(Boolean) : [];
      const current = String(img.getAttribute("src") || "").trim();
      if (current && !attempted.includes(current)) attempted.push(current);

      const candidates = this.collectCoverCandidates(item);
      const next = candidates.find((url) => !attempted.includes(url));
      if (next) {
        img.dataset.coverAttempts = [...attempted, next].join("||");
        img.src = next;
        return;
      }

      const cacheKey = this.getCoverCacheKey(item);
      if (cacheKey) this.coverMissCache[cacheKey] = true;
      img.src = this.buildPlaceholderCover(item);
    },
    buildPlaceholderCover(item) {
      const title = String((item && item.title) || "Freedeck").trim();
      const category = String((item && item.categories) || "游戏").trim();
      const initials = title
        .replace(/\s+/g, " ")
        .split(/[\/\s\-_:]+/)
        .filter(Boolean)
        .slice(0, 2)
        .map((part) => part.slice(0, 1).toUpperCase())
        .join("");
      const main = (initials || "G").slice(0, 2);
      const escapedTitle = title.replace(/[&<>"]/g, "");
      const escapedCategory = category.replace(/[&<>"]/g, "");
      const svg = `
<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 600 600">
  <defs>
    <linearGradient id="bg" x1="0" y1="0" x2="1" y2="1">
      <stop offset="0%" stop-color="#222222" />
      <stop offset="100%" stop-color="#0f0f0f" />
    </linearGradient>
  </defs>
  <rect width="600" height="600" fill="url(#bg)" />
  <circle cx="520" cy="110" r="140" fill="#303030" opacity="0.4" />
  <circle cx="120" cy="540" r="170" fill="#2a2a2a" opacity="0.35" />
  <text x="300" y="290" text-anchor="middle" font-size="118" fill="#f0f0f0" font-family="Roboto, Noto Sans SC, Microsoft YaHei" font-weight="500">${main}</text>
  <text x="44" y="498" font-size="30" fill="#d8d8d8" font-family="Roboto, Noto Sans SC, Microsoft YaHei">${escapedCategory}</text>
  <text x="44" y="542" font-size="22" fill="#bdbdbd" font-family="Roboto, Noto Sans SC, Microsoft YaHei">${escapedTitle.slice(0, 28)}</text>
</svg>`;
      return `data:image/svg+xml;charset=UTF-8,${encodeURIComponent(svg)}`;
    },
    async openInstallConfirm(item) {
      if (!item || !item.game_id) return;
      void this.fetchCoverForItem(item, { force: true });
      this.installDialog.visible = true;
      this.installDialog.loading = true;
      this.installDialog.submitting = false;
      this.installDialog.plan = null;
      this.installDialog.game = item;
      this.setFocusIndex(0, { scroll: false });

      const result = await apiPost("/api/tianyi/install/prepare", {
        game_id: item.game_id,
        share_url: String(item.down_url || ""),
        download_dir: this.settings.download_dir,
        install_dir: this.settings.install_dir,
      });
      this.installDialog.loading = false;
      if (result.status !== "success") {
        const errorText = this.buildPrepareInstallError(result);
        this.flash(errorText);
        this.closeInstallDialog(true);
        return;
      }
      this.installDialog.plan = result.data || null;
      if (this.installDialog.plan && this.installDialog.plan.install_dir) {
        this.settings.install_dir = String(this.installDialog.plan.install_dir || this.settings.install_dir || "");
      }
      this.setFocusIndex(this.modalCanConfirm ? 1 : 0, { scroll: false });
    },
    closeInstallDialog(force = false) {
      if (!force && this.installDialog.submitting) return;
      this.installDialog.visible = false;
      this.installDialog.loading = false;
      this.installDialog.submitting = false;
      this.installDialog.plan = null;
      this.installDialog.game = null;
      this.setFocusIndex(0, { scroll: false });
    },
    async confirmInstall() {
      if (!this.installDialog.plan || !this.modalCanConfirm) return;
      this.installDialog.submitting = true;
      const plan = this.installDialog.plan;
      const fileIds = Array.isArray(plan.files)
        ? plan.files.map((item) => String((item && item.file_id) || "")).filter(Boolean)
        : [];

      const result = await apiPost("/api/tianyi/install/start", {
        game_id: String(plan.game_id || ""),
        share_url: String(plan.share_url || (this.installDialog.game && this.installDialog.game.down_url) || ""),
        file_ids: fileIds,
        split_count: this.settings.split_count,
        download_dir: this.settings.download_dir,
        install_dir: this.settings.install_dir,
      });
      this.installDialog.submitting = false;
      if (result.status !== "success") {
        this.flash(result.message || "创建安装任务失败");
        return;
      }
      this.flash("安装任务已创建，正在下载");
      this.closeInstallDialog(true);
    },
    formatBytes(bytes) {
      const num = Number(bytes || 0);
      if (!Number.isFinite(num) || num <= 0) return "0 B";
      const units = ["B", "KB", "MB", "GB", "TB"];
      let value = num;
      let idx = 0;
      while (value >= 1024 && idx < units.length - 1) {
        value /= 1024;
        idx += 1;
      }
      if (idx === 0) return `${Math.floor(value)} ${units[idx]}`;
      return `${value.toFixed(2)} ${units[idx]}`;
    },
    formatBytesCompact(bytes) {
      const num = Number(bytes || 0);
      if (!Number.isFinite(num) || num <= 0) return "0B";
      if (num >= 1024 ** 3) return `${(num / (1024 ** 3)).toFixed(2)}G`;
      if (num >= 1024 ** 2) return `${(num / (1024 ** 2)).toFixed(2)}M`;
      if (num >= 1024) return `${(num / 1024).toFixed(2)}K`;
      return `${Math.floor(num)}B`;
    },
    splitModalTitle(title) {
      const raw = String(title || "").replace(/\s+/g, " ").trim();
      if (!raw) {
        return { cn: "-", en: "GAME INSTALL" };
      }

      const sepParts = raw
        .split(/[\/|｜]/)
        .map((part) => part.trim())
        .filter(Boolean);
      if (sepParts.length >= 2) {
        const cn = (sepParts[0].match(/[\u3400-\u9FFF0-9A-Za-z：:·《》、（）()\-]+/g) || [sepParts[0]]).join("").trim() || "-";
        const en = sepParts.slice(1).join(" ").replace(/[^A-Za-z0-9\s:.'\-&!?()]/g, " ").replace(/\s+/g, " ").trim().toUpperCase() || "GAME INSTALL";
        return { cn, en };
      }

      const cnParts = raw.match(/[\u3400-\u9FFF0-9：:·《》、（）()]+/g) || [];
      const cn = cnParts.join("").trim() || raw;
      const englishMatches = raw.match(/[A-Za-z][A-Za-z0-9\s:.'\-&!?()]+/g) || [];
      const en = englishMatches.join(" ").replace(/\s+/g, " ").trim().toUpperCase();
      return { cn, en: en || "GAME INSTALL" };
    },
    modalChineseTitle(title) {
      return this.splitModalTitle(title).cn || "-";
    },
    modalEnglishTitle(title) {
      return this.splitModalTitle(title).en || "GAME INSTALL";
    },
    statusClass(ok) {
      return ok ? "space-ok" : "space-bad";
    },
  },
}).mount("#app");
