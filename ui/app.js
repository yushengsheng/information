const stateTextMap = {
  idle: "等待启动",
  starting: "正在启动",
  running: "监控中",
  stopping: "正在停止",
  stopped: "已停止",
  error: "监控异常",
};

const stateHintMap = {
  WARMING_UP: "窗口尚未跑满，持续性判断还不完整。",
  NORMAL: "双向主动成交没有达到可疑阈值。",
  TWO_SIDED_ACTIVE: "买卖两侧都很活跃，但暂未触发疑似刷量。",
  SUSPECTED_WASH_LIKE: "成交活跃、方向均衡且价格位移有限，疑似刷量节奏。",
};

const judgementStatusTextMap = {
  positive: "成立",
  neutral: "待观察",
  negative: "不明显",
  warming: "预热中",
};

const form = document.getElementById("configForm");
const feedback = document.getElementById("feedback");
const modeText = document.getElementById("modeText");
const startButton = document.getElementById("startButton");
const addButton = document.getElementById("addButton");
const removeButton = document.getElementById("removeButton");
const stopButton = document.getElementById("stopButton");
const logOutput = document.getElementById("logOutput");
const windowGrid = document.getElementById("windowGrid");
const manageConfirmBar = document.getElementById("manageConfirmBar");
const headlinePanel = document.getElementById("headlinePanel");
const overallHeadline = document.getElementById("overallHeadline");
const overallSummary = document.getElementById("overallSummary");
const overallStrongestSymbol = document.getElementById("overallStrongestSymbol");
const overallWindow = document.getElementById("overallWindow");
const overallPhase = document.getElementById("overallPhase");
const versionInfo = document.getElementById("versionInfo");
const symbolOverviewGrid = document.getElementById("symbolOverviewGrid");
const selectedSymbolLabel = document.getElementById("selectedSymbolLabel");
const FRONTEND_VERSION = "20260324-sync-ui-1";

const fields = {
  symbols: document.getElementById("symbols"),
  windowsMinutes: document.getElementById("windowsMinutes"),
  reportInterval: document.getElementById("reportInterval"),
  baselineMinutes: document.getElementById("baselineMinutes"),
  runtimeSeconds: document.getElementById("runtimeSeconds"),
};

const manageSymbolsInput = document.getElementById("manageSymbols");

const summaryFields = {
  symbolCount: document.getElementById("summarySymbolCount"),
  readyCount: document.getElementById("summaryReadyCount"),
  strongestSymbol: document.getElementById("summaryStrongestSymbol"),
  strongestWindow: document.getElementById("summaryStrongestWindow"),
  updatedAt: document.getElementById("summaryUpdatedAt"),
};

let hasHydratedForm = false;
let pollTimer = null;
const expandedEvidenceWindows = new Set();
let isStartPending = false;
let isAddPending = false;
let isRemovePending = false;
let isStopPending = false;
let latestState = null;
let pendingManageAction = null;
let pendingCardRemovalSymbol = null;
let selectedSymbol = null;

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function setFeedback(message, isError = false) {
  feedback.textContent = message;
  feedback.classList.toggle("error", isError);
}

function clearPendingManageAction() {
  pendingManageAction = null;
  manageConfirmBar.hidden = true;
  manageConfirmBar.innerHTML = "";
}

function getAvailableSymbols(state) {
  return (state?.sessions || []).map((session) => session.symbol);
}

function ensureSelectedSymbol(state) {
  const symbols = getAvailableSymbols(state);
  if (!symbols.length) {
    selectedSymbol = null;
    return null;
  }
  if (selectedSymbol && symbols.includes(selectedSymbol)) {
    return selectedSymbol;
  }
  const preferred = state?.overview?.strongest_symbol;
  selectedSymbol = preferred && symbols.includes(preferred) ? preferred : symbols[0];
  return selectedSymbol;
}

function renderManageConfirm() {
  if (!pendingManageAction) {
    clearPendingManageAction();
    return;
  }
  const actionLabel = pendingManageAction.type === "add" ? "添加" : "移除";
  manageConfirmBar.hidden = false;
  manageConfirmBar.innerHTML = `
    <div class="confirm-text">确认要${actionLabel}这些币种吗：${escapeHtml(pendingManageAction.symbols.join(", "))}</div>
    <div class="confirm-actions">
      <button type="button" class="btn btn-confirm" id="confirmManageAction">确定${actionLabel}</button>
      <button type="button" class="btn btn-ghost" id="cancelManageAction">取消</button>
    </div>
  `;
  document.getElementById("confirmManageAction")?.addEventListener("click", async () => {
    const action = pendingManageAction;
    clearPendingManageAction();
    if (!action) {
      return;
    }
    if (action.type === "add") {
      await performAddSymbols(action.symbols);
    } else {
      await performRemoveSymbols(action.symbols);
    }
  });
  document.getElementById("cancelManageAction")?.addEventListener("click", () => {
    clearPendingManageAction();
    setFeedback("已取消本次操作");
  });
}

function hydrateForm(config) {
  if (!config) {
    return;
  }
  fields.symbols.value = config.symbols_text ?? (config.symbols ?? []).join(",");
  fields.windowsMinutes.value = (config.windows_minutes ?? [5, 10]).join(",");
  fields.reportInterval.value = config.report_interval ?? 10;
  fields.baselineMinutes.value = config.baseline_minutes ?? 120;
  fields.runtimeSeconds.value = config.runtime_seconds ?? 0;
}

function formPayload() {
  return {
    symbols: fields.symbols.value.trim().toUpperCase(),
    windows_minutes: fields.windowsMinutes.value.trim(),
    report_interval: Number(fields.reportInterval.value),
    baseline_minutes: Number(fields.baselineMinutes.value),
    runtime_seconds: Number(fields.runtimeSeconds.value),
  };
}

async function postJson(url, payload) {
  const response = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  const text = await response.text();
  let data;
  try {
    data = text ? JSON.parse(text) : {};
  } catch (_error) {
    const err = new Error("服务器返回了非 JSON 响应，可能仍连接到旧版服务");
    err.code = "NON_JSON";
    err.status = response.status;
    err.raw = text;
    throw err;
  }
  if (!response.ok) {
    const err = new Error(data.error || `HTTP ${response.status}`);
    err.code = "HTTP";
    err.status = response.status;
    throw err;
  }
  return data;
}

function splitSymbolsText(value) {
  return String(value || "")
    .toUpperCase()
    .split(/[\s,，;；]+/)
    .map((item) => item.trim())
    .filter(Boolean);
}

function currentConfigPayload(symbolsOverride) {
  const config = latestState?.config || {};
  const symbols = symbolsOverride ?? config.symbols ?? splitSymbolsText(fields.symbols.value);
  const windows = config.windows_minutes ?? splitSymbolsText(fields.windowsMinutes.value);
  return {
    symbols: symbols.join(","),
    windows_minutes: Array.isArray(windows) ? windows.join(",") : String(windows || fields.windowsMinutes.value).trim(),
    report_interval: Number(config.report_interval ?? fields.reportInterval.value),
    baseline_minutes: Number(config.baseline_minutes ?? fields.baselineMinutes.value),
    runtime_seconds: Number(config.runtime_seconds ?? fields.runtimeSeconds.value),
  };
}

async function restartMonitorWithSymbols(symbols) {
  if (!symbols.length) {
    return postJson("/api/stop", {});
  }
  return postJson("/api/start", currentConfigPayload(symbols));
}

async function syncMonitorWithSymbols(symbols) {
  if (!symbols.length) {
    return postJson("/api/stop", {});
  }
  return postJson("/api/sync-symbols", { symbols: symbols.join(",") });
}

function shouldFallbackToRestart(error) {
  return error?.code === "NON_JSON" || error?.status === 404 || error?.status === 405;
}

function renderMode(state) {
  latestState = state;
  document.body.dataset.mode = state.mode || "idle";
  modeText.textContent = state.message || stateTextMap[state.mode] || "等待启动";

  const active = Boolean(state.is_active);
  startButton.disabled = isStartPending || (active && state.mode !== "error");
  addButton.disabled = isAddPending;
  removeButton.disabled = isRemovePending;
  stopButton.disabled = isStopPending || !active;
}

function renderSummary(state) {
  const activeSymbol = ensureSelectedSymbol(state);
  const overview = state.overview || {};
  const sessions = state.sessions || [];

  headlinePanel.dataset.level = overview.level || "warming";
  overallHeadline.textContent = overview.headline || "等待启动";
  overallSummary.textContent = overview.summary || "启动监控后，这里会汇总当前最强的币种信号。";
  overallStrongestSymbol.textContent = overview.strongest_symbol ? `最强币种 ${overview.strongest_symbol}` : "最强币种 -";
  overallWindow.textContent = overview.strongest_window ? `窗口 ${overview.strongest_window}` : "窗口 -";
  overallPhase.textContent = overview.phase_text || "等待启动";
  if (versionInfo) {
    versionInfo.textContent = `前端 ${FRONTEND_VERSION} / 服务 ${state.app_version || "-"}`;
  }

  summaryFields.symbolCount.textContent = String(overview.symbol_count ?? sessions.length ?? 0);
  summaryFields.readyCount.textContent = String(overview.ready_count ?? 0);
  summaryFields.strongestSymbol.textContent = overview.strongest_symbol || "-";
  summaryFields.strongestWindow.textContent = overview.strongest_window || "-";
  summaryFields.updatedAt.textContent = state.last_update_at_text || "-";
  if (selectedSymbolLabel) {
    selectedSymbolLabel.textContent = activeSymbol ? `当前查看币种 ${activeSymbol}` : "当前查看币种 -";
  }
}

function renderSymbolOverview(state) {
  const sessions = state.sessions || [];
  const activeSymbol = ensureSelectedSymbol(state);
  if (!sessions.length) {
    symbolOverviewGrid.innerHTML = '<article class="empty-card">启动后在这里显示每个币种的一句话结论。</article>';
    return;
  }

  symbolOverviewGrid.innerHTML = sessions.map((session) => {
    const snapshot = session.snapshot;
    const overall = snapshot?.overall;
    const market = snapshot?.market;
    const depth = snapshot?.depth;
    const quoteAsset = snapshot?.symbol_meta?.quote_asset || "USDT";
    const line1 = overall?.headline || "正在准备数据";
    const line2 = overall?.summary || "等待首个快照。";
    const meta1 = market?.mid ? `中间价 ${Number(market.mid).toFixed(8)}` : "中间价 -";
    const meta2 = depth ? `最小深度 ${depth.depth_min_quote_text} ${quoteAsset}` : "最小深度 -";
    const actionHtml = pendingCardRemovalSymbol === session.symbol
      ? `
          <div class="symbol-actions">
            <button type="button" class="btn btn-confirm" data-confirm-remove-symbol="${escapeHtml(session.symbol)}">确定移除</button>
            <button type="button" class="btn btn-ghost" data-cancel-remove-symbol="${escapeHtml(session.symbol)}">取消</button>
          </div>
        `
      : `<button type="button" class="symbol-remove" data-remove-symbol="${escapeHtml(session.symbol)}">移除</button>`;
    return `
      <article class="symbol-overview-card" data-symbol-card="${escapeHtml(session.symbol)}" data-selected="${session.symbol === activeSymbol ? "true" : "false"}">
        <div class="symbol-card-head">
          <div>
            <div class="summary-label">币种</div>
            <h3>${escapeHtml(session.symbol)}</h3>
          </div>
          ${actionHtml}
        </div>
        <p>${escapeHtml(line1)}</p>
        <p>${escapeHtml(line2)}</p>
        <div class="evidence-strip">
          <span class="evidence-chip">${escapeHtml(meta1)}</span>
          <span class="evidence-chip">${escapeHtml(meta2)}</span>
        </div>
      </article>
    `;
  }).join("");

  const removeButtons = symbolOverviewGrid.querySelectorAll("[data-remove-symbol]");
  removeButtons.forEach((button) => {
    button.addEventListener("click", (event) => {
      event.stopPropagation();
      const symbol = button.dataset.removeSymbol;
      if (!symbol) {
        return;
      }
      pendingCardRemovalSymbol = symbol;
      renderSymbolOverview(latestState || { sessions: [] });
      setFeedback(`请确认是否移除 ${symbol}`);
    });
  });

  const symbolCards = symbolOverviewGrid.querySelectorAll("[data-symbol-card]");
  symbolCards.forEach((card) => {
    card.addEventListener("click", () => {
      const symbol = card.dataset.symbolCard;
      if (!symbol) {
        return;
      }
      selectedSymbol = symbol;
      renderSummary(latestState || { sessions: [] });
      renderSymbolOverview(latestState || { sessions: [] });
      renderWindows(latestState || { sessions: [] });
    });
  });

  const confirmButtons = symbolOverviewGrid.querySelectorAll("[data-confirm-remove-symbol]");
  confirmButtons.forEach((button) => {
    button.addEventListener("click", async (event) => {
      event.stopPropagation();
      const symbol = button.dataset.confirmRemoveSymbol;
      if (!symbol) {
        return;
      }
      await performRemoveSymbols([symbol]);
    });
  });

  const cancelButtons = symbolOverviewGrid.querySelectorAll("[data-cancel-remove-symbol]");
  cancelButtons.forEach((button) => {
    button.addEventListener("click", (event) => {
      event.stopPropagation();
      pendingCardRemovalSymbol = null;
      renderSymbolOverview(latestState || { sessions: [] });
      setFeedback("已取消本次移除");
    });
  });
}

function collectWindowItems(state) {
  const sessions = state.sessions || [];
  const windows = [];
  const activeSymbol = ensureSelectedSymbol(state);
  sessions.forEach((session, sessionIndex) => {
    if (activeSymbol && session.symbol !== activeSymbol) {
      return;
    }
    const snapshot = session.snapshot;
    if (!snapshot) {
      return;
    }
    const quoteAsset = snapshot?.symbol_meta?.quote_asset || "USDT";
    (snapshot.windows || []).forEach((item, windowIndex) => {
      windows.push({
        ...item,
        symbol: session.symbol,
        quoteAsset,
        orderKey: `${String(sessionIndex).padStart(4, "0")}-${String(windowIndex).padStart(4, "0")}`,
      });
    });
  });
  windows.sort((left, right) => left.orderKey.localeCompare(right.orderKey));
  return windows;
}

function renderWindows(state) {
  const activeSymbol = ensureSelectedSymbol(state);
  const windows = collectWindowItems(state);
  if (!windows.length) {
    expandedEvidenceWindows.clear();
    const symbolText = activeSymbol ? `${activeSymbol} 暂无窗口数据。` : "启动后在这里显示当前选中币种的窗口实时状态。";
    windowGrid.innerHTML = `<article class="empty-card">${escapeHtml(symbolText)}</article>`;
    return;
  }

  const activeWindowKeys = new Set(windows.map((item) => `${item.symbol}::${item.window_label}`));
  for (const key of [...expandedEvidenceWindows]) {
    if (!activeWindowKeys.has(key)) {
      expandedEvidenceWindows.delete(key);
    }
  }

  windowGrid.innerHTML = windows.map((item) => {
    const warmupWidth = Math.max(0, Math.min(100, (item.warmup_progress || 0) * 100));
    const hint = stateHintMap[item.state] || "";
    const judgement = item.judgement || {};
    const axes = judgement.axes || [];
    const quoteAsset = item.quoteAsset || "USDT";
    const windowKey = `${item.symbol}::${item.window_label}`;
    const isExpanded = expandedEvidenceWindows.has(windowKey);
    return `
      <article class="window-card" data-state="${escapeHtml(item.state)}" data-level="${escapeHtml(judgement.level || 'warming')}">
        <header>
          <div class="window-title-wrap">
            <span class="window-symbol">${escapeHtml(item.symbol)}</span>
            <div class="window-meta">持续窗口</div>
            <h3>${escapeHtml(item.window_label)}</h3>
          </div>
          <span class="state-chip">${escapeHtml(judgement.headline || item.state)}</span>
        </header>

        <p class="window-summary">${escapeHtml(judgement.summary || hint)}</p>

        <div class="axis-grid">
          ${axes.map((axis) => `
            <article class="axis-card" data-axis-status="${escapeHtml(axis.status)}">
              <span class="metric-label">${escapeHtml(axis.label)}</span>
              <strong>${escapeHtml(judgementStatusTextMap[axis.status] || axis.status)}</strong>
              <p>${escapeHtml(axis.summary)}</p>
            </article>
          `).join("")}
        </div>

        <div class="evidence-strip">
          <span class="evidence-chip">窗口分数 ${Number(item.score || 0).toFixed(1)}</span>
          <span class="evidence-chip">成交额 ${escapeHtml(item.total_quote_text)} ${escapeHtml(quoteAsset)}</span>
          <span class="evidence-chip">持续 ${escapeHtml(item.streak_text)}</span>
          <span class="evidence-chip">状态 ${escapeHtml(item.state)}</span>
        </div>

        <div class="warmup-wrap">
          <div class="window-meta">${escapeHtml(hint)}</div>
          <div class="warmup-bar"><span style="width: ${warmupWidth}%;"></span></div>
        </div>

        <details class="details-box" data-window-key="${escapeHtml(windowKey)}" ${isExpanded ? "open" : ""}>
          <summary>次级证据</summary>
          <div class="details-grid">
            <div class="metric">
              <span class="metric-label">主动买 / 卖</span>
              <strong>${escapeHtml(item.buy_quote_text)} / ${escapeHtml(item.sell_quote_text)}</strong>
            </div>
            <div class="metric">
              <span class="metric-label">成交额 / 时间加权深度</span>
              <strong>${escapeHtml(item.vol_to_twap_depth_ratio_text)}</strong>
            </div>
            <div class="metric">
              <span class="metric-label">时间加权深度</span>
              <strong>${escapeHtml(item.twap_depth_quote_text)} ${escapeHtml(quoteAsset)}</strong>
            </div>
            <div class="metric">
              <span class="metric-label">时间加权滑点</span>
              <strong>${escapeHtml(item.twap_slippage_text)}</strong>
            </div>
            <div class="metric">
              <span class="metric-label">成交间隔 CV</span>
              <strong>${escapeHtml(item.interval_cv_text)}</strong>
            </div>
            <div class="metric">
              <span class="metric-label">方向切换率</span>
              <strong>${Number(item.switch_rate || 0).toFixed(2)}</strong>
            </div>
            <div class="metric">
              <span class="metric-label">固定手数 Top1 / Top5</span>
              <strong>${escapeHtml(item.top_size_repeat_text)} / ${escapeHtml(item.top5_size_repeat_text)}</strong>
            </div>
            <div class="metric">
              <span class="metric-label">固定名义金额 Top1</span>
              <strong>${escapeHtml(item.top_notional_repeat_text)} (${escapeHtml(item.top_notional_bucket)} ${escapeHtml(quoteAsset)})</strong>
            </div>
            <div class="metric">
              <span class="metric-label">价格位移</span>
              <strong>${escapeHtml(item.price_return_text)}</strong>
            </div>
            <div class="metric">
              <span class="metric-label">盘口更新 / 补回</span>
              <strong>${Number(item.churn_per_second || 0).toFixed(2)} / ${Number(item.refill_per_minute || 0).toFixed(2)}</strong>
            </div>
          </div>
        </details>
      </article>
    `;
  }).join("");

  const detailsNodes = windowGrid.querySelectorAll(".details-box");
  detailsNodes.forEach((node) => {
    const key = node.dataset.windowKey;
    node.addEventListener("toggle", () => {
      if (!key) {
        return;
      }
      if (node.open) {
        expandedEvidenceWindows.add(key);
      } else {
        expandedEvidenceWindows.delete(key);
      }
    });
  });
}

function renderLogs(state) {
  const logs = state.logs || [];
  logOutput.textContent = logs.length ? logs.join("\n\n") : "等待日志输出。";
  logOutput.scrollTop = logOutput.scrollHeight;
}

function renderState(state) {
  renderMode(state);
  renderSummary(state);
  renderSymbolOverview(state);
  renderWindows(state);
  renderLogs(state);

  if (!hasHydratedForm) {
    hydrateForm(state.config);
    hasHydratedForm = true;
  }

  if (state.last_error) {
    setFeedback(state.last_error, true);
    return;
  }

  const message = state.message || stateTextMap[state.mode] || "等待启动";
  setFeedback(message, state.mode === "error");
}

async function loadState() {
  try {
    const response = await fetch("/api/state", { cache: "no-store" });
    const state = await response.json();
    renderState(state);
    return state;
  } catch (error) {
    setFeedback(`读取状态失败: ${error.message}`, true);
    return null;
  }
}

async function performAddSymbols(newSymbols) {
  if (!newSymbols.length) {
    setFeedback("请先输入要新增的币种", true);
    return;
  }
  isAddPending = true;
  renderMode(latestState || { mode: document.body.dataset.mode || "idle", is_active: false });
  setFeedback("正在更新监控币种...");
  try {
    let data;
    const freshState = (await loadState()) || latestState;
    const currentSymbols = freshState?.config?.symbols || splitSymbolsText(fields.symbols.value);
    const merged = new Set(currentSymbols);
    newSymbols.forEach((item) => merged.add(item));
    data = await syncMonitorWithSymbols(Array.from(merged));
    hydrateForm(data.state?.config);
    setFeedback(`已更新监控币种，共 ${data.state?.config?.symbols?.length || 0} 个币种`);
    manageSymbolsInput.value = "";
  } catch (error) {
    setFeedback(`添加失败: ${error.message}`, true);
  } finally {
    isAddPending = false;
    await loadState();
  }
}

async function performRemoveSymbols(removeSymbols) {
  if (!removeSymbols.length) {
    setFeedback("请先输入要移除的币种", true);
    return;
  }
  isRemovePending = true;
  renderMode(latestState || { mode: document.body.dataset.mode || "idle", is_active: false });
  setFeedback("正在更新监控币种...");
  try {
    if (latestState?.can_manage_symbols) {
      const freshState = (await loadState()) || latestState;
      const current = freshState?.config?.symbols || [];
      const removeSet = new Set(removeSymbols);
      const nextSymbols = current.filter((item) => !removeSet.has(item));
      const removed = current.filter((item) => removeSet.has(item));
      const missing = removeSymbols.filter((item) => !current.includes(item));
      const data = await syncMonitorWithSymbols(nextSymbols);
      hydrateForm(data.state?.config);
      if (removed.length) {
        setFeedback(`已移除 ${removed.join(", ")}${missing.length ? `；未找到 ${missing.join(", ")}` : ""}`);
        manageSymbolsInput.value = "";
      } else if (missing.length) {
        setFeedback(`这些币种当前不在监控中：${missing.join(", ")}`);
      } else {
        setFeedback("没有移除任何币种");
      }
    } else {
      const removeSet = new Set(removeSymbols);
      const current = splitSymbolsText(fields.symbols.value);
      const next = current.filter((item) => !removeSet.has(item));
      fields.symbols.value = next.join(",");
      setFeedback("已从启动配置里移除输入币种，尚未启动监控");
      manageSymbolsInput.value = "";
    }
  } catch (error) {
    setFeedback(`移除失败: ${error.message}`, true);
  } finally {
    isRemovePending = false;
    pendingCardRemovalSymbol = null;
    await loadState();
  }
}

form.addEventListener("submit", async (event) => {
  event.preventDefault();
  isStartPending = true;
  renderMode(latestState || { mode: document.body.dataset.mode || "idle", is_active: false });
  setFeedback("正在提交配置并启动监控...");
  try {
    const data = await postJson("/api/start", formPayload());
    hydrateForm(data.state?.config);
    const count = data.state?.config?.symbols?.length || 0;
    setFeedback(`已启动 ${count} 个币种`);
    await loadState();
  } catch (error) {
    setFeedback(`启动失败: ${error.message}`, true);
  } finally {
    isStartPending = false;
    await loadState();
  }
});

addButton.addEventListener("click", () => {
  const newSymbols = splitSymbolsText(manageSymbolsInput.value);
  if (!newSymbols.length) {
    setFeedback("请先输入要新增的币种", true);
    return;
  }
  pendingManageAction = { type: "add", symbols: newSymbols };
  renderManageConfirm();
  setFeedback(`请确认是否添加 ${newSymbols.join(", ")}`);
});

removeButton.addEventListener("click", () => {
  const removeSymbols = splitSymbolsText(manageSymbolsInput.value);
  if (!removeSymbols.length) {
    setFeedback("请先输入要移除的币种", true);
    return;
  }
  pendingManageAction = { type: "remove", symbols: removeSymbols };
  renderManageConfirm();
  setFeedback(`请确认是否移除 ${removeSymbols.join(", ")}`);
});

stopButton.addEventListener("click", async () => {
  isStopPending = true;
  renderMode(latestState || { mode: document.body.dataset.mode || "idle", is_active: true });
  setFeedback("正在请求停止...");
  try {
    await postJson("/api/stop", {});
  } catch (error) {
    setFeedback(`停止失败: ${error.message}`, true);
  } finally {
    isStopPending = false;
    await loadState();
  }
});

async function boot() {
  await loadState();
  pollTimer = window.setInterval(loadState, 2000);
}

boot();
