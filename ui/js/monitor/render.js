import { escapeHtml } from "../shared/text.js";
import { FRONTEND_VERSION, judgementStatusTextMap, stateHintMap, stateTextMap } from "./constants.js";
import { monitorDom } from "./dom.js";
import { ensureSelectedSymbol, hydrateForm, monitorUiState } from "./state.js";

export function setFeedback(message, isError = false) {
  monitorDom.feedback.textContent = message;
  monitorDom.feedback.classList.toggle("error", isError);
}

export function renderManageConfirm() {
  if (!monitorUiState.pendingManageAction) {
    monitorDom.manageConfirmBar.hidden = true;
    monitorDom.manageConfirmBar.innerHTML = "";
    return;
  }

  const actionLabel = monitorUiState.pendingManageAction.type === "add" ? "添加" : "移除";
  monitorDom.manageConfirmBar.hidden = false;
  monitorDom.manageConfirmBar.innerHTML = `
    <div class="confirm-text">确认要${actionLabel}这些币种吗：${escapeHtml(monitorUiState.pendingManageAction.symbols.join(", "))}</div>
    <div class="confirm-actions">
      <button type="button" class="btn btn-confirm" data-confirm-manage="${escapeHtml(monitorUiState.pendingManageAction.type)}">确定${actionLabel}</button>
      <button type="button" class="btn btn-ghost" data-cancel-manage="1">取消</button>
    </div>
  `;
}

export function renderMode(state) {
  document.body.dataset.mode = state.mode || "idle";
  monitorDom.modeText.textContent = state.message || stateTextMap[state.mode] || "等待启动";

  const active = Boolean(state.is_active);
  monitorDom.startButton.disabled = monitorUiState.isStartPending || (active && state.mode !== "error");
  monitorDom.addButton.disabled = monitorUiState.isAddPending || !state.can_manage_symbols;
  monitorDom.removeButton.disabled = monitorUiState.isRemovePending || !state.can_manage_symbols;
  monitorDom.stopButton.disabled = monitorUiState.isStopPending || !active;
}

export function renderSummary(state) {
  const activeSymbol = ensureSelectedSymbol(state);
  const overview = state.overview || {};
  const sessions = state.sessions || [];

  monitorDom.headlinePanel.dataset.level = overview.level || "warming";
  monitorDom.overallHeadline.textContent = overview.headline || "等待启动";
  monitorDom.overallSummary.textContent = overview.summary || "启动监控后，这里会汇总当前最强的币种信号。";
  monitorDom.overallStrongestSymbol.textContent = overview.strongest_symbol ? `最强币种 ${overview.strongest_symbol}` : "最强币种 -";
  monitorDom.overallWindow.textContent = overview.strongest_window ? `窗口 ${overview.strongest_window}` : "窗口 -";
  monitorDom.overallPhase.textContent = overview.phase_text || "等待启动";
  if (monitorDom.versionInfo) {
    monitorDom.versionInfo.textContent = `前端 ${FRONTEND_VERSION} / 服务 ${state.app_version || "-"}`;
  }

  monitorDom.summaryFields.symbolCount.textContent = String(overview.symbol_count ?? sessions.length ?? 0);
  monitorDom.summaryFields.readyCount.textContent = String(overview.ready_count ?? 0);
  monitorDom.summaryFields.strongestSymbol.textContent = overview.strongest_symbol || "-";
  monitorDom.summaryFields.strongestWindow.textContent = overview.strongest_window || "-";
  monitorDom.summaryFields.updatedAt.textContent = state.last_update_at_text || "-";
  if (monitorDom.selectedSymbolLabel) {
    monitorDom.selectedSymbolLabel.textContent = activeSymbol ? `当前查看币种 ${activeSymbol}` : "当前查看币种 -";
  }
}

export function renderSymbolOverview(state) {
  const sessions = state.sessions || [];
  const activeSymbol = ensureSelectedSymbol(state);
  if (!sessions.length) {
    monitorUiState.pendingCardRemovalSymbol = null;
    monitorDom.symbolOverviewGrid.innerHTML = '<article class="empty-card">启动后在这里显示每个币种的一句话结论。</article>';
    return;
  }

  if (
    monitorUiState.pendingCardRemovalSymbol &&
    !sessions.some((session) => session.symbol === monitorUiState.pendingCardRemovalSymbol)
  ) {
    monitorUiState.pendingCardRemovalSymbol = null;
  }

  monitorDom.symbolOverviewGrid.innerHTML = sessions.map((session) => {
    const snapshot = session.snapshot;
    const overall = snapshot?.overall;
    const market = snapshot?.market;
    const depth = snapshot?.depth;
    const quoteAsset = snapshot?.symbol_meta?.quote_asset || "USDT";
    const line1 = overall?.headline || "正在准备数据";
    const line2 = overall?.summary || "等待首个快照。";
    const meta1 = market?.mid ? `中间价 ${Number(market.mid).toFixed(8)}` : "中间价 -";
    const meta2 = depth ? `最小深度 ${depth.depth_min_quote_text} ${quoteAsset}` : "最小深度 -";
    const actionHtml = monitorUiState.pendingCardRemovalSymbol === session.symbol
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
}

function collectWindowItems(state) {
  const sessions = state.sessions || [];
  const activeSymbol = ensureSelectedSymbol(state);
  const windows = [];

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

export function renderWindows(state) {
  const activeSymbol = ensureSelectedSymbol(state);
  const windows = collectWindowItems(state);
  if (!windows.length) {
    monitorUiState.expandedEvidenceWindows.clear();
    const symbolText = activeSymbol ? `${activeSymbol} 暂无窗口数据。` : "启动后在这里显示当前选中币种的窗口实时状态。";
    monitorDom.windowGrid.innerHTML = `<article class="empty-card">${escapeHtml(symbolText)}</article>`;
    return;
  }

  const activeWindowKeys = new Set(windows.map((item) => `${item.symbol}::${item.window_label}`));
  for (const key of [...monitorUiState.expandedEvidenceWindows]) {
    if (!activeWindowKeys.has(key)) {
      monitorUiState.expandedEvidenceWindows.delete(key);
    }
  }

  monitorDom.windowGrid.innerHTML = windows.map((item) => {
    const warmupWidth = Math.max(0, Math.min(100, (item.warmup_progress || 0) * 100));
    const hint = stateHintMap[item.state] || "";
    const judgement = item.judgement || {};
    const axes = judgement.axes || [];
    const quoteAsset = item.quoteAsset || "USDT";
    const windowKey = `${item.symbol}::${item.window_label}`;
    const isExpanded = monitorUiState.expandedEvidenceWindows.has(windowKey);
    return `
      <article class="window-card" data-state="${escapeHtml(item.state)}" data-level="${escapeHtml(judgement.level || "warming")}">
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
}

export function renderLogs(state) {
  const logs = state.logs || [];
  monitorDom.logOutput.textContent = logs.length ? logs.join("\n\n") : "等待日志输出。";
  monitorDom.logOutput.scrollTop = monitorDom.logOutput.scrollHeight;
}

export function renderState(state) {
  monitorUiState.latestState = state;
  renderMode(state);
  renderSummary(state);
  renderSymbolOverview(state);
  renderWindows(state);
  renderLogs(state);
  renderManageConfirm();

  if (!monitorUiState.hasHydratedForm) {
    hydrateForm(state.config, monitorDom.fields);
    monitorUiState.hasHydratedForm = true;
  }

  if (state.last_error) {
    setFeedback(state.last_error, true);
    return;
  }

  const message = state.message || stateTextMap[state.mode] || "等待启动";
  setFeedback(message, state.mode === "error");
}
