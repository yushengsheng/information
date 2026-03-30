import { escapeHtml } from "../shared/text.js";
import { intelDom } from "./dom.js";
import { intelUiState, laneMetaMap, normalizeSections, sectionOrder, setConfigCollapsed, setSelectedLane } from "./state.js";

export function setFeedback(message, isError = false) {
  intelDom.feedback.textContent = message;
  intelDom.feedback.classList.toggle("error", isError);
}

export function setStatus(installed, connected, text) {
  intelDom.statusText.textContent = text;
  if (!installed) {
    intelDom.statusDot.style.background = "#ff6f61";
    return;
  }
  intelDom.statusDot.style.background = connected ? "#6fe6a6" : "#ffd166";
}

export function setStatusPending(text = "检查中...") {
  intelDom.statusText.textContent = text;
  intelDom.statusDot.style.background = "#ffd166";
}

function buildPersistedSentItems() {
  const items = [];
  for (const laneKey of sectionOrder) {
    const lane = laneMetaMap[laneKey];
    const sectionItems = Array.isArray(intelUiState.persistedSections?.[laneKey]) ? intelUiState.persistedSections[laneKey] : [];
    for (const item of sectionItems) {
      items.push({
        ...item,
        __laneKey: laneKey,
        __laneTitle: lane?.title || laneKey,
        __laneKicker: lane?.kicker || laneKey,
      });
    }
  }
  return items;
}

function getFocusedItems(laneKey) {
  if (laneKey === "sent") {
    return buildPersistedSentItems();
  }
  return Array.isArray(intelUiState.latestSections?.[laneKey]) ? intelUiState.latestSections[laneKey] : [];
}

export function formatSecondsFromMs(value) {
  const ms = Number(value || 0);
  if (!Number.isFinite(ms) || ms <= 0) {
    return "-";
  }
  return `${(ms / 1000).toFixed(ms >= 10000 ? 1 : 2)} 秒`;
}

export function renderServiceVersion(appVersion, digestVersion = null) {
  if (digestVersion && digestVersion !== appVersion) {
    intelDom.summaryVersion.textContent = `${appVersion} / 日报 ${digestVersion}`;
    return;
  }
  intelDom.summaryVersion.textContent = appVersion || "-";
}

export function renderBuildMeta(stats) {
  const timings = stats?.timings_ms || {};
  const raw = stats?.raw_counts || {};
  const selected = stats?.selected_counts || {};
  const summary = stats?.summary || {};
  if (!Object.keys(timings).length) {
    intelDom.digestBuildMeta.textContent = "最近一次生成 -";
    intelDom.digestSelectionMeta.textContent = "当前规则诊断 -";
    return;
  }
  const rawTotal = Number(raw.crypto || 0) + Number(raw.world || 0) + Number(raw.hot || 0) + Number(raw.custom || 0);
  const selectedTotal =
    Number(selected.crypto || 0)
    + Number(selected.world || 0)
    + Number(selected.persistent || 0)
    + Number(selected.hot || 0)
    + Number(selected.custom || 0);
  const summaryLabel = summary.used_ai
    ? `摘要 AI ${summary.provider || "-"} / ${summary.model || "-"}`
    : "摘要 回退";
  intelDom.digestBuildMeta.textContent = `最近一次生成 ${formatSecondsFromMs(timings.total)}，候选 ${rawTotal} 条，入选 ${selectedTotal} 条，${summaryLabel}`;

  const overall = stats?.selection_diagnostics?.overall || {};
  const overallTotal = Number(overall.total || 0);
  const overallEvents = Number(overall.event_matched || 0);
  const overallConfirmed = Number(overall.confirmed || 0);
  const avgAuthority = Number(overall.avg_authority || 0);
  const sourceMix = Object.entries(overall.source_counts || {})
    .filter(([, count]) => Number(count || 0) > 0)
    .map(([source, count]) => `${String(source || "").toUpperCase()} ${count}`)
    .join(" / ");
  const topEvents = Array.isArray(overall.top_events)
    ? overall.top_events
      .slice(0, 3)
      .map((item) => `${item.label} ${item.count}`)
      .join("、")
    : "";
  intelDom.digestSelectionMeta.textContent = overallTotal
    ? `当前规则：事件化 ${overallEvents}/${overallTotal}，已确认 ${overallConfirmed}/${overallTotal}，平均权威 ${avgAuthority ? avgAuthority.toFixed(1) : "-"}，来源 ${sourceMix || "-"}${topEvents ? `，主事件 ${topEvents}` : ""}`
    : "当前规则诊断 -";
}

export function renderDigestDateOptions(availableDates, selectedDate, { includeLatest = true } = {}) {
  const normalizedDates = Array.isArray(availableDates) ? availableDates.filter(Boolean) : [];
  const optionItems = [];
  if (includeLatest) {
    optionItems.push('<option value="">最新</option>');
  }
  optionItems.push(...normalizedDates.map((date) => `<option value="${escapeHtml(date)}">${escapeHtml(date)}</option>`));

  if (!optionItems.length) {
    intelDom.digestDateSelect.innerHTML = '<option value="">暂无历史</option>';
    intelDom.digestDateSelect.disabled = true;
    return;
  }

  intelDom.digestDateSelect.disabled = false;
  intelDom.digestDateSelect.innerHTML = optionItems.join("");

  const activeDate = includeLatest && selectedDate === ""
    ? ""
    : (normalizedDates.includes(selectedDate) ? selectedDate : (includeLatest ? "" : normalizedDates[0]));
  intelDom.digestDateSelect.value = activeDate;
}

function buildItemLinks(item) {
  const url = item.url ? String(item.url) : "";
  const externalUrl = item.external_url ? String(item.external_url) : "";
  const links = [];

  if (url) {
    links.push(`<a class="switch-link" href="${escapeHtml(url)}" target="_blank" rel="noopener noreferrer">原文</a>`);
  }
  if (externalUrl && externalUrl !== url) {
    links.push(`<a class="switch-link" href="${escapeHtml(externalUrl)}" target="_blank" rel="noopener noreferrer">外链</a>`);
  }
  if (!links.length) {
    links.push('<span class="evidence-chip">无链接</span>');
  }
  return links.join("");
}

function buildItemMetaChips(item) {
  const chips = [];
  if (item.__laneTitle) {
    chips.push(`<span class="evidence-chip">板块 ${escapeHtml(item.__laneTitle)}</span>`);
  }
  const source = escapeHtml((item.source || "").toUpperCase() || "SRC");
  const author = escapeHtml(item.author || "-");
  const summaryMode = item.summary_mode === "ai" ? "AI 摘要" : "规则摘要";
  chips.push(`<span class="evidence-chip">来源 ${source}</span>`);
  if (item.source_role_label) {
    chips.push(`<span class="evidence-chip">${escapeHtml(item.source_role_label)}</span>`);
  }
  chips.push(`<span class="evidence-chip">@${author}</span>`);
  chips.push(`<span class="evidence-chip">${summaryMode}</span>`);

  if (item.cluster_size) {
    chips.push(`<span class="evidence-chip">同事件 ${escapeHtml(String(item.cluster_size))} 条</span>`);
  }
  if (item.source_trust_tier) {
    chips.push(`<span class="evidence-chip">可信 ${escapeHtml(String(item.source_trust_tier))} 级</span>`);
  }
  if (item.source_domain) {
    chips.push(`<span class="evidence-chip">${escapeHtml(item.source_domain)}</span>`);
  }
  return chips.join("");
}

function buildDecisionChips(item) {
  const chips = Array.isArray(item.selection_reason_chips) ? item.selection_reason_chips.filter(Boolean) : [];
  if (!chips.length) {
    return '<span class="evidence-chip">常规优先级</span>';
  }
  return chips.map((chip) => `<span class="evidence-chip intel-decision-chip">${escapeHtml(String(chip))}</span>`).join("");
}

function buildDecisionDetails(item) {
  const details = [];
  if (item.topic_event_label) {
    details.push(`<span class="evidence-chip">事件类型 ${escapeHtml(item.topic_event_label)}</span>`);
  }
  if (Array.isArray(item.topic_event_entity_hits) && item.topic_event_entity_hits.length) {
    details.push(`<span class="evidence-chip">实体 ${escapeHtml(item.topic_event_entity_hits.slice(0, 4).join(" / "))}</span>`);
  }
  if (Array.isArray(item.topic_event_action_hits) && item.topic_event_action_hits.length) {
    details.push(`<span class="evidence-chip">动作 ${escapeHtml(item.topic_event_action_hits.slice(0, 4).join(" / "))}</span>`);
  }
  if (Array.isArray(item.topic_event_evidence_hits) && item.topic_event_evidence_hits.length) {
    details.push(`<span class="evidence-chip">证据 ${escapeHtml(item.topic_event_evidence_hits.slice(0, 4).join(" / "))}</span>`);
  }
  if (!details.length) {
    return "";
  }
  return `
    <details class="details-box intel-item-details">
      <summary>查看命中证据</summary>
      <div class="evidence-strip intel-item-detail-strip">
        ${details.join("")}
      </div>
    </details>
  `;
}

function renderFocusedLaneDiagnostics(lane, items) {
  if (!items.length) {
    intelDom.focusLaneSignalText.textContent = `${lane.title} 当前还没有可解释的入选结果。`;
    intelDom.focusLaneSignalChips.innerHTML = '<span class="evidence-chip">等待结果</span>';
    return;
  }

  const eventMatched = items.filter((item) => Boolean(item.topic_event_label)).length;
  const confirmed = items.filter((item) => Number(item.event_confirmation_count || 0) >= 1).length;
  const xLead = items.filter((item) => String(item.source || "").toLowerCase() === "x").length;
  const avgAuthorityValues = items
    .map((item) => Number(item.event_authority_score || item.source_authority_score || 0))
    .filter((value) => Number.isFinite(value) && value > 0);
  const avgAuthority = avgAuthorityValues.length
    ? (avgAuthorityValues.reduce((sum, value) => sum + value, 0) / avgAuthorityValues.length)
    : 0;
  const topEventMap = new Map();
  for (const item of items) {
    const label = String(item.topic_event_label || "").trim();
    if (!label) {
      continue;
    }
    topEventMap.set(label, (topEventMap.get(label) || 0) + 1);
  }
  const topEvents = Array.from(topEventMap.entries())
    .sort((a, b) => b[1] - a[1])
    .slice(0, 3);

  intelDom.focusLaneSignalText.textContent = `${lane.title} 共 ${items.length} 条，事件化 ${eventMatched} 条，已确认 ${confirmed} 条，X 代表 ${xLead} 条，平均权威 ${avgAuthority ? avgAuthority.toFixed(1) : "-"}`;

  const chips = [
    `<span class="evidence-chip">事件化 ${eventMatched}/${items.length}</span>`,
    `<span class="evidence-chip">已确认 ${confirmed}/${items.length}</span>`,
    `<span class="evidence-chip">X 主源 ${xLead}</span>`,
  ];
  for (const [label, count] of topEvents) {
    chips.push(`<span class="evidence-chip intel-signal-chip">${escapeHtml(label)} ${escapeHtml(String(count))}</span>`);
  }
  intelDom.focusLaneSignalChips.innerHTML = chips.join("");
}

function renderFocusedLane() {
  const lane = laneMetaMap[intelUiState.selectedLane] || laneMetaMap.crypto;
  const items = getFocusedItems(lane.key);

  intelDom.focusLaneTitle.textContent = lane.focusTitle;
  intelDom.focusLaneDescription.textContent = lane.focusDescription;
  intelDom.focusLaneKicker.textContent = lane.kicker;
  intelDom.focusLaneCount.textContent = `${items.length} 条`;
  renderFocusedLaneDiagnostics(lane, items);

  if (!items.length) {
    intelDom.focusedList.innerHTML = `<article class="empty-card">${escapeHtml(lane.empty)}</article>`;
    return;
  }

  intelDom.focusedList.innerHTML = items.map((item, index) => {
    const source = escapeHtml((item.source || "").toUpperCase() || "SRC");
    const text = escapeHtml(item.summary_text || item.text || "");
    const createdAt = item.created_at ? `<span class="evidence-chip">${escapeHtml(item.created_at)}</span>` : "";
    const rank = String(index + 1).padStart(2, "0");
    const laneKicker = escapeHtml(item.__laneKicker || lane.kicker);
    return `
      <article class="intel-item-card">
        <header class="intel-item-head">
          <div class="window-title-wrap">
            <span class="window-symbol">${laneKicker}</span>
            <h3>#${rank} ${source} 情报</h3>
          </div>
          <div class="intel-item-links">
            ${buildItemLinks(item)}
          </div>
        </header>
        <p class="window-summary intel-item-summary">${text}</p>
        <div class="intel-item-decision">
          <div class="intel-item-decision-label">入选原因</div>
          <div class="evidence-strip">
            ${buildDecisionChips(item)}
          </div>
          ${buildDecisionDetails(item)}
        </div>
        <div class="evidence-strip">
          ${buildItemMetaChips(item)}
          ${createdAt}
        </div>
      </article>
    `;
  }).join("");
}

function renderStrategyCards() {
  const sections = intelUiState.latestSections;
  const sentItems = buildPersistedSentItems();
  const counts = {
    crypto: sections.crypto.length,
    world: sections.world.length,
    persistent: sections.persistent.length,
    hot: sections.hot.length,
    custom: sections.custom.length,
    sent: sentItems.length,
  };

  for (const node of intelDom.laneCountNodes) {
    const lane = node.dataset.intelLaneCount;
    if (!lane || !(lane in counts)) {
      continue;
    }
    node.textContent = `${counts[lane]} 条`;
  }

  for (const card of intelDom.laneCards) {
    const lane = card.dataset.intelLaneCard;
    const active = lane === intelUiState.selectedLane;
    card.dataset.active = active ? "true" : "false";
    card.setAttribute("aria-pressed", active ? "true" : "false");
  }
}

export function renderItems(sections, { persistAsSent = false, persistedSections = null } = {}) {
  const normalizedSections = normalizeSections(sections);
  intelUiState.latestSections = normalizedSections;
  if (persistedSections) {
    intelUiState.persistedSections = normalizeSections(persistedSections);
  } else if (persistAsSent) {
    intelUiState.persistedSections = normalizedSections;
  }

  intelDom.summaryCryptoCount.textContent = String(normalizedSections.crypto.length);
  intelDom.summaryWorldCount.textContent = String(normalizedSections.world.length);
  intelDom.summaryPersistentCount.textContent = String(normalizedSections.persistent.length);
  intelDom.summaryHotCount.textContent = String(normalizedSections.hot.length);
  intelDom.summaryCustomCount.textContent = String(normalizedSections.custom.length);

  renderStrategyCards();
  renderFocusedLane();
}

export function applyConfig(config) {
  const limits = config?.limits || {};
  const custom = config?.custom || {};
  const telegram = config?.telegram || {};

  intelDom.dailyTimeInput.value = config?.daily_push_time || "08:00";
  intelDom.cryptoLimitInput.value = String(limits.crypto ?? 10);
  intelDom.worldLimitInput.value = String(limits.world ?? 3);
  intelDom.hotLimitInput.value = String(limits.hot ?? 2);
  intelDom.customUserLimitInput.value = String(limits.custom_user ?? 3);
  intelDom.customUsersInput.value = Array.isArray(custom.x_users) ? custom.x_users.join(",") : "";
  intelDom.summaryModeInput.value = config?.summary?.mode || "ai_first";
  intelDom.summaryModelInput.value = config?.summary?.model || "gpt-5.4";
  intelDom.telegramChatIdInput.value = telegram.chat_id || "";
}

export function renderTelegramStatus(data) {
  const telegram = data?.telegram || {};
  const daily = data?.daily || {};
  const scheduler = data?.scheduler || {};
  const delivery = data?.delivery || {};
  const pendingActions = Array.isArray(delivery.pending_actions) ? delivery.pending_actions : [];
  const hasPendingDelivery = Boolean(delivery.pending_state);

  intelDom.telegramTokenStatus.textContent = telegram.bot_token_configured
    ? `Token ${telegram.bot_token_masked || "已配置"}`
    : "Token 未配置";
  intelDom.telegramChatStatus.textContent = telegram.chat_id
    ? `目标 ${telegram.chat_title || telegram.chat_id}`
    : "目标会话未绑定";
  intelDom.telegramScheduleStatus.textContent = daily.enabled
    ? `定时 ${daily.push_time || "08:00"} ${daily.timezone || "Asia/Shanghai"}`
    : "定时已关闭";

  const schedulerMessage = scheduler.last_message || "调度状态未知";
  const lastSentAt = delivery.last_sent_at ? `；上次发送 ${delivery.last_sent_at}` : "";
  const pendingInfo = delivery.pending_state
    ? `；待确认 ${delivery.pending_digest_date || "今日"} ${delivery.pending_state === "sent_not_committed" ? "已发出" : "提交中"}`
    : "";
  const overdueInfo = delivery.is_overdue
    ? `；已超时 ${delivery.overdue_minutes || 0} 分钟${delivery.fallback_due ? "，等待自动补偿" : ""}${delivery.fallback_attempted_today ? "，今日已自动补偿过一次" : ""}`
    : "";
  const collectMessage = scheduler.last_collect_message ? `；${scheduler.last_collect_message}` : "";
  const lastCollectAt = scheduler.last_collect_at ? `；上次抓取 ${scheduler.last_collect_at}` : "";
  const nextCollectAt = scheduler.next_collect_due_at ? `；下次抓取 ${scheduler.next_collect_due_at}` : "";
  intelDom.telegramStatusText.textContent = `${schedulerMessage}${lastSentAt}${pendingInfo}${overdueInfo}${collectMessage}${lastCollectAt}${nextCollectAt}`;
  intelDom.telegramPendingConfirmButton.hidden = !(hasPendingDelivery && pendingActions.includes("confirm"));
  intelDom.telegramPendingClearButton.hidden = !(hasPendingDelivery && pendingActions.includes("clear"));
  intelDom.telegramPendingConfirmButton.disabled = !hasPendingDelivery;
  intelDom.telegramPendingClearButton.disabled = !hasPendingDelivery;
}

export function renderSummaryStatus(data) {
  const summary = data?.summary || {};
  const modeLabel = summary.mode === "fallback_only" ? "模式 仅回退" : "模式 AI优先";
  const providerLabel = summary.provider
    ? `引擎 ${summary.provider} / ${summary.model || "-"}`
    : `引擎 ${summary.model || "未发现"}`;
  const runtimeLabel = summary.active_mode === "ai"
    ? "当前 AI生效"
    : "当前 回退中";
  const sourceLabel = summary.credential_source ? `；来源 ${summary.credential_source}` : "";

  intelDom.summaryModeStatus.textContent = modeLabel;
  intelDom.summaryProviderStatus.textContent = providerLabel;
  intelDom.summaryRuntimeStatus.textContent = runtimeLabel;
  intelDom.summaryStatusText.textContent = `${summary.status_text || "摘要状态未知"}${sourceLabel}`;
}

function observabilitySeverityLabel(level) {
  if (level === "critical") {
    return "高风险";
  }
  if (level === "warn") {
    return "需关注";
  }
  if (level === "info") {
    return "提示";
  }
  return "正常";
}

function formatPercent(value) {
  const numeric = Number(value || 0);
  if (!Number.isFinite(numeric) || numeric <= 0) {
    return "0%";
  }
  return `${Math.round(numeric * 100)}%`;
}

function buildOpencliChip(opencli) {
  if (!opencli?.known) {
    return { label: "opencli 未采样", severity: "warn" };
  }
  if (!opencli.installed) {
    return { label: "opencli 未安装", severity: "critical" };
  }
  if (opencli.connected) {
    if (opencli.connection_source === "recent_success") {
      return { label: "opencli 最近成功", severity: "ok" };
    }
    return { label: "opencli 已连通", severity: "ok" };
  }
  if (opencli.auto_recover_on_demand) {
    return { label: "opencli 待机自恢复", severity: "ok" };
  }
  if (opencli.status_stale) {
    return { label: "opencli 状态过期", severity: "warn" };
  }
  return { label: "opencli 桥接未连通", severity: "critical" };
}

export function renderObservabilityStatus(data) {
  const overview = data?.overview || {};
  const metrics = data?.metrics || {};
  const sourceMix = metrics.source_mix || {};
  const summary = metrics.summary || {};
  const trend = metrics.trend || {};
  const opencli = metrics.opencli || {};
  const alerts = Array.isArray(data?.alerts) ? data.alerts : [];
  const level = String(overview.level || "ok").toLowerCase();
  const issueCount = Number(overview.issue_count || alerts.length || 0);
  const payloadSource = overview.payload_source === "build_task" ? "预览结果" : "最近缓存";
  const rssShare = formatPercent(sourceMix.rss_share || 0);
  const fallbackRatio = formatPercent(summary.fallback_ratio || 0);
  const opencliChip = buildOpencliChip(opencli);

  intelDom.observabilityHeadline.textContent = overview.status_text || "运行诊断 -";
  intelDom.observabilityChips.innerHTML = [
    `<span class="evidence-chip intel-observability-chip" data-severity="${escapeHtml(level)}">级别 ${escapeHtml(observabilitySeverityLabel(level))}</span>`,
    `<span class="evidence-chip">异常 ${escapeHtml(String(issueCount))}</span>`,
    `<span class="evidence-chip intel-observability-chip" data-severity="${escapeHtml(opencliChip.severity)}">${escapeHtml(opencliChip.label)}</span>`,
    `<span class="evidence-chip">X 候选 ${escapeHtml(String(sourceMix.x_total_configured || 0))}</span>`,
    `<span class="evidence-chip">RSS 占比 ${escapeHtml(rssShare)}</span>`,
    `<span class="evidence-chip">AI 回退 ${escapeHtml(fallbackRatio)}</span>`,
    `<span class="evidence-chip">依据 ${escapeHtml(payloadSource)}</span>`,
  ].join("");

  const trendChips = [
    `<span class="evidence-chip">采样 ${escapeHtml(String(trend.sample_count || 0))}</span>`,
    `<span class="evidence-chip">异常轮次 ${escapeHtml(String(trend.issue_runs || 0))}</span>`,
    `<span class="evidence-chip">重复异常 ${escapeHtml(String(trend.repeat_count || 0))}</span>`,
  ];
  const repeatItems = Array.isArray(trend.repeat_items) ? trend.repeat_items : [];
  for (const item of repeatItems.slice(0, 3)) {
    trendChips.push(
      `<span class="evidence-chip intel-observability-chip" data-severity="warn">${escapeHtml(String(item.title || item.code || "异常"))} ${escapeHtml(String(item.count || 0))} 次</span>`,
    );
  }
  intelDom.observabilityTrendText.textContent = trend.status_text || "趋势数据积累中，至少完成几轮后台抓取后再看更有意义。";
  intelDom.observabilityTrendChips.innerHTML = trendChips.join("");

  if (!alerts.length) {
    intelDom.observabilityList.innerHTML = `
      <article class="intel-observability-item" data-severity="ok">
        <div class="intel-observability-item-head">
          <strong>当前未发现明显运行异常</strong>
          <span class="intel-observability-badge" data-severity="ok">正常</span>
        </div>
        <p>后台抓取、来源结构和摘要状态目前没有出现需要立刻处理的信号。</p>
      </article>
    `;
    return;
  }

  intelDom.observabilityList.innerHTML = alerts.map((alert) => {
    const severity = String(alert.severity || "warn").toLowerCase();
    const recentCount = Number(alert.recent_count || 0);
    const repeatHint = recentCount > 0
      ? `<p class="intel-observability-hint">近 24 小时已出现 ${escapeHtml(String(recentCount))} 次。</p>`
      : "";
    const hint = alert.hint ? `<p class="intel-observability-hint">建议：${escapeHtml(String(alert.hint))}</p>` : "";
    return `
      <article class="intel-observability-item" data-severity="${escapeHtml(severity)}">
        <div class="intel-observability-item-head">
          <strong>${escapeHtml(String(alert.title || "运行异常"))}</strong>
          <span class="intel-observability-badge" data-severity="${escapeHtml(severity)}">${escapeHtml(observabilitySeverityLabel(severity))}</span>
        </div>
        <p>${escapeHtml(String(alert.detail || ""))}</p>
        ${repeatHint}
        ${hint}
      </article>
    `;
  }).join("");
}

export function applyLaneSelection(lane) {
  setSelectedLane(lane);
  renderStrategyCards();
  renderFocusedLane();
}

export function renderConfigCollapsedState() {
  const collapsed = Boolean(intelUiState.configCollapsed);
  intelDom.layout.dataset.configCollapsed = collapsed ? "true" : "false";
  intelDom.configPanel.dataset.collapsed = collapsed ? "true" : "false";
  intelDom.configBody.hidden = collapsed;
  intelDom.configCollapsedCopy.hidden = !collapsed;
  intelDom.configToggleButton.textContent = collapsed ? "展开配置" : "收起配置";
}

export function toggleConfigCollapsed() {
  setConfigCollapsed(!intelUiState.configCollapsed);
  renderConfigCollapsedState();
}

// Apply persisted state once on boot before data arrives.
renderConfigCollapsedState();
