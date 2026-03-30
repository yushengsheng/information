import { getJson, postJson } from "../shared/http.js";
import { escapeHtml } from "../shared/text.js";
import { intelDom } from "./dom.js";
import { intelUiState, setSelectedDigestDate } from "./state.js";
import {
  applyConfig,
  applyLaneSelection,
  formatSecondsFromMs,
  renderBuildMeta,
  renderConfigCollapsedState,
  renderDigestDateOptions,
  renderItems,
  renderObservabilityStatus,
  renderServiceVersion,
  renderSummaryStatus,
  renderTelegramStatus,
  setFeedback,
  setStatus,
  setStatusPending,
  toggleConfigCollapsed,
} from "./render.js";

let opencliReconnectTimer = 0;
let digestTaskWatcher = null;
let digestTaskWatcherId = "";
const digestOriginalLinkPattern = /^原文：(https?:\/\/\S+)$/;
const digestTaskPollIntervalMs = 2500;

function buildOpencliStatusUrl(mode = "runtime") {
  const params = new URLSearchParams();
  if (mode && mode !== "runtime") {
    params.set("mode", mode);
  }
  const suffix = params.toString() ? `?${params.toString()}` : "";
  return `/api/opencli/status${suffix}`;
}

function clearOpencliReconnectTimer() {
  if (opencliReconnectTimer) {
    window.clearTimeout(opencliReconnectTimer);
    opencliReconnectTimer = 0;
  }
}

function showOpencliBootGraceState() {
  setStatusPending("连接确认中...");
  setFeedback("opencli 正在继续确认连接；若扩展已就绪会自动恢复，无需刷新页面。", false);
}

function buildConfigPayload() {
  const users = intelDom.customUsersInput.value
    .split(/[，,\s]+/)
    .map((value) => value.trim().replace(/^@+/, ""))
    .filter(Boolean);

  return {
    config: {
      daily_push_time: intelDom.dailyTimeInput.value.trim() || "08:00",
      limits: {
        crypto: Number(intelDom.cryptoLimitInput.value || 10),
        world: Number(intelDom.worldLimitInput.value || 3),
        hot: Number(intelDom.hotLimitInput.value || 2),
        custom_user: Number(intelDom.customUserLimitInput.value || 3),
      },
      custom: {
        x_users: users,
      },
      summary: {
        mode: intelDom.summaryModeInput.value || "ai_first",
        model: intelDom.summaryModelInput.value.trim() || "gpt-5.4",
      },
      telegram: {
        chat_id: intelDom.telegramChatIdInput.value.trim(),
      },
    },
  };
}

async function loadServiceVersion() {
  try {
    const data = await getJson("/health");
    renderServiceVersion(data.app_version || "-", null);
  } catch {
    renderServiceVersion("-", null);
  }
}

async function checkOpencliStatus(mode = "doctor") {
  clearOpencliReconnectTimer();
  setFeedback(mode === "doctor" ? "正在深度检查 opencli 状态..." : "正在检查 opencli 状态...");
  const data = await getJson(buildOpencliStatusUrl(mode));
  renderServiceVersion(data.app_version || intelDom.summaryVersion.textContent || "-", null);
  setStatus(Boolean(data.installed), Boolean(data.connected), data.message || "未知状态");
  intelDom.doctorOutput.textContent = data.doctor || data.hint || "无诊断输出";

  if (!data.installed) {
    setFeedback("未安装 opencli。请先安装后重试。", true);
  } else if (data.auto_recover_on_demand) {
    setFeedback(data.message || "opencli 当前待机中，抓取时会自动恢复。", false);
  } else if (!data.connected) {
    setFeedback("opencli 已安装，但浏览器扩展未连接。请按诊断说明处理。", true);
  } else {
    setFeedback("opencli 状态正常，可生成日报。", false);
  }

  return data;
}

function sleep(ms) {
  return new Promise((resolve) => {
    window.setTimeout(resolve, ms);
  });
}

function setDigestBuildButtonsBusy(busy) {
  intelDom.previewDigestButton.disabled = Boolean(busy);
}

function setPendingDeliveryButtonsBusy(busy) {
  if (!intelDom.telegramPendingConfirmButton.hidden) {
    intelDom.telegramPendingConfirmButton.disabled = Boolean(busy);
  }
  if (!intelDom.telegramPendingClearButton.hidden) {
    intelDom.telegramPendingClearButton.disabled = Boolean(busy);
  }
}

function buildEmptySections() {
  return { crypto: [], world: [], persistent: [], hot: [], custom: [] };
}

function describeDigestTask(task) {
  return task?.final ? "正式日报" : "预览日报";
}

function isDigestTaskActive(task) {
  return Boolean(task && ["queued", "running"].includes(task.status));
}

async function loadDigestBuildStatus({ includeResult = false } = {}) {
  const params = new URLSearchParams();
  if (includeResult) {
    params.set("include_result", "1");
  }
  const suffix = params.toString() ? `?${params.toString()}` : "";
  return getJson(`/api/intel/digest/build/status${suffix}`);
}

function buildDigestMessageHtml(data) {
  const messageHtml = typeof data?.message_html === "string" ? data.message_html.trim() : "";
  if (messageHtml) {
    return messageHtml.replace(/<a\s+href=/g, '<a target="_blank" rel="noopener noreferrer" href=');
  }

  const message = typeof data?.message === "string" ? data.message.trim() : "";
  if (!message) {
    return "";
  }

  return message
    .split(/\r?\n/)
    .map((line) => {
      const match = line.trim().match(digestOriginalLinkPattern);
      if (match) {
        return `<a href="${escapeHtml(match[1])}" target="_blank" rel="noopener noreferrer">原文</a>`;
      }
      return escapeHtml(line);
    })
    .join("\n");
}

function renderDigestMessage(data, fallbackText) {
  const messageHtml = buildDigestMessageHtml(data);
  if (messageHtml) {
    intelDom.digestMessage.innerHTML = messageHtml;
    return;
  }
  intelDom.digestMessage.textContent = fallbackText;
}

function applyTaskResult(task) {
  const data = task?.result || {};
  renderDigestMessage(data, "生成成功，但正文为空");
  renderItems(data.sections || buildEmptySections());
  renderBuildMeta(data.build_stats || null);
  renderServiceVersion(data.app_version || intelDom.summaryVersion.textContent || "-", data.digest_app_version || null);
}

async function checkOpencliStatusWithRetry({ retries = 2, retryDelayMs = 1200, silent = false, deferDisconnectedUi = false, mode = "runtime" } = {}) {
  let lastResult = null;
  let lastError = null;

  for (let attempt = 0; attempt <= retries; attempt += 1) {
    try {
      if (silent) {
        const data = await getJson(buildOpencliStatusUrl(mode));
        renderServiceVersion(data.app_version || intelDom.summaryVersion.textContent || "-", null);
        intelDom.doctorOutput.textContent = data.doctor || data.hint || "无诊断输出";
        if (!data.installed) {
          setStatus(false, false, data.message || "未安装 opencli");
        } else if (data.connected) {
          clearOpencliReconnectTimer();
          setStatus(true, true, data.message || "已连通");
        } else if (data.auto_recover_on_demand) {
          clearOpencliReconnectTimer();
          setStatusPending(data.message || "待机中，抓取时自动恢复");
        } else if (deferDisconnectedUi && attempt < retries) {
          setStatusPending("连接确认中...");
        } else {
          setStatus(true, false, data.message || "未连通浏览器扩展");
        }
        lastResult = data;
      } else {
        lastResult = await checkOpencliStatus(mode);
      }

      if (lastResult?.connected || attempt >= retries) {
        return lastResult;
      }
    } catch (error) {
      lastError = error;
      if (attempt >= retries) {
        throw error;
      }
    }

    await sleep(retryDelayMs);
  }

  if (lastResult) {
    return lastResult;
  }
  throw lastError || new Error("opencli 状态检查失败");
}

function scheduleOpencliReconnectProbe({ maxAttempts = 10, retryDelayMs = 3000, showFailureState = false } = {}) {
  clearOpencliReconnectTimer();

  let remainingAttempts = maxAttempts;
  const probe = async () => {
    if (remainingAttempts <= 0) {
      opencliReconnectTimer = 0;
      if (showFailureState) {
        setStatus(true, false, "未连通浏览器扩展");
        setFeedback("opencli 长时间未连通，请按诊断说明处理。", true);
      }
      return;
    }
    remainingAttempts -= 1;

    try {
      const data = await checkOpencliStatusWithRetry({
        retries: 1,
        retryDelayMs: 1000,
        silent: true,
        deferDisconnectedUi: true,
        mode: "runtime",
      });
      if (data?.installed && data?.connected) {
        clearOpencliReconnectTimer();
        setFeedback("opencli 已自动恢复连接，可生成日报。", false);
        return;
      }
      if (data?.installed && data?.auto_recover_on_demand) {
        clearOpencliReconnectTimer();
        setStatusPending(data.message || "待机中，抓取时自动恢复");
        setFeedback(data.message || "opencli 当前待机中，抓取时会自动恢复。", false);
        return;
      }
      if (!data?.installed) {
        clearOpencliReconnectTimer();
        return;
      }
    } catch {
      // Keep probing for transient boot-time failures.
    }

    opencliReconnectTimer = window.setTimeout(probe, retryDelayMs);
  };

  opencliReconnectTimer = window.setTimeout(probe, retryDelayMs);
}

async function loadConfig() {
  const data = await getJson("/api/intel/config");
  applyConfig(data.config || {});
  renderServiceVersion(data.app_version || intelDom.summaryVersion.textContent || "-", null);
}

async function loadTelegramStatus() {
  const data = await getJson("/api/intel/telegram/status");
  renderServiceVersion(data.app_version || intelDom.summaryVersion.textContent || "-", null);
  renderTelegramStatus(data);
  if (!intelDom.telegramChatIdInput.value && data?.telegram?.chat_id) {
    intelDom.telegramChatIdInput.value = data.telegram.chat_id;
  }
  return data;
}

async function handlePendingDeliveryAction(action) {
  const actionLabel = action === "confirm" ? "确认已发送" : "清除待确认";
  setPendingDeliveryButtonsBusy(true);
  setFeedback(`正在${actionLabel}...`);
  try {
    const data = await postJson("/api/intel/telegram/pending-delivery", { action });
    await Promise.all([loadTelegramStatus(), loadObservabilityStatus(), loadLatestDigest()]);
    setFeedback(data.message || `${actionLabel}完成。`, false);
  } catch (error) {
    const detail = error.detail ? `\n${error.detail}` : "";
    setFeedback(`${actionLabel}失败：${error.message}${detail}`, true);
  } finally {
    setPendingDeliveryButtonsBusy(false);
  }
}

async function loadSummaryStatus() {
  const data = await getJson("/api/intel/summary/status");
  renderServiceVersion(data.app_version || intelDom.summaryVersion.textContent || "-", null);
  renderSummaryStatus(data);
  return data;
}

async function loadObservabilityStatus() {
  const data = await getJson("/api/intel/observability/status");
  renderServiceVersion(data.app_version || intelDom.summaryVersion.textContent || "-", null);
  renderObservabilityStatus(data);
  return data;
}

async function loadLatestDigest(selectedDate = intelUiState.selectedDigestDate) {
  const params = new URLSearchParams({ mode: "display" });
  if (selectedDate) {
    params.set("date", selectedDate);
  }
  const data = await getJson(`/api/intel/digest/latest?${params.toString()}`);
  const sentSections = data.sent_exists ? (data.sent_sections || buildEmptySections()) : buildEmptySections();
  renderServiceVersion(data.app_version || intelDom.summaryVersion.textContent || "-", data.digest_app_version || null);
  renderDigestDateOptions(data.available_dates || [], data.selected_date || "");
  setSelectedDigestDate(data.selected_date || "");
  if (!data.exists) {
    renderDigestMessage(null, "尚未生成日报。");
    renderItems(buildEmptySections(), { persistedSections: sentSections });
    renderBuildMeta(null);
    return;
  }
  renderDigestMessage(data, "尚无日报正文");
  renderItems(data.sections || {}, {
    persistAsSent: Boolean(data.final),
    persistedSections: !data.final && data.sent_exists ? sentSections : null,
  });
  renderBuildMeta(data.build_stats || null);
}

intelDom.laneCards.forEach((card) => {
  card.addEventListener("click", () => {
    const lane = card.dataset.intelLaneCard;
    if (!lane) {
      return;
    }
    applyLaneSelection(lane);
  });
});

intelDom.configToggleButton.addEventListener("click", () => {
  toggleConfigCollapsed();
});

intelDom.digestDateSelect.addEventListener("change", async () => {
  const selectedDate = intelDom.digestDateSelect.value;
  setSelectedDigestDate(selectedDate);
  try {
    await loadLatestDigest(selectedDate);
    setFeedback(`已切换到${selectedDate ? ` ${selectedDate} ` : "最新"}情报。`, false);
  } catch (error) {
    setFeedback(`读取${selectedDate ? ` ${selectedDate} ` : "最新"}情报失败：${error.message}`, true);
  }
});

async function watchDigestTask(taskId, initialTask = null) {
  if (!taskId) {
    return null;
  }
  if (digestTaskWatcher && digestTaskWatcherId === taskId) {
    return digestTaskWatcher;
  }

  digestTaskWatcherId = taskId;
  digestTaskWatcher = (async () => {
    let task = initialTask;
    setDigestBuildButtonsBusy(true);

    try {
      while (true) {
        if (!task || String(task.id || "") !== taskId) {
          const status = await loadDigestBuildStatus();
          task = status?.task || null;
        }

        if (!task || String(task.id || "") !== taskId) {
          throw new Error("后台任务状态已丢失，请重新发起生成。");
        }

        renderServiceVersion(task.app_version || intelDom.summaryVersion.textContent || "-", null);

        if (isDigestTaskActive(task)) {
          setFeedback(task.message || `${describeDigestTask(task)}正在后台生成中...`, false);
          await sleep(digestTaskPollIntervalMs);
          task = null;
          continue;
        }

        if (task.status === "succeeded") {
          if (!task.result && task.has_result) {
            const fullStatus = await loadDigestBuildStatus({ includeResult: true });
            task = fullStatus?.task || task;
          }
          const data = task.result || {};
          applyTaskResult(task);
          if (task.final) {
            await loadLatestDigest(data.digest_date || "");
          }
          await loadObservabilityStatus();
          const seconds = formatSecondsFromMs(data?.build_stats?.timings_ms?.total);
          setFeedback(
            task.final ? `正式日报已生成并计入去重，用时 ${seconds}。` : `预览日报生成完成，用时 ${seconds}。`,
            false,
          );
          return task;
        }

        if (task.status === "failed") {
          throw new Error(task.error || task.message || "日报生成失败");
        }

        throw new Error(`未知任务状态：${task.status || "-"}`);
      }
    } finally {
      setDigestBuildButtonsBusy(false);
      if (digestTaskWatcherId === taskId) {
        digestTaskWatcherId = "";
        digestTaskWatcher = null;
      }
    }
  })();

  return digestTaskWatcher;
}

async function buildDigestPreview() {
  setDigestBuildButtonsBusy(true);
  setFeedback("正在提交预览日报任务...");
  try {
    const data = await postJson("/api/intel/digest/build", {
      final: false,
      respect_sent: false,
    });
    const task = data?.task || null;
    if (!task?.id) {
      throw new Error("服务未返回有效任务 ID");
    }
    setFeedback(
      data.submitted
        ? "预览日报任务已提交到后台，页面会自动刷新结果。"
        : "已有预览日报任务在后台运行，正在继续等待结果。",
      false,
    );
    await watchDigestTask(String(task.id), task);
  } catch (error) {
    const detail = error.detail ? `\n${error.detail}` : "";
    setFeedback(`日报生成失败：${error.message}${detail}`, true);
    setDigestBuildButtonsBusy(false);
  }
}

intelDom.configForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  intelDom.saveConfigButton.disabled = true;
  setFeedback("正在保存配置...");
  try {
    const data = await postJson("/api/intel/config", buildConfigPayload());
    applyConfig(data.config || {});
    renderServiceVersion(data.app_version || intelDom.summaryVersion.textContent || "-", null);
    await Promise.all([loadTelegramStatus(), loadSummaryStatus(), loadObservabilityStatus()]);
    setFeedback("配置已保存（重启后仍保留）。", false);
  } catch (error) {
    const detail = error.detail ? `\n${error.detail}` : "";
    setFeedback(`保存失败：${error.message}${detail}`, true);
  } finally {
    intelDom.saveConfigButton.disabled = false;
  }
});

intelDom.checkButton.addEventListener("click", async () => {
  intelDom.checkButton.disabled = true;
  try {
    const data = await checkOpencliStatusWithRetry({ retries: 1, retryDelayMs: 1000, silent: false, mode: "doctor" });
    if (data?.installed && !data?.connected && !data?.auto_recover_on_demand) {
      scheduleOpencliReconnectProbe({ showFailureState: true });
    }
  } catch (error) {
    setFeedback(`状态检查失败：${error.message}`, true);
  } finally {
    intelDom.checkButton.disabled = false;
  }
});

intelDom.telegramRefreshButton.addEventListener("click", async () => {
  intelDom.telegramRefreshButton.disabled = true;
  try {
    const [data] = await Promise.all([loadTelegramStatus(), loadObservabilityStatus()]);
    setFeedback(data.hint || "Telegram 状态已刷新。", false);
  } catch (error) {
    setFeedback(`刷新 Telegram 状态失败：${error.message}`, true);
  } finally {
    intelDom.telegramRefreshButton.disabled = false;
  }
});

intelDom.telegramResolveButton.addEventListener("click", async () => {
  intelDom.telegramResolveButton.disabled = true;
  setFeedback("正在识别最近的 Telegram 会话...");
  try {
    const data = await postJson("/api/intel/telegram/resolve-chat", {});
    if (data?.telegram?.chat_id) {
      intelDom.telegramChatIdInput.value = data.telegram.chat_id;
    }
    await loadTelegramStatus();
    setFeedback(data.message || "Telegram 会话已绑定。", false);
  } catch (error) {
    const detail = error.detail ? `\n${error.detail}` : "";
    setFeedback(`识别 Telegram 会话失败：${error.message}${detail}`, true);
  } finally {
    intelDom.telegramResolveButton.disabled = false;
  }
});

intelDom.telegramTestButton.addEventListener("click", async () => {
  intelDom.telegramTestButton.disabled = true;
  setFeedback("正在发送 Telegram 测试日报...");
  try {
    const data = await postJson("/api/intel/telegram/test-send", {});
    await loadTelegramStatus();
    setFeedback(data.message || "Telegram 测试日报已发送。", false);
  } catch (error) {
    const detail = error.detail ? `\n${error.detail}` : "";
    setFeedback(`Telegram 测试发送失败：${error.message}${detail}`, true);
  } finally {
    intelDom.telegramTestButton.disabled = false;
  }
});

intelDom.telegramPendingConfirmButton.addEventListener("click", async () => {
  await handlePendingDeliveryAction("confirm");
});

intelDom.telegramPendingClearButton.addEventListener("click", async () => {
  await handlePendingDeliveryAction("clear");
});

intelDom.summaryRefreshButton.addEventListener("click", async () => {
  intelDom.summaryRefreshButton.disabled = true;
  try {
    const data = await loadSummaryStatus();
    await loadObservabilityStatus();
    setFeedback(data?.summary?.status_text || "摘要状态已刷新。", false);
  } catch (error) {
    setFeedback(`刷新摘要状态失败：${error.message}`, true);
  } finally {
    intelDom.summaryRefreshButton.disabled = false;
  }
});

intelDom.previewDigestButton.addEventListener("click", async () => {
  await buildDigestPreview();
});

(async function boot() {
  renderConfigCollapsedState();
  renderItems(buildEmptySections(), { persistAsSent: true });
  setStatusPending("检查中...");
  await loadServiceVersion();
  try {
    const bootResults = await Promise.all([
      checkOpencliStatusWithRetry({ retries: 5, retryDelayMs: 1500, silent: true, deferDisconnectedUi: true, mode: "runtime" }),
      loadConfig(),
      loadLatestDigest(),
      loadTelegramStatus(),
      loadSummaryStatus(),
      loadDigestBuildStatus(),
      loadObservabilityStatus(),
    ]);
    const opencliStatus = bootResults[0];
    const digestTaskStatus = bootResults[5];
    if (isDigestTaskActive(digestTaskStatus?.task)) {
      watchDigestTask(String(digestTaskStatus.task.id), digestTaskStatus.task).catch((error) => {
        setFeedback(`后台任务失败：${error.message}`, true);
      });
    } else if (digestTaskStatus?.task?.status === "succeeded" && !digestTaskStatus.task.final && digestTaskStatus.task.has_result) {
      const fullStatus = await loadDigestBuildStatus({ includeResult: true });
      if (fullStatus?.task?.result) {
        applyTaskResult(fullStatus.task);
      }
    }
    if (opencliStatus?.installed && opencliStatus?.connected) {
      clearOpencliReconnectTimer();
      setFeedback("opencli 状态正常，可生成日报。", false);
    } else if (opencliStatus?.installed && opencliStatus?.auto_recover_on_demand) {
      clearOpencliReconnectTimer();
      setFeedback(opencliStatus.message || "opencli 当前待机中，抓取时会自动恢复。", false);
    } else if (opencliStatus?.installed) {
      showOpencliBootGraceState();
      scheduleOpencliReconnectProbe({ maxAttempts: 12, retryDelayMs: 3000, showFailureState: true });
    } else if (opencliStatus) {
      setFeedback("未安装 opencli。请先安装后重试。", true);
    } else if (!intelDom.feedback.textContent || intelDom.feedback.textContent === "等待操作。") {
      setFeedback("页面已就绪。", false);
    }
  } catch (error) {
    setFeedback(`初始化失败：${error.message}`, true);
  }
})();
