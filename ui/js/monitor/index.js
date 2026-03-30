import { getJson, postJson } from "../shared/http.js";
import { monitorDom } from "./dom.js";
import { buildFormPayload, hydrateForm, monitorUiState, splitSymbolsText } from "./state.js";
import { renderManageConfirm, renderMode, renderState, renderSummary, renderSymbolOverview, renderWindows, setFeedback } from "./render.js";

function clearPendingManageAction() {
  monitorUiState.pendingManageAction = null;
  renderManageConfirm();
}

async function loadState() {
  try {
    const state = await getJson("/api/state");
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
  monitorUiState.isAddPending = true;
  renderMode(monitorUiState.latestState || { mode: document.body.dataset.mode || "idle", is_active: false, can_manage_symbols: false });
  setFeedback("正在更新监控币种...");
  try {
    const freshState = (await loadState()) || monitorUiState.latestState;
    const currentSymbols = freshState?.config?.symbols || splitSymbolsText(monitorDom.fields.symbols.value);
    const merged = Array.from(new Set([...currentSymbols, ...newSymbols]));
    const data = await postJson("/api/sync-symbols", { symbols: merged.join(",") });
    if (data.state?.config) {
      hydrateForm(data.state.config, monitorDom.fields);
    }
    setFeedback(`已更新监控币种，共 ${data.state?.config?.symbols?.length || merged.length} 个币种`);
    monitorDom.manageSymbolsInput.value = "";
  } catch (error) {
    setFeedback(`添加失败: ${error.message}`, true);
  } finally {
    monitorUiState.isAddPending = false;
    await loadState();
  }
}

async function performRemoveSymbols(removeSymbols) {
  if (!removeSymbols.length) {
    setFeedback("请先输入要移除的币种", true);
    return;
  }
  monitorUiState.isRemovePending = true;
  renderMode(monitorUiState.latestState || { mode: document.body.dataset.mode || "idle", is_active: false, can_manage_symbols: false });
  setFeedback("正在更新监控币种...");
  try {
    if (monitorUiState.latestState?.can_manage_symbols) {
      const freshState = (await loadState()) || monitorUiState.latestState;
      const current = freshState?.config?.symbols || [];
      const removeSet = new Set(removeSymbols);
      const nextSymbols = current.filter((item) => !removeSet.has(item));
      const removed = current.filter((item) => removeSet.has(item));
      const missing = removeSymbols.filter((item) => !current.includes(item));
      const data = await postJson("/api/sync-symbols", { symbols: nextSymbols.join(",") });
      if (data.state?.config) {
        hydrateForm(data.state.config, monitorDom.fields);
      }
      if (removed.length) {
        setFeedback(`已移除 ${removed.join(", ")}${missing.length ? `；未找到 ${missing.join(", ")}` : ""}`);
        monitorDom.manageSymbolsInput.value = "";
      } else if (missing.length) {
        setFeedback(`这些币种当前不在监控中：${missing.join(", ")}`);
      } else {
        setFeedback("没有移除任何币种");
      }
    } else {
      const removeSet = new Set(removeSymbols);
      const current = splitSymbolsText(monitorDom.fields.symbols.value);
      const next = current.filter((item) => !removeSet.has(item));
      monitorDom.fields.symbols.value = next.join(",");
      setFeedback("已从待启动配置里移除输入币种");
      monitorDom.manageSymbolsInput.value = "";
    }
  } catch (error) {
    setFeedback(`移除失败: ${error.message}`, true);
  } finally {
    monitorUiState.isRemovePending = false;
    monitorUiState.pendingCardRemovalSymbol = null;
    await loadState();
  }
}

monitorDom.form.addEventListener("submit", async (event) => {
  event.preventDefault();
  monitorUiState.isStartPending = true;
  renderMode(monitorUiState.latestState || { mode: document.body.dataset.mode || "idle", is_active: false, can_manage_symbols: false });
  setFeedback("正在提交配置并启动监控...");
  try {
    const data = await postJson("/api/start", buildFormPayload(monitorDom.fields));
    if (data.state?.config) {
      hydrateForm(data.state.config, monitorDom.fields);
    }
    const count = data.state?.config?.symbols?.length || 0;
    setFeedback(`已启动 ${count} 个币种`);
  } catch (error) {
    setFeedback(`启动失败: ${error.message}`, true);
  } finally {
    monitorUiState.isStartPending = false;
    await loadState();
  }
});

monitorDom.addButton.addEventListener("click", () => {
  if (!monitorUiState.latestState?.can_manage_symbols) {
    setFeedback("请先启动监控，再管理运行中币种", true);
    return;
  }
  const newSymbols = splitSymbolsText(monitorDom.manageSymbolsInput.value);
  if (!newSymbols.length) {
    setFeedback("请先输入要新增的币种", true);
    return;
  }
  monitorUiState.pendingManageAction = { type: "add", symbols: newSymbols };
  renderManageConfirm();
  setFeedback(`请确认是否添加 ${newSymbols.join(", ")}`);
});

monitorDom.removeButton.addEventListener("click", () => {
  if (!monitorUiState.latestState?.can_manage_symbols) {
    setFeedback("请先启动监控，再管理运行中币种", true);
    return;
  }
  const removeSymbols = splitSymbolsText(monitorDom.manageSymbolsInput.value);
  if (!removeSymbols.length) {
    setFeedback("请先输入要移除的币种", true);
    return;
  }
  monitorUiState.pendingManageAction = { type: "remove", symbols: removeSymbols };
  renderManageConfirm();
  setFeedback(`请确认是否移除 ${removeSymbols.join(", ")}`);
});

monitorDom.stopButton.addEventListener("click", async () => {
  monitorUiState.isStopPending = true;
  renderMode(monitorUiState.latestState || { mode: document.body.dataset.mode || "idle", is_active: true, can_manage_symbols: false });
  setFeedback("正在请求停止...");
  try {
    await postJson("/api/stop", {});
  } catch (error) {
    setFeedback(`停止失败: ${error.message}`, true);
  } finally {
    monitorUiState.isStopPending = false;
    await loadState();
  }
});

monitorDom.manageConfirmBar.addEventListener("click", async (event) => {
  const confirmButton = event.target.closest("[data-confirm-manage]");
  if (confirmButton) {
    const action = monitorUiState.pendingManageAction;
    clearPendingManageAction();
    if (!action) {
      return;
    }
    if (action.type === "add") {
      await performAddSymbols(action.symbols);
    } else {
      await performRemoveSymbols(action.symbols);
    }
    return;
  }

  if (event.target.closest("[data-cancel-manage]")) {
    clearPendingManageAction();
    setFeedback("已取消本次操作");
  }
});

monitorDom.symbolOverviewGrid.addEventListener("click", async (event) => {
  const removeButton = event.target.closest("[data-remove-symbol]");
  if (removeButton) {
    const symbol = removeButton.dataset.removeSymbol;
    if (!symbol) {
      return;
    }
    monitorUiState.pendingCardRemovalSymbol = symbol;
    renderSymbolOverview(monitorUiState.latestState || { sessions: [] });
    setFeedback(`请确认是否移除 ${symbol}`);
    return;
  }

  const confirmRemoveButton = event.target.closest("[data-confirm-remove-symbol]");
  if (confirmRemoveButton) {
    const symbol = confirmRemoveButton.dataset.confirmRemoveSymbol;
    if (symbol) {
      await performRemoveSymbols([symbol]);
    }
    return;
  }

  const cancelRemoveButton = event.target.closest("[data-cancel-remove-symbol]");
  if (cancelRemoveButton) {
    monitorUiState.pendingCardRemovalSymbol = null;
    renderSymbolOverview(monitorUiState.latestState || { sessions: [] });
    setFeedback("已取消本次移除");
    return;
  }

  const card = event.target.closest("[data-symbol-card]");
  if (card) {
    const symbol = card.dataset.symbolCard;
    if (!symbol) {
      return;
    }
    monitorUiState.selectedSymbol = symbol;
    renderSummary(monitorUiState.latestState || { sessions: [] });
    renderSymbolOverview(monitorUiState.latestState || { sessions: [] });
    renderWindows(monitorUiState.latestState || { sessions: [] });
  }
});

monitorDom.windowGrid.addEventListener("toggle", (event) => {
  const target = event.target;
  if (!(target instanceof HTMLDetailsElement) || !target.matches(".details-box")) {
    return;
  }
  const key = target.dataset.windowKey;
  if (!key) {
    return;
  }
  if (target.open) {
    monitorUiState.expandedEvidenceWindows.add(key);
  } else {
    monitorUiState.expandedEvidenceWindows.delete(key);
  }
}, true);

async function boot() {
  await loadState();
  monitorUiState.pollTimer = window.setInterval(loadState, 2000);
}

window.addEventListener("beforeunload", () => {
  if (monitorUiState.pollTimer) {
    window.clearInterval(monitorUiState.pollTimer);
  }
});

boot();
