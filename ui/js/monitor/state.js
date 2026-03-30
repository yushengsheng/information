export const monitorUiState = {
  hasHydratedForm: false,
  pollTimer: null,
  expandedEvidenceWindows: new Set(),
  isStartPending: false,
  isAddPending: false,
  isRemovePending: false,
  isStopPending: false,
  latestState: null,
  pendingManageAction: null,
  pendingCardRemovalSymbol: null,
  selectedSymbol: null,
};

export function splitSymbolsText(value) {
  return String(value || "")
    .toUpperCase()
    .split(/[\s,，;；]+/)
    .map((item) => item.trim())
    .filter(Boolean);
}

export function splitCsvValues(value) {
  return String(value || "")
    .split(/[\s,，;；]+/)
    .map((item) => item.trim())
    .filter(Boolean);
}

export function getAvailableSymbols(state) {
  return (state?.sessions || []).map((session) => session.symbol);
}

export function ensureSelectedSymbol(state) {
  const symbols = getAvailableSymbols(state);
  if (!symbols.length) {
    monitorUiState.selectedSymbol = null;
    return null;
  }
  if (monitorUiState.selectedSymbol && symbols.includes(monitorUiState.selectedSymbol)) {
    return monitorUiState.selectedSymbol;
  }
  const preferred = state?.overview?.strongest_symbol;
  monitorUiState.selectedSymbol = preferred && symbols.includes(preferred) ? preferred : symbols[0];
  return monitorUiState.selectedSymbol;
}

export function hydrateForm(config, fields) {
  if (!config) {
    return;
  }
  fields.symbols.value = config.symbols_text ?? (config.symbols ?? []).join(",");
  fields.windowsMinutes.value = (config.windows_minutes ?? [5, 10]).join(",");
  fields.reportInterval.value = config.report_interval ?? 10;
  fields.baselineMinutes.value = config.baseline_minutes ?? 120;
  fields.runtimeSeconds.value = config.runtime_seconds ?? 0;
}

export function buildFormPayload(fields) {
  return {
    symbols: fields.symbols.value.trim().toUpperCase(),
    windows_minutes: fields.windowsMinutes.value.trim(),
    report_interval: Number(fields.reportInterval.value),
    baseline_minutes: Number(fields.baselineMinutes.value),
    runtime_seconds: Number(fields.runtimeSeconds.value),
  };
}

export function currentConfigPayload(latestState, fields, symbolsOverride) {
  const config = latestState?.config || {};
  const symbols = symbolsOverride ?? config.symbols ?? splitSymbolsText(fields.symbols.value);
  const windows = config.windows_minutes ?? splitCsvValues(fields.windowsMinutes.value);
  return {
    symbols: symbols.join(","),
    windows_minutes: Array.isArray(windows) ? windows.join(",") : String(windows || fields.windowsMinutes.value).trim(),
    report_interval: Number(config.report_interval ?? fields.reportInterval.value),
    baseline_minutes: Number(config.baseline_minutes ?? fields.baselineMinutes.value),
    runtime_seconds: Number(config.runtime_seconds ?? fields.runtimeSeconds.value),
  };
}
