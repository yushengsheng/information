const laneStorageKey = "intel:selected-lane";
const configCollapsedStorageKey = "intel:config-collapsed";
const digestDateStorageKey = "intel:selected-digest-date";

export const laneMetaMap = {
  crypto: {
    key: "crypto",
    kicker: "主线 01",
    title: "币圈 / 加密",
    description: "盯 ETF、监管、交易所、链上安全、资金流和机构动作，不追散乱喊单。",
    focusTitle: "币圈 / 加密情报",
    focusDescription: "优先 ETF、监管、交易所、链上安全与资金流，只保留更像主线的信号。",
    empty: "今日没有新的高价值加密主线。",
  },
  world: {
    key: "world",
    kicker: "主线 02",
    title: "战争 / 全球大事",
    description: "重点是战争、地缘冲突、选举、灾害和会影响全球市场与秩序的事件。",
    focusTitle: "战争 / 全球大事情报",
    focusDescription: "这里优先看战争、地缘冲突、选举、灾害和会影响全球市场定价的大事件。",
    empty: "今日没有新的全球级主线事件。",
  },
  persistent: {
    key: "persistent",
    kicker: "跟踪",
    title: "持续发酵",
    description: "专门保留跨时间窗口持续升温、被多次命中的事件，不让真正的大事一闪而过。",
    focusTitle: "持续发酵事件",
    focusDescription: "这里优先看连续数轮抓取后仍在升温的大事，适合盯持续升级的战争、监管和市场主线。",
    empty: "当前没有持续发酵的高价值事件。",
  },
  hot: {
    key: "hot",
    kicker: "补充",
    title: "热门事件补充",
    description: "只拿少量 AI、科技、宏观、灾害等额外热点，避免把主线挤掉。",
    focusTitle: "热门事件补充",
    focusDescription: "这里只做少量补充，避免 AI、科技、宏观等高热度事件冲掉主线阅读重心。",
    empty: "今日没有额外补充热点。",
  },
  custom: {
    key: "custom",
    kicker: "自选",
    title: "自定义关注",
    description: "你自己点名的 X 账号单独保留，不和主线抢位。",
    focusTitle: "自定义关注账号",
    focusDescription: "这里只展示你明确点名的 X 账号，和主线流分开看，不互相挤占版面。",
    empty: "当前没有自定义账号结果。",
  },
  sent: {
    key: "sent",
    kicker: "历史",
    title: "已发送",
    description: "查看当前所选日期正式日报里已经发送或已计入去重的全部内容。",
    focusTitle: "已发送信息",
    focusDescription: "这里汇总当前所选日期正式日报里已经发送或已计入去重的全部内容，方便回看完整结果。",
    empty: "当前所选日期还没有已发送日报。",
  },
};

export const laneOrder = ["crypto", "world", "persistent", "hot", "custom", "sent"];
export const sectionOrder = ["crypto", "world", "persistent", "hot", "custom"];

function readStorage(key) {
  try {
    return window.localStorage.getItem(key);
  } catch {
    return null;
  }
}

function writeStorage(key, value) {
  try {
    window.localStorage.setItem(key, value);
  } catch {
    // Ignore localStorage write failures.
  }
}

function buildEmptySections() {
  return {
    crypto: [],
    world: [],
    persistent: [],
    hot: [],
    custom: [],
  };
}

const storedLane = readStorage(laneStorageKey);
const storedCollapsed = readStorage(configCollapsedStorageKey);
const storedDigestDate = readStorage(digestDateStorageKey);

export const intelUiState = {
  selectedLane: laneMetaMap[storedLane] ? storedLane : "crypto",
  configCollapsed: storedCollapsed === "1",
  selectedDigestDate: storedDigestDate || "",
  latestSections: buildEmptySections(),
  persistedSections: buildEmptySections(),
};

export function normalizeSections(sections) {
  const normalized = buildEmptySections();
  for (const key of sectionOrder) {
    normalized[key] = Array.isArray(sections?.[key]) ? sections[key] : [];
  }
  return normalized;
}

export function setSelectedLane(lane) {
  if (!laneMetaMap[lane]) {
    return;
  }
  intelUiState.selectedLane = lane;
  writeStorage(laneStorageKey, lane);
}

export function setConfigCollapsed(collapsed) {
  intelUiState.configCollapsed = Boolean(collapsed);
  writeStorage(configCollapsedStorageKey, collapsed ? "1" : "0");
}

export function setSelectedDigestDate(digestDate) {
  intelUiState.selectedDigestDate = String(digestDate || "");
  writeStorage(digestDateStorageKey, intelUiState.selectedDigestDate);
}
