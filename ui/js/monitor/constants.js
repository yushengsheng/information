export const FRONTEND_VERSION = "20260326-sync-ui-4";

export const stateTextMap = {
  idle: "等待启动",
  starting: "正在启动",
  running: "监控中",
  stopping: "正在停止",
  stopped: "已停止",
  error: "监控异常",
};

export const stateHintMap = {
  WARMING_UP: "窗口尚未跑满，持续性判断还不完整。",
  NORMAL: "双向主动成交没有达到可疑阈值。",
  TWO_SIDED_ACTIVE: "买卖两侧都很活跃，但暂未触发疑似刷量。",
  SUSPECTED_WASH_LIKE: "成交活跃、方向均衡且价格位移有限，疑似刷量节奏。",
};

export const judgementStatusTextMap = {
  positive: "成立",
  neutral: "待观察",
  negative: "不明显",
  warming: "预热中",
};
