# Binance 疑似对敲成交量监控器

这个项目基于 Binance 现货公开市场数据进行实时监控，用来识别持续性的双向吃单流，并突出显示那些看起来像“对敲型成交量”的行为。

它适合类似 `NIGHTUSDT` 这样的场景，你希望知道市场是否同时出现了：

- 大额 taker 主动买入和大额 taker 主动卖出
- 很高的成交频率
- 净价格波动有限
- 在最近 5 分钟和 10 分钟内持续存在的异常模式

## 检测内容

脚本只使用 Binance 的公开市场数据：

- `aggTrade`：识别 taker 方向上的主动成交
- `bookTicker`：跟踪盘口最优价位的更新频率和补单迹象
- `depth@100ms` + REST depth snapshot：维护本地订单簿，用于深度和滑点检查
- `kline_1m`：维护正常 1 分钟成交额与成交笔数的滚动基线

检测器会针对每个滚动窗口输出以下状态：

- `WARMING_UP`
- `NORMAL`
- `TWO_SIDED_ACTIVE`
- `SUSPECTED_WASH_LIKE`

分类依据包括：

- 当前窗口成交额相对近期 1 分钟基线的放大倍数
- 当前窗口成交笔数相对近期 1 分钟基线的放大倍数
- taker 买卖两侧的均衡程度
- taker 方向切换频率
- 窗口内绝对价格变动幅度
- 最优盘口更新频率与简单补单迹象
- 本地订单簿 0.2% 档位深度
- 每秒采样一次的 0.2% 深度时间加权结果
- 以 10,000 quote 市值扫单时的估算滑点
- 固定成交数量的重复集中度
- 固定成交金额的重复集中度
- 成交到达时间间隔的规律性

## 重要限制

这个工具检测的是“持续性的双向主动成交流”，并不能识别背后的账户身份。

Binance 公开市场数据不会暴露交易者身份，也不能证明每一笔 taker 成交都一定来自字面意义上的 `MARKET` 订单。更准确的理解方式是：它反映的是激进的 taker 成交流，其中既可能包含市价单，也可能包含以激进价格成交的限价单。

## 运行要求

- Python 3.10+
- 能访问 Binance 公开 REST 和 WebSocket 接口的网络环境

安装依赖：

```bash
python -m pip install -r requirements.txt
```

## 页面结构（可切换）

- 刷量监控面板：`http://127.0.0.1:8765/`
- X 指定信息面板：`http://127.0.0.1:8765/x`
- 两个页面顶部都有切换按钮，可随时互相跳转。

## X 指定信息面板（opencli）

该页面通过 `opencli` 抓取 X 信息（当前实现为“指定账号 + 关键词”的搜索）。

前置要求：

1. 安装 opencli：
   ```bash
   npm install -g @jackwener/opencli
   ```
2. 在 Chrome 安装并启用 opencli Browser Bridge 扩展。
3. 在 Chrome 中登录 x.com。
4. 执行 `opencli doctor` 显示 Extension connected 后再抓取。

## Telegram 日报推送（本地）

当前默认配置已经按北京时间执行：

- 时区：`Asia/Shanghai`
- 每日推送时间：`08:00`
- bot token 保存在本地忽略文件：`data/intel_secrets.json`

首次绑定 Telegram 目标会话时，需要先给机器人 `@xinxiliu_bot` 发送一次 `/start` 或任意消息，否则 Telegram 不会把你的 `chat_id` 暴露给 bot。

常用命令：

```bash
./.venv/bin/python scripts/intel_daily_job.py status
./.venv/bin/python scripts/intel_daily_job.py resolve-chat
./.venv/bin/python scripts/intel_daily_job.py test-send
./.venv/bin/python scripts/intel_daily_job.py run-daily --force
```

如果希望由 macOS 每天自动在本地执行一次日报检查并投递到 Telegram：

```bash
./install_daily_intel_launchd.sh
```

卸载本地日任务：

```bash
./uninstall_daily_intel_launchd.sh
```

## UI 启动方式

双击：

```text
start_monitor_ui.bat
```

脚本会自动安装依赖、启动本地服务并打开浏览器。

如果你想静默启动，不显示黑色终端窗口，双击：

```text
start_monitor_ui_hidden.vbs
```

这个隐藏启动器会使用 `pythonw` 启动 UI。建议在依赖已经安装完成后使用。

如果页面虽然关闭了，但本项目占用的本地进程还没退出，可以双击：

```text
stop_monitor_ui.bat
```

这个脚本会强制停止本项目对应的本地 UI / Python 后台进程，并尝试一并清理同项目目录下遗留在其他端口上的历史实例。

macOS 对应脚本：

```bash
./start_monitor_ui_mac.sh
./stop_monitor_ui_mac.sh
./self_check_mac.sh
```

`start_monitor_ui_mac.sh` 现在会校验端口上的服务版本是否真的是当前代码版本；如果旧进程仍然占着 `8765`，脚本会直接报错退出，而不是误报“started”。

默认访问地址：

```text
http://127.0.0.1:8765
```

## GitHub Releases

仓库已接入 GitHub Actions 的 release 工作流：

- 工作流文件：`.github/workflows/release.yml`
- 触发方式：推送形如 `v*` 的 tag
- 打包环境：GitHub Actions 干净环境
- 产物内容：基于当前 tag 的源码 zip、tar.gz 和 SHA256 校验文件

发布新版本的最简流程：

```bash
git push origin main
git tag v20260330-sync-46
git push origin v20260330-sync-46
```

Tag 推上去之后，GitHub 会在干净环境里先安装依赖、跑测试，再自动创建 Release 并上传打包产物。

在 UI 中可以调整：

- 交易对列表，例如 `NIGHTUSDT,BTCUSDT,ETHUSDT`
- 滚动窗口分钟数，例如 `5,10,15`
- 日志刷新间隔（秒）
- 基线回看分钟数
- 可选的自动停止运行时长（秒）

UI 采用“结论优先”的展示方式：

- 顶部横幅：一行给出整体结论
- 交易对总览行：每个交易对一行结论
- 每个窗口默认只展示三个核心问题
  - 双向 taker 成交流是否显著放大
  - 在这种成交强度下，价格是否没有明显波动
  - 这种行为是否呈现机械化特征
- 次级指标收纳在可折叠的证据区域中

监控运行过程中，可以不停机追加新的交易对：

- 在交易对输入框中填入新增交易对
- 点击 `添加币种`
- UI 会保留当前正在监控的交易对，只启动尚未运行的新交易对

## CLI 运行方式

默认监控 `NIGHTUSDT`：

```bash
python monitor.py
```

常见示例：

```bash
python monitor.py --symbol NIGHTUSDT
python monitor.py --symbol NIGHTUSDT --report-interval 5
python monitor.py --symbol NIGHTUSDT --windows 300 600 900
python monitor.py --symbol NIGHTUSDT --runtime-seconds 120
python app.py --no-browser
```

## 输出字段说明

- `WARMING_UP`：脚本尚未采集满一个完整实时窗口，因此还不能给出稳定的 5 分钟或 10 分钟判断
- `quote`：窗口内 taker 成交额，以及相对基线的倍率
- `buy` / `sell`：taker 买入和 taker 卖出的成交额
- `balance`：`min(buy, sell) / max(buy, sell)`
- `trades`：成交笔数，以及相对基线的倍率
- `switch_rate`：taker 方向在买卖之间切换的频率
- `move`：窗口内绝对价格变动
- `book_churn`：最优买一 / 卖一更新速率
- `refill`：同价位盘口补单迹象的每分钟次数
- `vol/depth`：窗口 taker 成交额除以当前 0.2% 单边最小深度
- `vol/tw_depth`：窗口 taker 成交额除以采样得到的 0.2% 单边深度中位数
- `tw_slip`：窗口内采样得到的 10k 扫单滑点中位数
- `gap_cv`：成交间隔时间的变异系数；越低通常越像机械化流
- `size_top1`：使用最常见单一成交数量的成交占比
- `size_top5`：使用前五个最常见成交数量的成交占比
- `notional_top1`：落入最常见成交金额桶的成交占比
- `streak`：当前非正常状态已经持续了多久

UI 摘要中还会显示：

- 当前 0.2% 买盘深度和卖盘深度
- 10k 买入扫单和 10k 卖出扫单的估算滑点

## 阈值调节

默认参数设置得相对保守：

- `--active-volume-ratio 1.40`
- `--active-trade-ratio 1.15`
- `--active-balance 0.68`
- `--active-switch-rate 0.30`
- `--suspicious-volume-ratio 2.20`
- `--suspicious-trade-ratio 1.60`
- `--suspicious-balance 0.82`
- `--suspicious-switch-rate 0.45`
- `--suspicious-max-return 0.0040`

如果是噪音更大或流动性更强的交易对，可以适当提高可疑阈值。如果是流动性较薄的交易对，可以略微降低阈值，但需要同时关注误报率。
