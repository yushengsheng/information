#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
PYTHON_BIN="/opt/homebrew/bin/python3.11"
if [[ ! -x "$PYTHON_BIN" ]]; then
  PYTHON_BIN="python3"
fi

say() {
  printf "[check] %s\n" "$1"
}

EXPECTED_VERSION="$("$PYTHON_BIN" - <<'PY' "$PROJECT_DIR/app_config.py"
import re,sys
text=open(sys.argv[1],encoding='utf-8').read()
match=re.search(r'^APP_VERSION\s*=\s*"([^"]+)"', text, re.M)
print(match.group(1) if match else "")
PY
)"

say "python 语法检查"
"$PYTHON_BIN" -m py_compile "$PROJECT_DIR/app.py" "$PROJECT_DIR/monitor.py"

say "确保服务关闭"
"$PROJECT_DIR/stop_monitor_ui_mac.sh" >/dev/null 2>&1 || true

say "后台启动（不打开浏览器）"
OPEN_BROWSER=0 "$PROJECT_DIR/start_monitor_ui_mac.sh" >/dev/null

say "健康检查 /health"
HEALTH_JSON="$(curl -fsS "http://127.0.0.1:8765/health")"
"$PYTHON_BIN" - <<'PY' "$HEALTH_JSON" "$EXPECTED_VERSION"
import json,sys
obj=json.loads(sys.argv[1])
assert obj.get('app_version') == sys.argv[2]
PY

say "状态检查 /api/state"
STATE_JSON="$(curl -fsS "http://127.0.0.1:8765/api/state")"
"$PYTHON_BIN" - <<'PY' "$STATE_JSON"
import json,sys
obj=json.loads(sys.argv[1])
assert isinstance(obj,dict)
assert obj.get('mode') in {'idle','starting','running','stopping','stopped','error'}
PY

say "TG 检查 /api/intel/telegram/status"
TG_JSON="$(curl -fsS "http://127.0.0.1:8765/api/intel/telegram/status")"
"$PYTHON_BIN" - <<'PY' "$TG_JSON"
import json,sys
obj=json.loads(sys.argv[1])
assert obj.get('ok') is True
assert 'telegram' in obj
PY

say "停止服务"
"$PROJECT_DIR/stop_monitor_ui_mac.sh" >/dev/null

sleep 1
if lsof -nP -iTCP:8765 -sTCP:LISTEN >/dev/null 2>&1; then
  echo "[error] 端口 8765 仍被占用" >&2
  lsof -nP -iTCP:8765 -sTCP:LISTEN >&2 || true
  exit 1
fi

echo "SELF_CHECK_OK"
