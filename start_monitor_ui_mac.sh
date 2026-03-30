#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
VENV_DIR="$PROJECT_DIR/.venv"
LOG_DIR="$PROJECT_DIR/logs"
LOG_FILE="$LOG_DIR/ui.log"
URL="http://127.0.0.1:8765"
REQ_FILE="$PROJECT_DIR/requirements.txt"
REQ_STAMP="$VENV_DIR/.requirements.sha256"
OPEN_BROWSER="${OPEN_BROWSER:-1}"

PYTHON_BIN="/opt/homebrew/bin/python3.11"
if [[ ! -x "$PYTHON_BIN" ]]; then
  PYTHON_BIN="python3"
fi

calc_requirements_hash() {
  "$PYTHON_BIN" - <<'PY' "$REQ_FILE"
import hashlib,sys
p=sys.argv[1]
raw=open(p,'rb').read()
print(hashlib.sha256(raw).hexdigest())
PY
}

expected_app_version() {
  "$PYTHON_BIN" - <<'PY' "$PROJECT_DIR/app_config.py"
import re,sys
text=open(sys.argv[1],encoding='utf-8').read()
match=re.search(r'^APP_VERSION\s*=\s*"([^"]+)"', text, re.M)
print(match.group(1) if match else "")
PY
}

health_matches_expected_version() {
  local payload
  payload="$(curl -fsS "$URL/health" 2>/dev/null || true)"
  [[ -n "$payload" ]] || return 1
  "$PYTHON_BIN" - <<'PY' "$payload" "$EXPECTED_VERSION"
import json,sys
try:
    data=json.loads(sys.argv[1])
except Exception:
    raise SystemExit(1)
raise SystemExit(0 if str(data.get("app_version") or "") == sys.argv[2] else 1)
PY
}

mkdir -p "$LOG_DIR"
EXPECTED_VERSION="$(expected_app_version)"

# Stop existing instance first
"$PROJECT_DIR/stop_monitor_ui_mac.sh" >/dev/null 2>&1 || true

if lsof -nP -iTCP:8765 -sTCP:LISTEN >/dev/null 2>&1; then
  echo "启动失败：端口 8765 仍被占用，无法切换到版本 $EXPECTED_VERSION" >&2
  lsof -nP -iTCP:8765 -sTCP:LISTEN >&2 || true
  exit 1
fi

# Ensure virtualenv
if [[ ! -x "$VENV_DIR/bin/python" ]]; then
  "$PYTHON_BIN" -m venv "$VENV_DIR"
fi

CURRENT_HASH="$(calc_requirements_hash)"
LAST_HASH=""
if [[ -f "$REQ_STAMP" ]]; then
  LAST_HASH="$(cat "$REQ_STAMP" 2>/dev/null || true)"
fi

# Install dependencies only when requirements changed (or first run)
if [[ "$CURRENT_HASH" != "$LAST_HASH" ]]; then
  "$VENV_DIR/bin/python" -m pip -q install -U pip >/dev/null 2>&1 || true
  "$VENV_DIR/bin/python" -m pip -q install -r "$REQ_FILE"
  printf "%s" "$CURRENT_HASH" >"$REQ_STAMP"
fi

# Start Flask UI in background
nohup "$VENV_DIR/bin/python" "$PROJECT_DIR/app.py" --host 127.0.0.1 --port 8765 --no-browser >>"$LOG_FILE" 2>&1 &
LAUNCHED_PID=$!

# Wait until the new build is ready
for _ in {1..40}; do
  if health_matches_expected_version && curl -fsS "$URL/api/intel/telegram/status" >/dev/null 2>&1; then
    if [[ "$OPEN_BROWSER" == "1" ]]; then
      open "$URL" >/dev/null 2>&1 || true
    fi
    echo "started"
    exit 0
  fi
  if ! kill -0 "$LAUNCHED_PID" >/dev/null 2>&1; then
    break
  fi
  sleep 0.5
done

CURRENT_HEALTH="$(curl -fsS "$URL/health" 2>/dev/null || true)"
echo "启动失败：新版本 $EXPECTED_VERSION 未在 20 秒内就绪。请查看日志：$LOG_FILE" >&2
if [[ -n "$CURRENT_HEALTH" ]]; then
  echo "当前端口返回：$CURRENT_HEALTH" >&2
fi
if lsof -nP -iTCP:8765 -sTCP:LISTEN >/dev/null 2>&1; then
  lsof -nP -iTCP:8765 -sTCP:LISTEN >&2 || true
fi
exit 1
