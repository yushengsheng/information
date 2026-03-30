#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
VENV_DIR="$PROJECT_DIR/.venv"
REQ_FILE="$PROJECT_DIR/requirements.txt"
REQ_STAMP="$VENV_DIR/.requirements.sha256"
LOG_DIR="$PROJECT_DIR/logs"
PYTHON_BIN="/opt/homebrew/bin/python3.11"
LABEL="com.inverse.intel.daily"
PLIST_DIR="$HOME/Library/LaunchAgents"
PLIST_PATH="$PLIST_DIR/$LABEL.plist"
STDOUT_LOG="$LOG_DIR/intel_daily_launchd.out.log"
STDERR_LOG="$LOG_DIR/intel_daily_launchd.err.log"

if [[ ! -x "$PYTHON_BIN" ]]; then
  PYTHON_BIN="python3"
fi

calc_requirements_hash() {
  "$PYTHON_BIN" - <<'PY' "$REQ_FILE"
import hashlib,sys
raw=open(sys.argv[1],'rb').read()
print(hashlib.sha256(raw).hexdigest())
PY
}

mkdir -p "$PLIST_DIR" "$LOG_DIR"

if [[ ! -x "$VENV_DIR/bin/python" ]]; then
  "$PYTHON_BIN" -m venv "$VENV_DIR"
fi

CURRENT_HASH="$(calc_requirements_hash)"
LAST_HASH=""
if [[ -f "$REQ_STAMP" ]]; then
  LAST_HASH="$(cat "$REQ_STAMP" 2>/dev/null || true)"
fi

if [[ "$CURRENT_HASH" != "$LAST_HASH" ]]; then
  "$VENV_DIR/bin/python" -m pip -q install -U pip >/dev/null 2>&1 || true
  "$VENV_DIR/bin/python" -m pip -q install -r "$REQ_FILE"
  printf "%s" "$CURRENT_HASH" >"$REQ_STAMP"
fi

cat >"$PLIST_PATH" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>$LABEL</string>
  <key>ProgramArguments</key>
  <array>
    <string>$VENV_DIR/bin/python</string>
    <string>$PROJECT_DIR/scripts/intel_daily_job.py</string>
    <string>run-daily</string>
  </array>
  <key>WorkingDirectory</key>
  <string>$PROJECT_DIR</string>
  <key>RunAtLoad</key>
  <true/>
  <key>StartCalendarInterval</key>
  <dict>
    <key>Hour</key>
    <integer>8</integer>
    <key>Minute</key>
    <integer>0</integer>
  </dict>
  <key>EnvironmentVariables</key>
  <dict>
    <key>PATH</key>
    <string>/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin</string>
    <key>PYTHONUNBUFFERED</key>
    <string>1</string>
    <key>TZ</key>
    <string>Asia/Shanghai</string>
  </dict>
  <key>StandardOutPath</key>
  <string>$STDOUT_LOG</string>
  <key>StandardErrorPath</key>
  <string>$STDERR_LOG</string>
</dict>
</plist>
PLIST

launchctl bootout "gui/$(id -u)" "$PLIST_PATH" >/dev/null 2>&1 || true
BOOTSTRAP_OUTPUT="$(launchctl bootstrap "gui/$(id -u)" "$PLIST_PATH" 2>&1)" || true
if [[ -z "$BOOTSTRAP_OUTPUT" ]]; then
  launchctl enable "gui/$(id -u)/$LABEL" >/dev/null 2>&1 || true
  echo "installed"
else
  echo "plist_installed_only"
  echo "launchctl bootstrap failed in current shell: $BOOTSTRAP_OUTPUT"
  echo "请在你本机自己的终端里再执行一次 ./install_daily_intel_launchd.sh，或重新登录 macOS 后让 LaunchAgents 自动接管。"
fi

echo "plist: $PLIST_PATH"
echo "stdout: $STDOUT_LOG"
echo "stderr: $STDERR_LOG"
