#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
PID_FILE="$PROJECT_DIR/.monitor_ui.pid"
TMP_PID_LIST="$(mktemp -t info_blast_stop_pids.XXXXXX)"
TMP_REMNANT_LIST="$(mktemp -t info_blast_stop_remnants.XXXXXX)"
SELF_PID="$$"
PARENT_PID="${PPID:-0}"
GRANDPARENT_PID="$(ps -p "$PARENT_PID" -o ppid= 2>/dev/null | tr -d ' ' || true)"

cleanup_temp_files() {
  rm -f "$TMP_PID_LIST" "$TMP_REMNANT_LIST" >/dev/null 2>&1 || true
}
trap cleanup_temp_files EXIT

collect_pid() {
  local pid="$1"
  [[ -n "$pid" ]] || return 0
  [[ "$pid" =~ ^[0-9]+$ ]] || return 0
  printf "%s\n" "$pid" >>"$TMP_PID_LIST"
}

command_for_pid() {
  local pid="$1"
  ps -p "$pid" -o command= 2>/dev/null || true
}

command_name_for_pid() {
  local pid="$1"
  ps -p "$pid" -o comm= 2>/dev/null | tr -d ' ' || true
}

is_python_pid() {
  local pid="$1"
  local command_name
  command_name="$(command_name_for_pid "$pid")"
  command_name="$(printf '%s' "$command_name" | tr '[:upper:]' '[:lower:]')"
  [[ "$command_name" == python* ]]
}

is_protected_pid() {
  local pid="$1"
  [[ "$pid" == "$SELF_PID" ]] && return 0
  [[ "$pid" == "$PARENT_PID" ]] && return 0
  [[ -n "$GRANDPARENT_PID" && "$pid" == "$GRANDPARENT_PID" ]] && return 0
  return 1
}

kill_pid_forcefully() {
  local pid="$1"
  [[ -n "$pid" ]] || return 0
  [[ "$pid" =~ ^[0-9]+$ ]] || return 0

  kill "$pid" >/dev/null 2>&1 || true
  for _ in {1..20}; do
    if ! kill -0 "$pid" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.25
  done
  if kill -0 "$pid" >/dev/null 2>&1; then
    kill -9 "$pid" >/dev/null 2>&1 || true
  fi
}

collect_project_python_pids() {
  # 1) PID file
  if [[ -f "$PID_FILE" ]]; then
    pid_from_file="$(python3 - <<'PY' "$PID_FILE"
import json,sys
p=sys.argv[1]
try:
    data=json.load(open(p,'r',encoding='utf-8'))
    pid=int(data.get('pid',0))
    if pid>0:
        print(pid)
except Exception:
    pass
PY
)"
    collect_pid "${pid_from_file:-}"
  fi

  # 2) Any project-owned python entrypoints still running
  while IFS= read -r pid; do
    collect_pid "$pid"
  done < <(pgrep -f "$PROJECT_DIR/app.py" 2>/dev/null || true)

  while IFS= read -r pid; do
    collect_pid "$pid"
  done < <(pgrep -f "$PROJECT_DIR/monitor.py" 2>/dev/null || true)

  while IFS= read -r pid; do
    collect_pid "$pid"
  done < <(pgrep -f "$PROJECT_DIR/scripts/intel_daily_job.py" 2>/dev/null || true)

  # 2.5) Fallback: any process holding files under this project directory
  while IFS= read -r pid; do
    is_python_pid "$pid" || continue
    is_protected_pid "$pid" && continue
    collect_pid "$pid"
  done < <(lsof -t +D "$PROJECT_DIR" 2>/dev/null | sort -u || true)

  # 3) Any listening process whose command line points at this project
  while IFS= read -r pid; do
    [[ -n "$pid" ]] || continue
    is_python_pid "$pid" || continue
    is_protected_pid "$pid" && continue
    cmd="$(command_for_pid "$pid")"
    case "$cmd" in
      *"$PROJECT_DIR"*)
        collect_pid "$pid"
        ;;
    esac
  done < <(lsof -t -nP -iTCP -sTCP:LISTEN 2>/dev/null | sort -u || true)
}

collect_remaining_project_listeners() {
  : >"$TMP_REMNANT_LIST"

  while IFS= read -r pid; do
    [[ -n "$pid" ]] || continue
    is_python_pid "$pid" || continue
    while IFS= read -r line; do
      [[ -n "$line" ]] || continue
      printf "%s\n" "$line" >>"$TMP_REMNANT_LIST"
    done < <(lsof -Pan -p "$pid" -iTCP -sTCP:LISTEN 2>/dev/null | awk 'NR>1 {print $9}' || true)
  done < <(lsof -t +D "$PROJECT_DIR" 2>/dev/null | sort -u || true)

  while IFS= read -r pid; do
    [[ -n "$pid" ]] || continue
    is_python_pid "$pid" || continue
    cmd="$(command_for_pid "$pid")"
    case "$cmd" in
      *"$PROJECT_DIR"*)
        while IFS= read -r line; do
          [[ -n "$line" ]] || continue
          printf "%s\n" "$line" >>"$TMP_REMNANT_LIST"
        done < <(lsof -Pan -p "$pid" -iTCP -sTCP:LISTEN 2>/dev/null | awk 'NR>1 {print $9}' || true)
        ;;
    esac
  done < <(lsof -t -nP -iTCP -sTCP:LISTEN 2>/dev/null | sort -u || true)
}

collect_project_python_pids
sort -u "$TMP_PID_LIST" | while IFS= read -r pid; do
  [[ -n "$pid" ]] || continue
  kill_pid_forcefully "$pid"
done

rm -f "$PID_FILE" >/dev/null 2>&1 || true

collect_remaining_project_listeners
if [[ -s "$TMP_REMNANT_LIST" ]]; then
  remaining="$(sort -u "$TMP_REMNANT_LIST" | tr '\n' ' ' | sed 's/[[:space:]]\+$//')"
  echo "stopped (remaining listeners: $remaining)" >&2
  exit 1
fi

echo "stopped"
