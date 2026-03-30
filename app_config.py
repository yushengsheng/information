from __future__ import annotations

from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent
UI_DIR = ROOT_DIR / "ui"
DATA_DIR = ROOT_DIR / "data"

DEFAULT_PORT = 8765
PID_FILE = ROOT_DIR / ".monitor_ui.pid"
APP_VERSION = "20260330-sync-47"
INTEL_BACKGROUND_FETCH_INTERVAL_SECONDS = 2 * 60 * 60
INTEL_DAILY_AGGREGATE_WINDOW_SECONDS = 24 * 60 * 60

INTEL_CONFIG_FILE = DATA_DIR / "intel_config.json"
INTEL_LATEST_FILE = DATA_DIR / "intel_latest_digest.json"
INTEL_LATEST_SENT_FILE = DATA_DIR / "intel_latest_sent_digest.json"
INTEL_HISTORY_FILE = DATA_DIR / "intel_digest_history.json"
INTEL_OBSERVABILITY_HISTORY_FILE = DATA_DIR / "intel_observability_history.json"
INTEL_SNAPSHOT_POOL_FILE = DATA_DIR / "intel_snapshot_pool.json"
INTEL_EVENT_POOL_FILE = DATA_DIR / "intel_event_pool.json"
INTEL_SENT_FILE = DATA_DIR / "intel_sent_registry.json"
INTEL_SECRETS_FILE = DATA_DIR / "intel_secrets.json"
INTEL_DELIVERY_STATE_FILE = DATA_DIR / "intel_delivery_state.json"
INTEL_BUILD_TASK_FILE = DATA_DIR / "intel_build_task.json"
