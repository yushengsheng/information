from __future__ import annotations

import importlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


store = importlib.import_module("services.intel.store")


class IntelStoreTests(unittest.TestCase):
    def test_load_sent_registry_prunes_expired_keys(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            data_dir = Path(tmpdir)
            sent_file = data_dir / "intel_sent_registry.json"
            sent_file.write_text(
                json.dumps(
                    {
                        "keys": {
                            "stale-key": 1_000_000 - store.SENT_RETENTION_SECONDS - 60,
                            "fresh-key": 1_000_000 - 60,
                        },
                        "updated_at": 1_000_000 - 60,
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            with patch.object(store, "DATA_DIR", data_dir), patch.object(store, "INTEL_SENT_FILE", sent_file), patch.object(
                store.time,
                "time",
                autospec=True,
                return_value=1_000_000,
            ):
                payload = store.load_sent_registry()

            self.assertEqual(payload["keys"], {"fresh-key": 1_000_000 - 60})
            self.assertEqual(payload["retention_seconds"], store.SENT_RETENTION_SECONDS)

    def test_load_latest_digest_prefers_snapshot_and_exposes_latest_sent_sections(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            data_dir = Path(tmpdir)
            latest_file = data_dir / "intel_latest_digest.json"
            latest_sent_file = data_dir / "intel_latest_sent_digest.json"
            history_file = data_dir / "intel_digest_history.json"

            snapshot_payload = {
                "digest_date": "2026-03-30",
                "generated_at": "2026-03-30T09:00:00+08:00",
                "sections": {"crypto": [{"id": "snapshot-1"}], "world": [], "persistent": [], "hot": [], "custom": []},
                "final": False,
            }
            sent_payload = {
                "digest_date": "2026-03-29",
                "generated_at": "2026-03-29T08:00:00+08:00",
                "sections": {"crypto": [], "world": [{"id": "sent-1"}], "persistent": [], "hot": [], "custom": []},
                "final": True,
            }
            latest_file.write_text(json.dumps(snapshot_payload, ensure_ascii=False), encoding="utf-8")
            latest_sent_file.write_text(json.dumps(sent_payload, ensure_ascii=False), encoding="utf-8")
            history_file.write_text(
                json.dumps(
                    {
                        "version": 1,
                        "retention_days": 3,
                        "items": [{"digest_date": "2026-03-29", "payload": sent_payload}],
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            with patch.object(store, "DATA_DIR", data_dir), patch.object(store, "INTEL_LATEST_FILE", latest_file), patch.object(
                store,
                "INTEL_LATEST_SENT_FILE",
                latest_sent_file,
            ), patch.object(store, "INTEL_HISTORY_FILE", history_file):
                payload = store.load_latest_digest()

            self.assertTrue(payload["exists"])
            self.assertEqual(payload["display_source"], "snapshot")
            self.assertEqual(payload["digest_date"], "2026-03-30")
            self.assertTrue(payload["sent_exists"])
            self.assertEqual(payload["sent_digest_date"], "2026-03-29")
            self.assertEqual(payload["sent_sections"]["world"][0]["id"], "sent-1")

    def test_load_observability_history_prunes_expired_items(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            data_dir = Path(tmpdir)
            history_file = data_dir / "intel_observability_history.json"
            now_ts = 1_000_000
            history_file.write_text(
                json.dumps(
                    {
                        "version": 1,
                        "retention_seconds": store.OBSERVABILITY_RETENTION_SECONDS,
                        "items": [
                            {
                                "captured_at": now_ts - store.OBSERVABILITY_RETENTION_SECONDS - 60,
                                "alert_codes": ["stale_issue"],
                                "level": "warn",
                            },
                            {
                                "captured_at": now_ts - 60,
                                "alert_codes": ["fresh_issue"],
                                "level": "critical",
                                "repeat_codes": {"fresh_issue": 2},
                            },
                        ],
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            with patch.object(store, "DATA_DIR", data_dir), patch.object(
                store,
                "INTEL_OBSERVABILITY_HISTORY_FILE",
                history_file,
            ), patch.object(store.time, "time", autospec=True, return_value=now_ts):
                items = store.load_observability_history()

            self.assertEqual(len(items), 1)
            self.assertEqual(items[0]["alert_codes"], ["fresh_issue"])
            self.assertEqual(items[0]["repeat_codes"], {"fresh_issue": 2})


if __name__ == "__main__":
    unittest.main()
