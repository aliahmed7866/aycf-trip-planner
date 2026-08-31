import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import watch_service


class WatchNotificationTests(unittest.TestCase):
    def test_notification_status_reports_missing_termux_api(self):
        with patch.dict("os.environ", {"AYCF_NOTIFICATIONS": "true"}, clear=False), patch("watch_service.shutil.which", return_value=None):
            status = watch_service.notification_status()
        self.assertFalse(status["ok"])
        self.assertTrue(status["enabled"])
        self.assertFalse(status["available"])
        self.assertIn("termux-api", status["detail"])

    def test_failed_notification_is_retried_on_next_watch_check(self):
        with tempfile.TemporaryDirectory() as td:
            db_path = str(Path(td) / "watches.sqlite3")
            travel_day = date.today() + timedelta(days=2)
            watch_service.add_watch("Liverpool", "Budapest", travel_day.isoformat(), path=db_path)

            class DummyScanDB:
                pass

            with patch("watch_service.available_dates_for_watch", return_value={travel_day}), patch(
                "watch_service.send_termux_notification",
                side_effect=[(False, "Termux API permission denied"), (True, "Notification submitted to Android.")],
            ):
                first = watch_service.check_watches(DummyScanDB(), notify=True, path=db_path)
                second = watch_service.check_watches(DummyScanDB(), notify=True, path=db_path)

            self.assertEqual(first["new_matches"], 1)
            self.assertEqual(first["notifications"], 0)
            self.assertEqual(first["notification_failures"], 1)
            self.assertEqual(second["new_matches"], 0)
            self.assertEqual(second["notifications"], 1)
            self.assertEqual(second["notification_failures"], 0)

            match = watch_service.recent_matches(path=db_path, limit=1)[0]
            self.assertIsNotNone(match["notified_at"])
            self.assertIsNone(match["notification_error"])
            self.assertIsNotNone(match["notification_attempted_at"])

    def test_existing_database_is_migrated_with_notification_diagnostics(self):
        with tempfile.TemporaryDirectory() as td:
            db_path = str(Path(td) / "watches.sqlite3")
            with watch_service._connect(db_path) as conn:
                conn.executescript("""
                CREATE TABLE flight_watches (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    origin TEXT NOT NULL,
                    destination TEXT NOT NULL,
                    date_from TEXT NOT NULL,
                    date_to TEXT NOT NULL,
                    enabled INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL,
                    last_checked_at TEXT,
                    last_error TEXT,
                    UNIQUE(origin, destination, date_from, date_to)
                );
                CREATE TABLE flight_watch_matches (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    watch_id INTEGER NOT NULL REFERENCES flight_watches(id) ON DELETE CASCADE,
                    flight_date TEXT NOT NULL,
                    first_seen_at TEXT NOT NULL,
                    last_seen_at TEXT NOT NULL,
                    available INTEGER NOT NULL DEFAULT 1,
                    notified_at TEXT,
                    UNIQUE(watch_id, flight_date)
                );
                """)
            watch_service.init_watch_db(db_path)
            with watch_service._connect(db_path) as conn:
                columns = {row["name"] for row in conn.execute("PRAGMA table_info(flight_watch_matches)")}
            self.assertIn("notification_attempted_at", columns)
            self.assertIn("notification_error", columns)

    def test_run_notification_uses_named_channel_when_supported(self):
        notification = "/usr/bin/termux-notification"
        channel = "/usr/bin/termux-notification-channel"

        def which(name):
            return {
                "termux-notification": notification,
                "termux-notification-channel": channel,
                "termux-notification-list": None,
            }.get(name)

        channel_proc = MagicMock(returncode=0, stdout="", stderr="")
        notify_proc = MagicMock(returncode=0, stdout="", stderr="")
        with patch("watch_service.shutil.which", side_effect=which), patch(
            "watch_service.subprocess.run", side_effect=[channel_proc, notify_proc]
        ) as run:
            sent, detail = watch_service._run_notification("Title", "Body", "990001")

        self.assertTrue(sent)
        self.assertIn("AYCF flight alerts", detail)
        self.assertEqual(run.call_args_list[0].args[0], [channel, "aycf-flight-alerts", "AYCF flight alerts"])
        notify_cmd = run.call_args_list[1].args[0]
        self.assertIn("--channel", notify_cmd)
        self.assertEqual(notify_cmd[notify_cmd.index("--channel") + 1], "aycf-flight-alerts")

    def test_unconfirmed_notification_list_is_not_treated_as_post_failure(self):
        notification = "/usr/bin/termux-notification"
        channel = "/usr/bin/termux-notification-channel"
        verifier = "/usr/bin/termux-notification-list"

        def which(name):
            return {
                "termux-notification": notification,
                "termux-notification-channel": channel,
                "termux-notification-list": verifier,
            }.get(name)

        ok = MagicMock(returncode=0, stdout="", stderr="")
        empty_list = MagicMock(returncode=0, stdout="[]", stderr="")
        with patch("watch_service.shutil.which", side_effect=which), patch(
            "watch_service.subprocess.run", side_effect=[ok, ok, empty_list]
        ), patch("watch_service.time.sleep"):
            sent, detail = watch_service._run_notification("Title", "Body", "990001")

        self.assertTrue(sent)
        self.assertIn("does not prove posting failed", detail)


if __name__ == "__main__":
    unittest.main()
