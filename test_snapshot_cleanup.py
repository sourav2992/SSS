import os
import pytest
from datetime import datetime, timezone
from json import dumps
from unittest.mock import MagicMock, patch

# ── environment setup (must happen before any src imports) ────────────────────
os.environ["LOG_LEVEL"]                   = "INFO"
os.environ["REDIS_HOST"]                  = "fake_host"
os.environ["REDIS_AGENT_STATE_KEY"]       = "fake_key"
os.environ["REDIS_PASSWORD"]              = "fake_password"
os.environ["SNOWFLAKE_USERNAME"]          = "fake_snow_user"
os.environ["SNOWFLAKE_PASSWORD"]          = "fake_snow_pass"
os.environ["SNOWFLAKE_ACCOUNT"]           = "fake_snow_account"
os.environ["EXCHANGE_URL"]                = "https://fake-exchange.com"
os.environ["EXCHANGE_CLIENT_ID"]          = "fake_client_id"
os.environ["EXCHANGE_CLIENT_SECRET"]      = "fake_client_secret"
os.environ["AGENT_STALE_THRESHOLD_SECONDS"] = "36000"   # 10 hours
os.environ["AWS_REGION"]                  = "us-east-1"
os.environ["BA"]                          = "ba"
os.environ["BAP"]                         = "bap"
os.environ["POWERTOOLS_SERVICE_NAME"]     = "ent-voice-monitoring-agent-service-test"
os.environ["PUBLISH_DATASET"]             = "testdataset"

# ── helpers ───────────────────────────────────────────────────────────────────

def _now_ms():
    """Current time in milliseconds since epoch."""
    return datetime.now(timezone.utc).timestamp() * 1000


def _make_redis_mock(hgetall_data=None, hlen_return=0, hdel_return=0):
    """Build a MagicMock that looks like a redis.Redis instance."""
    mock_r = MagicMock()
    mock_r.ping.return_value  = True
    mock_r.hgetall.return_value = hgetall_data or {}
    mock_r.hlen.return_value  = hlen_return
    mock_r.hdel.return_value  = hdel_return
    return mock_r


# ─────────────────────────────────────────────────────────────────────────────
# Tests for cleanup_stale_agents()
#
# What cleanup_stale_agents() does:
#   1. Reads all agents from Redis hash via hgetall
#   2. Checks each agent's lastUpdate timestamp
#   3. If older than AGENT_STALE_THRESHOLD_SECONDS → marks for deletion
#   4. Deletes all stale agents with a single hdel call
#   5. Logs every removed EID for audit trail
#   6. Returns count of removed agents
# ─────────────────────────────────────────────────────────────────────────────

class TestCleanupStaleAgents:

    def test_removes_stale_agent(self, mocker):
        """
        cleanup_stale_agents() calls hdel for an agent whose
        lastUpdate is older than the threshold.
        """
        # agent last updated 48 hours ago — well past 10-hour threshold
        old_ts  = _now_ms() - (48 * 3600 * 1000)
        mock_r  = _make_redis_mock(
            hgetall_data={
                "STALE_EID": dumps({"lastUpdate": old_ts, "agentStatus": "On Call"})
            },
            hdel_return=1,
            hlen_return=0,
        )
        mocker.patch("src.snapshot.get_all_secrets", return_value={
            "EXCHANGE_CLIENT_ID": "x", "EXCHANGE_CLIENT_SECRET": "x",
            "REDIS_PASSWORD": "x", "SNOWFLAKE_USERNAME": "x", "SNOWFLAKE_PASSWORD": "x"
        })

        from src.snapshot import cleanup_stale_agents
        result = cleanup_stale_agents(mock_r)

        # hdel must be called with the stale EID
        mock_r.hdel.assert_called_once_with(
            os.environ["REDIS_AGENT_STATE_KEY"],
            "STALE_EID"
        )
        assert result == 1

    def test_keeps_fresh_agent(self, mocker):
        """
        cleanup_stale_agents() does NOT call hdel for an agent
        whose lastUpdate is within the threshold.
        """
        # agent last updated 1 hour ago — fresh
        fresh_ts = _now_ms() - (1 * 3600 * 1000)
        mock_r   = _make_redis_mock(
            hgetall_data={
                "FRESH_EID": dumps({"lastUpdate": fresh_ts, "agentStatus": "Online"})
            },
            hlen_return=1,
        )
        mocker.patch("src.snapshot.get_all_secrets", return_value={
            "EXCHANGE_CLIENT_ID": "x", "EXCHANGE_CLIENT_SECRET": "x",
            "REDIS_PASSWORD": "x", "SNOWFLAKE_USERNAME": "x", "SNOWFLAKE_PASSWORD": "x"
        })

        from src.snapshot import cleanup_stale_agents
        result = cleanup_stale_agents(mock_r)

        # hdel must NOT be called — agent is fresh
        mock_r.hdel.assert_not_called()
        assert result == 0

    def test_empty_redis_hash(self, mocker):
        """
        cleanup_stale_agents() handles an empty Redis hash gracefully —
        returns 0 and does not call hdel.
        """
        mock_r = _make_redis_mock(hgetall_data={}, hlen_return=0)
        mocker.patch("src.snapshot.get_all_secrets", return_value={
            "EXCHANGE_CLIENT_ID": "x", "EXCHANGE_CLIENT_SECRET": "x",
            "REDIS_PASSWORD": "x", "SNOWFLAKE_USERNAME": "x", "SNOWFLAKE_PASSWORD": "x"
        })

        from src.snapshot import cleanup_stale_agents
        result = cleanup_stale_agents(mock_r)

        mock_r.hdel.assert_not_called()
        assert result == 0

    def test_mixed_stale_and_fresh(self, mocker):
        """
        cleanup_stale_agents() removes only stale agents and keeps
        fresh ones when both exist in the hash.
        """
        old_ts   = _now_ms() - (48 * 3600 * 1000)   # 48h ago — stale
        fresh_ts = _now_ms() - (1  * 3600 * 1000)   # 1h ago  — fresh

        mock_r = _make_redis_mock(
            hgetall_data={
                "STALE_EID": dumps({"lastUpdate": old_ts,   "agentStatus": "On Call"}),
                "FRESH_EID": dumps({"lastUpdate": fresh_ts, "agentStatus": "Online"}),
            },
            hdel_return=1,
            hlen_return=1,
        )
        mocker.patch("src.snapshot.get_all_secrets", return_value={
            "EXCHANGE_CLIENT_ID": "x", "EXCHANGE_CLIENT_SECRET": "x",
            "REDIS_PASSWORD": "x", "SNOWFLAKE_USERNAME": "x", "SNOWFLAKE_PASSWORD": "x"
        })

        from src.snapshot import cleanup_stale_agents
        result = cleanup_stale_agents(mock_r)

        # hdel called with ONLY the stale EID — not the fresh one
        mock_r.hdel.assert_called_once_with(
            os.environ["REDIS_AGENT_STATE_KEY"],
            "STALE_EID"
        )
        assert result == 1

    def test_multiple_stale_agents_deleted_in_one_call(self, mocker):
        """
        cleanup_stale_agents() removes all stale agents in a SINGLE
        hdel call (not one call per agent).
        """
        old_ts = _now_ms() - (72 * 3600 * 1000)

        mock_r = _make_redis_mock(
            hgetall_data={
                "EID_A": dumps({"lastUpdate": old_ts, "agentStatus": "Online"}),
                "EID_B": dumps({"lastUpdate": old_ts, "agentStatus": "On Call"}),
                "EID_C": dumps({"lastUpdate": old_ts, "agentStatus": "Break"}),
            },
            hdel_return=3,
            hlen_return=0,
        )
        mocker.patch("src.snapshot.get_all_secrets", return_value={
            "EXCHANGE_CLIENT_ID": "x", "EXCHANGE_CLIENT_SECRET": "x",
            "REDIS_PASSWORD": "x", "SNOWFLAKE_USERNAME": "x", "SNOWFLAKE_PASSWORD": "x"
        })

        from src.snapshot import cleanup_stale_agents
        result = cleanup_stale_agents(mock_r)

        # hdel called exactly ONCE — not 3 times
        assert mock_r.hdel.call_count == 1
        assert result == 3

    def test_unparseable_record_treated_as_stale(self, mocker):
        """
        cleanup_stale_agents() treats a record that cannot be
        JSON-parsed as stale and includes it in hdel.
        """
        mock_r = _make_redis_mock(
            hgetall_data={
                "BAD_EID": "{{not valid json}}",
            },
            hdel_return=1,
            hlen_return=0,
        )
        mocker.patch("src.snapshot.get_all_secrets", return_value={
            "EXCHANGE_CLIENT_ID": "x", "EXCHANGE_CLIENT_SECRET": "x",
            "REDIS_PASSWORD": "x", "SNOWFLAKE_USERNAME": "x", "SNOWFLAKE_PASSWORD": "x"
        })

        from src.snapshot import cleanup_stale_agents
        result = cleanup_stale_agents(mock_r)

        mock_r.hdel.assert_called_once_with(
            os.environ["REDIS_AGENT_STATE_KEY"],
            "BAD_EID"
        )
        assert result == 1

    def test_calls_hlen_after_cleanup(self, mocker):
        """
        cleanup_stale_agents() calls hlen after hdel to log
        how many agents remain — audit trail requirement.
        """
        old_ts = _now_ms() - (48 * 3600 * 1000)
        mock_r = _make_redis_mock(
            hgetall_data={
                "EID001": dumps({"lastUpdate": old_ts, "agentStatus": "Online"})
            },
            hdel_return=1,
            hlen_return=5,
        )
        mocker.patch("src.snapshot.get_all_secrets", return_value={
            "EXCHANGE_CLIENT_ID": "x", "EXCHANGE_CLIENT_SECRET": "x",
            "REDIS_PASSWORD": "x", "SNOWFLAKE_USERNAME": "x", "SNOWFLAKE_PASSWORD": "x"
        })

        from src.snapshot import cleanup_stale_agents
        cleanup_stale_agents(mock_r)

        mock_r.hlen.assert_called_once_with(os.environ["REDIS_AGENT_STATE_KEY"])

    def test_returns_zero_when_no_stale_agents(self, mocker):
        """
        cleanup_stale_agents() returns 0 when all agents are fresh.
        """
        fresh_ts = _now_ms() - (1 * 3600 * 1000)
        mock_r   = _make_redis_mock(
            hgetall_data={
                "EID_FRESH": dumps({"lastUpdate": fresh_ts, "agentStatus": "Online"})
            },
            hlen_return=1,
        )
        mocker.patch("src.snapshot.get_all_secrets", return_value={
            "EXCHANGE_CLIENT_ID": "x", "EXCHANGE_CLIENT_SECRET": "x",
            "REDIS_PASSWORD": "x", "SNOWFLAKE_USERNAME": "x", "SNOWFLAKE_PASSWORD": "x"
        })

        from src.snapshot import cleanup_stale_agents
        result = cleanup_stale_agents(mock_r)

        assert result == 0


# ─────────────────────────────────────────────────────────────────────────────
# Tests for the midnight Eastern check in lambda_handler()
#
# What the midnight check does:
#   if now_eastern.hour == 0 and now_eastern.minute == 0:
#       cleanup_stale_agents(r)   ← runs ONCE at exactly 00:00 Eastern
#   else:
#       skip cleanup
#
# These tests verify:
#   - cleanup IS called at exactly midnight (hour=0, minute=0)
#   - cleanup is NOT called at any other minute during the midnight hour
#   - cleanup is NOT called at any non-midnight hour
# ─────────────────────────────────────────────────────────────────────────────

class TestMidnightCleanupCondition:

    def _base_mocks(self, mocker):
        """Set up all the mocks needed to run lambda_handler."""
        # mock secrets
        mocker.patch("src.snapshot.get_all_secrets", return_value={
            "EXCHANGE_CLIENT_ID": "fake_id",
            "EXCHANGE_CLIENT_SECRET": "fake_secret",
            "REDIS_PASSWORD": "fake_redis_pw",
            "SNOWFLAKE_USERNAME": "fake_snow_user",
            "SNOWFLAKE_PASSWORD": "fake_snow_pw",
        })
        # mock exchange token
        mocker.patch("src.snapshot.create_devex_requestor")
        # mock active region check — always active
        mocker.patch("src.snapshot.check_active_region", return_value=True)
        # mock cyber logger
        mocker.patch("src.snapshot.CyberLogger")
        mocker.patch("src.snapshot.init_cyber_logger")
        # mock wait_until so test doesn't actually wait
        mocker.patch("src.snapshot.wait_until")
        # mock Redis
        mock_r = _make_redis_mock(hgetall_data={}, hlen_return=0)
        mocker.patch("redis.Redis", return_value=mock_r)
        # mock snowflake
        mocker.patch("src.snapshot.get_snowflake_context", return_value=MagicMock())
        mocker.patch("src.snapshot.get_agent_data", return_value={})
        # mock process_agent_state so test doesn't hit postgres
        mocker.patch("src.snapshot.process_agent_state")
        # mock cleanup_stale_agents so we can assert on it
        cleanup_mock = mocker.patch("src.snapshot.cleanup_stale_agents", return_value=0)
        return mock_r, cleanup_mock

    def _make_event(self):
        """Standard scheduled event payload."""
        from datetime import timedelta
        one_min_ago = datetime.now(timezone.utc) - timedelta(minutes=1)
        return {
            "version": "0",
            "time": one_min_ago.isoformat().split(".")[0] + "Z",
        }

    def test_cleanup_runs_at_exactly_midnight(self, mocker):
        """
        cleanup_stale_agents() IS called when hour=0 AND minute=0.
        This is exactly 00:00 Eastern — the correct once-per-day trigger.
        """
        mock_r, cleanup_mock = self._base_mocks(mocker)

        # mock datetime.now(eastern) to return exactly midnight
        midnight = datetime(2026, 5, 21, 0, 0, 0)   # hour=0, minute=0
        mocker.patch("src.snapshot.datetime") \
              .now.side_effect = lambda tz=None: (
                  midnight if tz and str(tz) == "America/New_York"
                  else datetime.now(timezone.utc)
              )

        from src.snapshot import lambda_handler
        resp = lambda_handler(self._make_event(), context=None)

        cleanup_mock.assert_called_once_with(mock_r)

    def test_cleanup_does_not_run_at_midnight_minute_1(self, mocker):
        """
        cleanup_stale_agents() is NOT called at 00:01 Eastern.
        Ryan's fix — minute == 0 check prevents 60 runs per night.
        """
        mock_r, cleanup_mock = self._base_mocks(mocker)

        # 00:01 Eastern — one minute past midnight
        one_past = datetime(2026, 5, 21, 0, 1, 0)   # hour=0, minute=1
        mocker.patch("src.snapshot.datetime") \
              .now.side_effect = lambda tz=None: (
                  one_past if tz and str(tz) == "America/New_York"
                  else datetime.now(timezone.utc)
              )

        from src.snapshot import lambda_handler
        lambda_handler(self._make_event(), context=None)

        cleanup_mock.assert_not_called()

    def test_cleanup_does_not_run_at_noon(self, mocker):
        """
        cleanup_stale_agents() is NOT called during daytime hours.
        """
        mock_r, cleanup_mock = self._base_mocks(mocker)

        # 12:00 Eastern — midday
        noon = datetime(2026, 5, 21, 12, 0, 0)   # hour=12, minute=0
        mocker.patch("src.snapshot.datetime") \
              .now.side_effect = lambda tz=None: (
                  noon if tz and str(tz) == "America/New_York"
                  else datetime.now(timezone.utc)
              )

        from src.snapshot import lambda_handler
        lambda_handler(self._make_event(), context=None)

        cleanup_mock.assert_not_called()

    def test_cleanup_does_not_run_at_11pm(self, mocker):
        """
        cleanup_stale_agents() is NOT called at 23:00 Eastern.
        Confirms hour check is strict — only hour=0 qualifies.
        """
        mock_r, cleanup_mock = self._base_mocks(mocker)

        eleven_pm = datetime(2026, 5, 21, 23, 0, 0)   # hour=23
        mocker.patch("src.snapshot.datetime") \
              .now.side_effect = lambda tz=None: (
                  eleven_pm if tz and str(tz) == "America/New_York"
                  else datetime.now(timezone.utc)
              )

        from src.snapshot import lambda_handler
        lambda_handler(self._make_event(), context=None)

        cleanup_mock.assert_not_called()

    def test_cleanup_only_runs_once_across_60_invocations(self, mocker):
        """
        Simulates the Lambda firing every minute during the midnight hour
        (00:00 to 00:59 Eastern). cleanup_stale_agents() should be called
        exactly ONCE — only at minute=0.

        This is the direct regression test for Ryan's Issue 7 fix.
        Before the fix (hour==0 only): cleanup ran 60 times.
        After the fix (hour==0 AND minute==0): cleanup runs exactly 1 time.
        """
        cleanup_call_count = 0

        for minute in range(60):   # simulate all 60 minutes of the midnight hour
            mock_r, cleanup_mock = self._base_mocks(mocker)

            fake_time = datetime(2026, 5, 21, 0, minute, 0)
            mocker.patch("src.snapshot.datetime") \
                  .now.side_effect = lambda tz=None: (
                      fake_time if tz and str(tz) == "America/New_York"
                      else datetime.now(timezone.utc)
                  )

            from src.snapshot import lambda_handler
            import importlib
            import src.snapshot
            importlib.reload(src.snapshot)

            if cleanup_mock.called:
                cleanup_call_count += 1

        # cleanup should have been triggered exactly once (at minute=0)
        assert cleanup_call_count == 1, (
            f"Expected cleanup to run 1 time but it ran {cleanup_call_count} times. "
            f"Check that both hour==0 AND minute==0 are required."
        )
