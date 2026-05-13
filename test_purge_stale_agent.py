# flake8: noqa
import os
import sys
import pytest
from datetime import datetime, timezone
from json import dumps
from unittest.mock import MagicMock, patch, call

# ── environment setup (must happen before importing the module) ────────────────
os.environ["REDIS_HOST"]          = "fake_host"
os.environ["REDIS_AGENT_STATE_KEY"] = "fake_key"
os.environ["REDIS_PASSWORD"]      = "fake_password"

# ── helpers ───────────────────────────────────────────────────────────────────

def _now_ms():
    """Current time in milliseconds since epoch."""
    return datetime.now(timezone.utc).timestamp() * 1000


def _make_redis_mock(hgetall_data=None, hlen_return=0, hdel_return=0, ping_return=True):
    """Build a MagicMock that looks like a redis.Redis instance."""
    mock_r = MagicMock()
    mock_r.ping.return_value = ping_return
    mock_r.hgetall.return_value = hgetall_data or {}
    mock_r.hlen.return_value = hlen_return
    mock_r.hdel.return_value = hdel_return
    return mock_r


# ── tests: connect() ──────────────────────────────────────────────────────────

class TestConnect:

    def test_connect_success(self, mocker):
        """connect() returns a Redis instance when ping succeeds."""
        mock_redis_instance = MagicMock()
        mock_redis_instance.ping.return_value = True
        mocker.patch("redis.Redis", return_value=mock_redis_instance)

        from src.purgeStaleAgent import connect
        result = connect()

        assert result == mock_redis_instance
        mock_redis_instance.ping.assert_called_once()

    def test_connect_uses_correct_env_vars(self, mocker):
        """connect() passes REDIS_HOST and REDIS_PASSWORD to Redis."""
        mock_redis_instance = MagicMock()
        mock_redis_cls = mocker.patch("redis.Redis", return_value=mock_redis_instance)

        from src.purgeStaleAgent import connect
        connect()

        call_kwargs = mock_redis_cls.call_args.kwargs
        assert call_kwargs["host"] == "fake_host"
        assert call_kwargs["password"] == "fake_password"
        assert call_kwargs["port"] == 6379
        assert call_kwargs["ssl"] is True
        assert call_kwargs["decode_responses"] is True

    def test_connect_raises_on_ping_failure(self, mocker):
        """connect() raises when Redis.ping() raises a ConnectionError."""
        mock_redis_instance = MagicMock()
        mock_redis_instance.ping.side_effect = Exception("Connection refused")
        mocker.patch("redis.Redis", return_value=mock_redis_instance)

        from src.purgeStaleAgent import connect
        with pytest.raises(Exception, match="Connection refused"):
            connect()


# ── tests: find_stale_eids() ──────────────────────────────────────────────────

class TestFindStaleEids:

    def test_returns_empty_when_no_agents(self):
        """find_stale_eids() returns [] when Redis hash is empty."""
        mock_r = _make_redis_mock(hgetall_data={})

        from src.purgeStaleAgent import find_stale_eids
        result = find_stale_eids(mock_r, threshold_hours=24)

        assert result == []

    def test_returns_stale_agent(self):
        """find_stale_eids() returns agents whose lastUpdate is older than threshold."""
        # 48 hours ago in ms
        old_ts = _now_ms() - (48 * 3600 * 1000)
        mock_r = _make_redis_mock(hgetall_data={
            "EID001": dumps({"lastUpdate": old_ts, "agentStatus": "On Call"}),
        })

        from src.purgeStaleAgent import find_stale_eids
        result = find_stale_eids(mock_r, threshold_hours=24)

        assert len(result) == 1
        assert result[0]["eid"] == "EID001"
        assert result[0]["agent_status"] == "On Call"
        assert result[0]["age_hours"] > 24

    def test_does_not_return_fresh_agent(self):
        """find_stale_eids() does NOT return agents updated within the threshold."""
        # 1 hour ago — fresh
        fresh_ts = _now_ms() - (1 * 3600 * 1000)
        mock_r = _make_redis_mock(hgetall_data={
            "EID002": dumps({"lastUpdate": fresh_ts, "agentStatus": "Online"}),
        })

        from src.purgeStaleAgent import find_stale_eids
        result = find_stale_eids(mock_r, threshold_hours=24)

        assert result == []

    def test_mixed_stale_and_fresh(self):
        """find_stale_eids() only returns stale agents from a mixed set."""
        old_ts   = _now_ms() - (72 * 3600 * 1000)   # 3 days ago
        fresh_ts = _now_ms() - (1  * 3600 * 1000)   # 1 hour ago

        mock_r = _make_redis_mock(hgetall_data={
            "STALE_EID": dumps({"lastUpdate": old_ts,   "agentStatus": "On Call"}),
            "FRESH_EID": dumps({"lastUpdate": fresh_ts, "agentStatus": "Online"}),
        })

        from src.purgeStaleAgent import find_stale_eids
        result = find_stale_eids(mock_r, threshold_hours=24)

        eids = [r["eid"] for r in result]
        assert "STALE_EID" in eids
        assert "FRESH_EID" not in eids

    def test_unparseable_record_treated_as_stale(self):
        """find_stale_eids() treats records that cannot be JSON-parsed as stale."""
        mock_r = _make_redis_mock(hgetall_data={
            "BAD_EID": "this is not valid json {{{{",
        })

        from src.purgeStaleAgent import find_stale_eids
        result = find_stale_eids(mock_r, threshold_hours=24)

        assert len(result) == 1
        assert result[0]["eid"] == "BAD_EID"
        assert result[0]["agent_status"] == "UNKNOWN (parse error)"
        assert result[0]["last_update_ms"] == 0
        assert result[0]["age_hours"] is None

    def test_missing_lastUpdate_defaults_to_zero(self):
        """find_stale_eids() treats agents with no lastUpdate field as stale."""
        mock_r = _make_redis_mock(hgetall_data={
            "NO_TS_EID": dumps({"agentStatus": "Online"}),  # no lastUpdate key
        })

        from src.purgeStaleAgent import find_stale_eids
        result = find_stale_eids(mock_r, threshold_hours=24)

        assert len(result) == 1
        assert result[0]["eid"] == "NO_TS_EID"

    def test_multiple_stale_agents(self):
        """find_stale_eids() returns all stale agents when multiple exist."""
        old_ts = _now_ms() - (100 * 3600 * 1000)
        mock_r = _make_redis_mock(hgetall_data={
            "EID_A": dumps({"lastUpdate": old_ts, "agentStatus": "Online"}),
            "EID_B": dumps({"lastUpdate": old_ts, "agentStatus": "On Call"}),
            "EID_C": dumps({"lastUpdate": old_ts, "agentStatus": "Break"}),
        })

        from src.purgeStaleAgent import find_stale_eids
        result = find_stale_eids(mock_r, threshold_hours=24)

        assert len(result) == 3
        eids = {r["eid"] for r in result}
        assert eids == {"EID_A", "EID_B", "EID_C"}

    def test_age_hours_calculated_correctly(self):
        """find_stale_eids() calculates age_hours accurately."""
        hours_ago = 50
        old_ts = _now_ms() - (hours_ago * 3600 * 1000)
        mock_r = _make_redis_mock(hgetall_data={
            "EID_AGE": dumps({"lastUpdate": old_ts, "agentStatus": "Online"}),
        })

        from src.purgeStaleAgent import find_stale_eids
        result = find_stale_eids(mock_r, threshold_hours=24)

        assert len(result) == 1
        # Allow 0.1h tolerance for test execution time
        assert abs(result[0]["age_hours"] - hours_ago) < 0.1

    def test_custom_threshold_hours(self):
        """find_stale_eids() respects a custom threshold_hours value."""
        # 2 hours ago — stale under 1h threshold, fresh under 24h
        ts_2h_ago = _now_ms() - (2 * 3600 * 1000)
        mock_r = _make_redis_mock(hgetall_data={
            "EID_2H": dumps({"lastUpdate": ts_2h_ago, "agentStatus": "Online"}),
        })

        from src.purgeStaleAgent import find_stale_eids

        # With 1h threshold → stale
        result_1h = find_stale_eids(mock_r, threshold_hours=1)
        assert len(result_1h) == 1

        # With 24h threshold → fresh
        result_24h = find_stale_eids(mock_r, threshold_hours=24)
        assert len(result_24h) == 0


# ── tests: print_report() ─────────────────────────────────────────────────────

class TestPrintReport:

    def test_prints_no_stale_message_when_empty(self, capsys):
        """print_report() prints a 'no stale records' message for empty list."""
        from src.purgeStaleAgent import print_report
        print_report([], threshold_hours=24)

        captured = capsys.readouterr()
        assert "No stale records found" in captured.out

    def test_prints_stale_agents(self, capsys):
        """print_report() prints EID, age, and status for each stale agent."""
        stale = [
            {"eid": "EID001", "age_hours": 48.0, "agent_status": "On Call"},
            {"eid": "EID002", "age_hours": 72.5, "agent_status": "Online"},
        ]
        from src.purgeStaleAgent import print_report
        print_report(stale, threshold_hours=24)

        captured = capsys.readouterr()
        assert "EID001" in captured.out
        assert "EID002" in captured.out
        assert "On Call" in captured.out
        assert "Online" in captured.out

    def test_prints_na_for_none_age(self, capsys):
        """print_report() prints 'N/A' for records with age_hours=None."""
        stale = [
            {"eid": "BAD_EID", "age_hours": None, "agent_status": "UNKNOWN (parse error)"},
        ]
        from src.purgeStaleAgent import print_report
        print_report(stale, threshold_hours=24)

        captured = capsys.readouterr()
        assert "N/A" in captured.out
        assert "BAD_EID" in captured.out

    def test_prints_threshold_in_header(self, capsys):
        """print_report() includes the threshold value in the header."""
        from src.purgeStaleAgent import print_report
        print_report([], threshold_hours=48)

        captured = capsys.readouterr()
        assert "48" in captured.out


# ── tests: purge() ────────────────────────────────────────────────────────────

class TestPurge:

    def test_prints_nothing_to_delete_when_empty(self, capsys):
        """purge() prints 'Nothing to delete' when stale list is empty."""
        mock_r = _make_redis_mock()

        from src.purgeStaleAgent import purge
        purge(mock_r, stale=[], dry_run=False)

        captured = capsys.readouterr()
        assert "Nothing to delete" in captured.out
        mock_r.hdel.assert_not_called()

    def test_dry_run_does_not_call_hdel(self, capsys):
        """purge() with dry_run=True prints what would be deleted but does NOT call hdel."""
        mock_r = _make_redis_mock()
        stale = [
            {"eid": "EID001", "age_hours": 48.0, "agent_status": "Online"},
        ]

        from src.purgeStaleAgent import purge
        purge(mock_r, stale=stale, dry_run=True)

        mock_r.hdel.assert_not_called()
        captured = capsys.readouterr()
        assert "DRY RUN" in captured.out
        assert "EID001" in captured.out

    def test_dry_run_lists_all_eids(self, capsys):
        """purge() dry run prints every EID that would be deleted."""
        mock_r = _make_redis_mock()
        stale = [
            {"eid": "EID_A", "age_hours": 50.0, "agent_status": "On Call"},
            {"eid": "EID_B", "age_hours": 60.0, "agent_status": "Online"},
        ]

        from src.purgeStaleAgent import purge
        purge(mock_r, stale=stale, dry_run=True)

        captured = capsys.readouterr()
        assert "EID_A" in captured.out
        assert "EID_B" in captured.out

    def test_live_run_calls_hdel_with_all_eids(self):
        """purge() with dry_run=False calls hdel with all stale EIDs."""
        mock_r = _make_redis_mock(hdel_return=2, hlen_return=5)
        stale = [
            {"eid": "EID001", "age_hours": 48.0, "agent_status": "Online"},
            {"eid": "EID002", "age_hours": 72.0, "agent_status": "On Call"},
        ]

        from src.purgeStaleAgent import purge
        purge(mock_r, stale=stale, dry_run=False)

        mock_r.hdel.assert_called_once_with(
            os.environ["REDIS_AGENT_STATE_KEY"],
            "EID001",
            "EID002",
        )

    def test_live_run_prints_deleted_count(self, capsys):
        """purge() prints how many records were deleted after hdel."""
        mock_r = _make_redis_mock(hdel_return=3, hlen_return=2)
        stale = [
            {"eid": "EID_X", "age_hours": 50.0, "agent_status": "Online"},
            {"eid": "EID_Y", "age_hours": 60.0, "agent_status": "Break"},
            {"eid": "EID_Z", "age_hours": 70.0, "agent_status": "On Call"},
        ]

        from src.purgeStaleAgent import purge
        purge(mock_r, stale=stale, dry_run=False)

        captured = capsys.readouterr()
        assert "3" in captured.out   # deleted count
        assert "2" in captured.out   # remaining count

    def test_live_run_checks_remaining_after_delete(self):
        """purge() calls hlen to report remaining agents after deletion."""
        mock_r = _make_redis_mock(hdel_return=1, hlen_return=0)
        stale = [{"eid": "EID001", "age_hours": 48.0, "agent_status": "Online"}]

        from src.purgeStaleAgent import purge
        purge(mock_r, stale=stale, dry_run=False)

        mock_r.hlen.assert_called_once_with(os.environ["REDIS_AGENT_STATE_KEY"])


# ── tests: main() ─────────────────────────────────────────────────────────────

class TestMain:

    def test_main_success_flow(self, mocker, capsys):
        """main() connects, finds stale agents, prints report, and purges."""
        old_ts = _now_ms() - (48 * 3600 * 1000)
        mock_r = _make_redis_mock(
            hgetall_data={"EID001": dumps({"lastUpdate": old_ts, "agentStatus": "Online"})},
            hlen_return=0,
            hdel_return=1,
        )
        mocker.patch("src.purgeStaleAgent.connect", return_value=mock_r)
        mocker.patch("sys.argv", ["purge_stale_agents.py"])

        from src.purgeStaleAgent import main
        main()

        mock_r.hdel.assert_called_once()
        captured = capsys.readouterr()
        assert "EID001" in captured.out

    def test_main_dry_run_flag(self, mocker, capsys):
        """main() respects --dry-run flag and does not call hdel."""
        old_ts = _now_ms() - (48 * 3600 * 1000)
        mock_r = _make_redis_mock(
            hgetall_data={"EID001": dumps({"lastUpdate": old_ts, "agentStatus": "Online"})},
        )
        mocker.patch("src.purgeStaleAgent.connect", return_value=mock_r)
        mocker.patch("sys.argv", ["purge_stale_agents.py", "--dry-run"])

        from src.purgeStaleAgent import main
        main()

        mock_r.hdel.assert_not_called()
        captured = capsys.readouterr()
        assert "DRY RUN" in captured.out

    def test_main_custom_threshold(self, mocker, capsys):
        """main() passes --threshold-hours to find_stale_eids."""
        mock_r = _make_redis_mock(hgetall_data={})
        mocker.patch("src.purgeStaleAgent.connect", return_value=mock_r)
        mocker.patch("sys.argv", ["purge_stale_agents.py", "--threshold-hours", "48"])

        find_stale_mock = mocker.patch(
            "src.purgeStaleAgent.find_stale_eids", return_value=[]
        )

        from src.purgeStaleAgent import main
        main()

        find_stale_mock.assert_called_once_with(mock_r, 48.0)

    def test_main_exits_on_connection_error(self, mocker, capsys):
        """main() calls sys.exit(1) when Redis connection fails."""
        mocker.patch(
            "src.purgeStaleAgent.connect",
            side_effect=Exception("Cannot connect")
        )
        mocker.patch("sys.argv", ["purge_stale_agents.py"])

        from src.purgeStaleAgent import main
        with pytest.raises(SystemExit) as exc_info:
            main()

        assert exc_info.value.code == 1
        captured = capsys.readouterr()
        assert "ERROR" in captured.out
        assert "Cannot connect" in captured.out

    def test_main_no_stale_agents(self, mocker, capsys):
        """main() prints 'Nothing to delete' when all agents are fresh."""
        fresh_ts = _now_ms() - (1 * 3600 * 1000)
        mock_r = _make_redis_mock(
            hgetall_data={"EID_FRESH": dumps({"lastUpdate": fresh_ts, "agentStatus": "Online"})},
        )
        mocker.patch("src.purgeStaleAgent.connect", return_value=mock_r)
        mocker.patch("sys.argv", ["purge_stale_agents.py"])

        from src.purgeStaleAgent import main
        main()

        mock_r.hdel.assert_not_called()
        captured = capsys.readouterr()
        assert "Nothing to delete" in captured.out

    def test_main_prints_total_agent_count(self, mocker, capsys):
        """main() prints the total number of agents in the Redis hash."""
        mock_r = _make_redis_mock(hgetall_data={}, hlen_return=42)
        mocker.patch("src.purgeStaleAgent.connect", return_value=mock_r)
        mocker.patch("sys.argv", ["purge_stale_agents.py"])

        from src.purgeStaleAgent import main
        main()

        captured = capsys.readouterr()
        assert "42" in captured.out
