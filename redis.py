# flake8: noqa
import os
import pytest
import json
import responses as responses_lib
from datetime import datetime, timedelta, timezone
from unittest.mock import patch, MagicMock, call
from json import dumps

import responses

base_url = "https://api-it.cloud.capitalone.com"


def set_env_vars():
    os.environ["ENVIRONMENT_GROUP"]  = "test"
    os.environ["ENVIRONMENT_REGION"] = "local"
    os.environ["VAULT_LOCKBOX"]      = "mytestlockbox"
    os.environ["AWS_REGION"]         = "us-east-1"
    os.environ["LOG_LEVEL"]             = "INFO"
    os.environ["REDIS_AGENT_STATE_KEY"] = "fake_key"
    os.environ["MINUTES_TO_RUN"]        = "1"
    os.environ["REDIS_HOST"]            = "fake_host"
    os.environ["EXCHANGE_URL"]          = base_url
    os.environ["EXCHANGE_CLIENT_ID"]    = "abc123"
    os.environ["DB_NAME"]               = "FAKE_INFLUX_URL"
    os.environ["DB_PORT"]               = "5432"
    os.environ[
        "VAULT_SNOWFLAKE_USERNAME_PATH"
    ] = "managed/84de47d0-485a-4285-bdc4-2cd52875c38d/ent_voice_user_key"
    os.environ[
        "VAULT_SNOWFLAKE_PASSWORD_PATH"
    ] = "managed/84de47d0-485a-4285-bdc4-2cd52875c38d/ent_voice_pass_key"
    os.environ["SNOWFLAKE_USERNAME"]      = "imc_snow"
    os.environ["SNOWFLAKE_ACCOUNT"]       = "cptlone-sfprodeast"
    os.environ["BOOTSTRAP_SERVERS"]       = "some servers"
    os.environ["SDP_ENV"]                 = "qa"
    os.environ["BA"]                      = "ba"
    os.environ["BAP"]                     = "bap"
    os.environ["POWERTOOLS_SERVICE_NAME"] = "ent-voice-monitoring-agent-service-test"
    os.environ["PUBLISH_DATASET"]         = "testdataset"
    os.environ["SNOWFLAKE_SECRET_PATH"]   = "qa-snowflake"
    os.environ["POSTGRES_SECRET_PATH"]    = "dev-postgres-secrets"
    os.environ["EXCHANGE_SECRET_PATH"]    = "exchange-preprod"
    os.environ["REDIS_SECRET_PATH"]       = "redis.password"
    os.environ["AGENT_STALE_THRESHOLD_SECONDS"] = "36000"


def set_vault_responses():
    for (key, path) in [
        ("redis.username",  "redis.username"),
        ("redis.password",  "redis.password"),
        ("password",        "snowflake/password"),
        ("secret",          "exchange/secret"),
        ("enterpriseVoiceDevx.clientId",     "enterpriseVoiceDevx.clientId"),
        ("enterpriseVoiceDevx.clientSecret", "enterpriseVoiceDevx.clientSecret"),
        ("enterpriseVoice.rdsPassword",      "enterpriseVoice.rdsPassword"),
        ("enterpriseVoice.rdsUserName",      "enterpriseVoice.rdsUserName"),
        ("enterpriseVoice.rdsHost",          "enterpriseVoice.rdsHost"),
        ("ent_voice_user_key",
         "managed/84de47d0-485a-4285-bdc4-2cd52875c38d/ent_voice_user_key"),
        ("ent_voice_pass_key",
         "managed/84de47d0-485a-4285-bdc4-2cd52875c38d/ent_voice_pass_key"),
    ]:
        responses.add(
            responses.GET,
            f"http://127.0.0.1:8200/v1/mytestlockbox/{path}",
            json={"data": {"key": "fake"}},
            status=200,
        )
    responses.add(
        responses.POST,
        f"{base_url}/oauth2/token",
        json={"access_token": "FAKE_ACCESS_TOKEN",
              "issued_at": datetime.now().second,
              "expires_in": 100000},
        status=200,
    )


@pytest.fixture
def mock_boto3_client(mocker):
    mock_client = mocker.Mock()
    mock_client.get_secret_value.side_effect = lambda SecretId: {
        "dev-postgres-secrets": {
            "SecretString": json.dumps({
                "username": "test_user",
                "password": "test_password",
                "host":     "test_host",
                "port":     "5432",
                "dbname":   "test_db",
            })
        },
        "qa-snowflake": {
            "SecretString": json.dumps({
                "username": "snowflake_user",
                "password": "snowflake_password",
            })
        },
        "exchange-preprod": {
            "SecretString": json.dumps({
                "clientId":     "test_client_id",
                "clientSecret": "test_client_secret",
            })
        },
        "redis.password": {
            "SecretString": json.dumps({
                "redis_token": "redis_test_token"
            })
        },
    }[SecretId]
    mocker.patch("boto3.session.Session.client", return_value=mock_client)
    return mock_client


def _now_ms():
    return datetime.now(timezone.utc).timestamp() * 1000



def _make_redis_mock(initial_data=None):
    """
    Returns a MagicMock that behaves like a Redis hash client.
    Supports: ping, hgetall, hset, hdel, hlen.
    Uses a real dict internally so all operations work correctly.
    """
    store = dict(initial_data or {})

    mock_r         = MagicMock()
    mock_r.ping.return_value = True

    # hgetall returns the whole store
    mock_r.hgetall.side_effect = lambda key: dict(store)

    # hset stores a field
    mock_r.hset.side_effect = lambda key, field=None, value=None, **kw: (
        store.update({field: value}) or 1
    )

    # hdel removes fields and returns count removed
    def _hdel(key, *fields):
        removed = 0
        for f in fields:
            if f in store:
                del store[f]
                removed += 1
        return removed
    mock_r.hdel.side_effect = _hdel

    # hlen returns current length
    mock_r.hlen.side_effect = lambda key: len(store)

    # expose store for assertions
    mock_r._store = store

    return mock_r



class TestCleanupStaleAgents:

    def _common_mocks(self, mocker, mock_boto3_client, redis_data=None):
        """
        Common mocks for all cleanup tests.
        Returns (mock_redis) ready to use.
        """
        set_env_vars()
        set_vault_responses()
        mocker.patch("snowflake.connector.connect", return_value=MagicMock())
        mocker.patch("src.lib.cyber_logging.ContextProvider")

        mock_redis = _make_redis_mock(redis_data or {})
        mocker.patch("redis.Redis", return_value=mock_redis)
        return mock_redis

    @responses.activate
    def test_removes_stale_agent(self, mocker, mock_boto3_client):
        """
        cleanup_stale_agents() removes an agent whose lastUpdate
        is older than the 10-hour threshold.
        """
        old_ts     = _now_ms() - (48 * 3600 * 1000)
        mock_redis = self._common_mocks(mocker, mock_boto3_client, {
            "STALE_EID": dumps({"lastUpdate": old_ts, "agentStatus": "On Call"})
        })

        from src.snapshot import cleanup_stale_agents
        result = cleanup_stale_agents(mock_redis)

        assert result == 1
        # field removed from store
        assert "STALE_EID" not in mock_redis._store

    @responses.activate
    def test_keeps_fresh_agent(self, mocker, mock_boto3_client):
        """
        cleanup_stale_agents() does NOT remove an agent whose
        lastUpdate is within the 10-hour threshold.
        """
        fresh_ts   = _now_ms() - (1 * 3600 * 1000)
        mock_redis = self._common_mocks(mocker, mock_boto3_client, {
            "FRESH_EID": dumps({"lastUpdate": fresh_ts, "agentStatus": "Online"})
        })

        from src.snapshot import cleanup_stale_agents
        result = cleanup_stale_agents(mock_redis)

        assert result == 0
        # field still in store
        assert "FRESH_EID" in mock_redis._store

    @responses.activate
    def test_empty_redis_hash(self, mocker, mock_boto3_client):
        """
        cleanup_stale_agents() handles empty Redis hash — returns 0.
        """
        mock_redis = self._common_mocks(mocker, mock_boto3_client, {})

        from src.snapshot import cleanup_stale_agents
        result = cleanup_stale_agents(mock_redis)

        assert result == 0

    @responses.activate
    def test_mixed_stale_and_fresh(self, mocker, mock_boto3_client):
        """
        cleanup_stale_agents() removes only stale agents and
        keeps fresh ones when both exist.
        """
        old_ts   = _now_ms() - (48 * 3600 * 1000)
        fresh_ts = _now_ms() - (1  * 3600 * 1000)

        mock_redis = self._common_mocks(mocker, mock_boto3_client, {
            "STALE_EID": dumps({"lastUpdate": old_ts,   "agentStatus": "On Call"}),
            "FRESH_EID": dumps({"lastUpdate": fresh_ts, "agentStatus": "Online"}),
        })

        from src.snapshot import cleanup_stale_agents
        result = cleanup_stale_agents(mock_redis)

        assert result == 1
        assert "STALE_EID" not in mock_redis._store
        assert "FRESH_EID" in mock_redis._store

    @responses.activate
    def test_multiple_stale_agents(self, mocker, mock_boto3_client):
        """
        cleanup_stale_agents() removes all stale agents when
        multiple exist in the hash.
        """
        old_ts     = _now_ms() - (72 * 3600 * 1000)
        mock_redis = self._common_mocks(mocker, mock_boto3_client, {
            "EID_A": dumps({"lastUpdate": old_ts, "agentStatus": "Online"}),
            "EID_B": dumps({"lastUpdate": old_ts, "agentStatus": "On Call"}),
            "EID_C": dumps({"lastUpdate": old_ts, "agentStatus": "Break"}),
        })

        from src.snapshot import cleanup_stale_agents
        result = cleanup_stale_agents(mock_redis)

        assert result == 3
        assert len(mock_redis._store) == 0

    @responses.activate
    def test_unparseable_record_treated_as_stale(self, mocker, mock_boto3_client):
        """
        cleanup_stale_agents() treats unparseable JSON as stale
        and removes it.
        """
        mock_redis = self._common_mocks(mocker, mock_boto3_client, {
            "BAD_EID": "{{not valid json}}",
        })

        from src.snapshot import cleanup_stale_agents
        result = cleanup_stale_agents(mock_redis)

        assert result == 1
        assert "BAD_EID" not in mock_redis._store

    @responses.activate
    def test_returns_zero_when_all_fresh(self, mocker, mock_boto3_client):
        """
        cleanup_stale_agents() returns 0 when all agents are fresh.
        """
        fresh_ts   = _now_ms() - (1 * 3600 * 1000)
        mock_redis = self._common_mocks(mocker, mock_boto3_client, {
            "EID_1": dumps({"lastUpdate": fresh_ts, "agentStatus": "Online"}),
            "EID_2": dumps({"lastUpdate": fresh_ts, "agentStatus": "Break"}),
        })

        from src.snapshot import cleanup_stale_agents
        result = cleanup_stale_agents(mock_redis)

        assert result == 0
        assert len(mock_redis._store) == 2




class TestMidnightCleanupCondition:

    def _make_event(self):
        one_min_ago = datetime.now(timezone.utc) - timedelta(minutes=1)
        return {
            "version": "0",
            "time": one_min_ago.isoformat().split(".")[0] + "Z",
        }

    def _patch_all(self, mocker, mock_boto3_client, eastern_hour, eastern_minute):
        set_env_vars()
        set_vault_responses()

        mocker.patch("snowflake.connector.connect", return_value=MagicMock())

        mock_redis = _make_redis_mock({})
        mocker.patch("redis.Redis", return_value=mock_redis)

        mocker.patch("src.snapshot.check_active_region", return_value=True)
        mocker.patch("src.lib.cyber_logging.ContextProvider")
        mocker.patch("src.snapshot.wait_until")
        mocker.patch("src.snapshot.get_snowflake_context", return_value=MagicMock())
        mocker.patch("src.snapshot.get_agent_data", return_value={})
        mocker.patch("src.snapshot.process_agent_state")

        cleanup_mock = mocker.patch(
            "src.snapshot.cleanup_stale_agents",
            return_value=0
        )


        fake_eastern         = MagicMock()
        fake_eastern.hour    = eastern_hour
        fake_eastern.minute  = eastern_minute
        fake_eastern.strftime.return_value = (
            f"{eastern_hour:02d}:{eastern_minute:02d} EDT"
        )

        _real_datetime = datetime

        def _smart_now(tz=None):
            # if called with a ZoneInfo timezone → return fake eastern time
            # if called with UTC or no arg → return real datetime (needed
            # for snapshot_time > current_time comparison in lambda_handler)
            if tz is not None and str(type(tz)) == "<class 'zoneinfo.ZoneInfo'>":
                return fake_eastern
            return _real_datetime.now(tz) if tz else _real_datetime.now(timezone.utc)

        mock_dt             = MagicMock(wraps=_real_datetime)
        mock_dt.now         = MagicMock(side_effect=_smart_now)
        mock_dt.now.return_value = _real_datetime.now(timezone.utc)
        mocker.patch("src.snapshot.datetime", mock_dt)

        # reset module globals
        import src.snapshot
        src.snapshot._devex_requestor = None
        src.snapshot._agent_data      = None

        return mock_redis, cleanup_mock

    @responses.activate
    def test_cleanup_runs_at_exactly_midnight(self, mocker, mock_boto3_client):
        """
        cleanup_stale_agents() IS called at hour=0, minute=0.
        Exactly midnight Eastern — correct once-per-day trigger.
        """
        mock_redis, cleanup_mock = self._patch_all(
            mocker, mock_boto3_client,
            eastern_hour=0, eastern_minute=0
        )

        from src.snapshot import lambda_handler
        lambda_handler(self._make_event(), context=None)

        cleanup_mock.assert_called_once_with(mock_redis)

    @responses.activate
    def test_cleanup_does_not_run_at_midnight_minute_1(
        self, mocker, mock_boto3_client
    ):
        """
        cleanup_stale_agents() NOT called at hour=0, minute=1.
        Ryan Issue 7: minute==0 check stops 60 runs per night.
        """
        mock_redis, cleanup_mock = self._patch_all(
            mocker, mock_boto3_client,
            eastern_hour=0, eastern_minute=1
        )

        from src.snapshot import lambda_handler
        lambda_handler(self._make_event(), context=None)

        cleanup_mock.assert_not_called()

    @responses.activate
    def test_cleanup_does_not_run_at_noon(self, mocker, mock_boto3_client):
        """
        cleanup_stale_agents() NOT called at 12:00 Eastern.
        """
        mock_redis, cleanup_mock = self._patch_all(
            mocker, mock_boto3_client,
            eastern_hour=12, eastern_minute=0
        )

        from src.snapshot import lambda_handler
        lambda_handler(self._make_event(), context=None)

        cleanup_mock.assert_not_called()

    @responses.activate
    def test_cleanup_does_not_run_at_11pm(self, mocker, mock_boto3_client):
        """
        cleanup_stale_agents() NOT called at 23:00 Eastern.
        """
        mock_redis, cleanup_mock = self._patch_all(
            mocker, mock_boto3_client,
            eastern_hour=23, eastern_minute=0
        )

        from src.snapshot import lambda_handler
        lambda_handler(self._make_event(), context=None)

        cleanup_mock.assert_not_called()