# flake8: noqa
import os
import pytest
import json
import responses as responses_lib
from datetime import datetime, timedelta, timezone
from unittest.mock import patch, MagicMock, call
from json import dumps

import responses

from tests.mocks.redis_client import RedisClientMock

base_url = "https://api-it.cloud.capitalone.com"


def set_env_vars():
    # setup environment vars — matches exactly test_snapshot_lambda_handler.py
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


# ─────────────────────────────────────────────────────────────────────────────
# PART 1 — Tests for cleanup_stale_agents()
# ─────────────────────────────────────────────────────────────────────────────

class TestCleanupStaleAgents:

    def _common_mocks(self, mocker, mock_boto3_client):
        """
        Common mocks for all cleanup tests.
        KEY FIX: mock snowflake.connector.connect so no real
        Snowflake connection is attempted during import or test run.
        """
        set_env_vars()
        set_vault_responses()
        # ── CRITICAL: stop Snowflake from connecting ──────────────────────────
        mocker.patch("snowflake.connector.connect", return_value=MagicMock())
        # ── stop cyber logger from hitting AWS ───────────────────────────────
        mocker.patch("src.lib.cyber_logging.ContextProvider")

    @responses.activate
    def test_removes_stale_agent(self, mocker, mock_boto3_client):
        """
        cleanup_stale_agents() removes an agent whose lastUpdate
        is older than the 10-hour threshold.
        """
        self._common_mocks(mocker, mock_boto3_client)

        old_ts     = _now_ms() - (48 * 3600 * 1000)
        mock_redis = RedisClientMock()
        mock_redis.hset("fake_key", "STALE_EID",
                        dumps({"lastUpdate": old_ts, "agentStatus": "On Call"}))
        mocker.patch("redis.Redis", return_value=mock_redis)

        from src.snapshot import cleanup_stale_agents
        result = cleanup_stale_agents(mock_redis)

        assert result >= 1

    @responses.activate
    def test_keeps_fresh_agent(self, mocker, mock_boto3_client):
        """
        cleanup_stale_agents() does NOT remove an agent whose
        lastUpdate is within the 10-hour threshold.
        """
        self._common_mocks(mocker, mock_boto3_client)

        fresh_ts   = _now_ms() - (1 * 3600 * 1000)
        mock_redis = RedisClientMock()
        mock_redis.hset("fake_key", "FRESH_EID",
                        dumps({"lastUpdate": fresh_ts, "agentStatus": "Online"}))
        mocker.patch("redis.Redis", return_value=mock_redis)

        from src.snapshot import cleanup_stale_agents
        result = cleanup_stale_agents(mock_redis)

        assert result == 0

    @responses.activate
    def test_empty_redis_hash(self, mocker, mock_boto3_client):
        """
        cleanup_stale_agents() handles empty Redis hash — returns 0.
        """
        self._common_mocks(mocker, mock_boto3_client)

        mock_redis = RedisClientMock()
        mocker.patch("redis.Redis", return_value=mock_redis)

        from src.snapshot import cleanup_stale_agents
        result = cleanup_stale_agents(mock_redis)

        assert result == 0

    @responses.activate
    def test_mixed_stale_and_fresh(self, mocker, mock_boto3_client):
        """
        cleanup_stale_agents() removes only stale agents and
        keeps fresh ones when both exist.
        """
        self._common_mocks(mocker, mock_boto3_client)

        old_ts   = _now_ms() - (48 * 3600 * 1000)
        fresh_ts = _now_ms() - (1  * 3600 * 1000)

        mock_redis = RedisClientMock()
        mock_redis.hset("fake_key", "STALE_EID",
                        dumps({"lastUpdate": old_ts,   "agentStatus": "On Call"}))
        mock_redis.hset("fake_key", "FRESH_EID",
                        dumps({"lastUpdate": fresh_ts, "agentStatus": "Online"}))
        mocker.patch("redis.Redis", return_value=mock_redis)

        from src.snapshot import cleanup_stale_agents
        result = cleanup_stale_agents(mock_redis)

        assert result == 1

    @responses.activate
    def test_multiple_stale_agents(self, mocker, mock_boto3_client):
        """
        cleanup_stale_agents() removes all stale agents when
        multiple exist in the hash.
        """
        self._common_mocks(mocker, mock_boto3_client)

        old_ts     = _now_ms() - (72 * 3600 * 1000)
        mock_redis = RedisClientMock()
        for eid in ["EID_A", "EID_B", "EID_C"]:
            mock_redis.hset("fake_key", eid,
                            dumps({"lastUpdate": old_ts, "agentStatus": "Online"}))
        mocker.patch("redis.Redis", return_value=mock_redis)

        from src.snapshot import cleanup_stale_agents
        result = cleanup_stale_agents(mock_redis)

        assert result == 3

    @responses.activate
    def test_unparseable_record_treated_as_stale(self, mocker, mock_boto3_client):
        """
        cleanup_stale_agents() treats unparseable JSON as stale
        and removes it.
        """
        self._common_mocks(mocker, mock_boto3_client)

        mock_redis = RedisClientMock()
        mock_redis.hset("fake_key", "BAD_EID", "{{not valid json}}")
        mocker.patch("redis.Redis", return_value=mock_redis)

        from src.snapshot import cleanup_stale_agents
        result = cleanup_stale_agents(mock_redis)

        assert result == 1

    @responses.activate
    def test_returns_zero_when_all_fresh(self, mocker, mock_boto3_client):
        """
        cleanup_stale_agents() returns 0 when all agents are fresh.
        """
        self._common_mocks(mocker, mock_boto3_client)

        fresh_ts   = _now_ms() - (1 * 3600 * 1000)
        mock_redis = RedisClientMock()
        for eid in ["EID_1", "EID_2"]:
            mock_redis.hset("fake_key", eid,
                            dumps({"lastUpdate": fresh_ts, "agentStatus": "Online"}))
        mocker.patch("redis.Redis", return_value=mock_redis)

        from src.snapshot import cleanup_stale_agents
        result = cleanup_stale_agents(mock_redis)

        assert result == 0


# ─────────────────────────────────────────────────────────────────────────────
# PART 2 — Tests for midnight Eastern check in lambda_handler()
# ─────────────────────────────────────────────────────────────────────────────

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

        # ── CRITICAL: stop Snowflake from connecting ──────────────────────────
        mocker.patch("snowflake.connector.connect", return_value=MagicMock())

        mock_redis = RedisClientMock()
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

        # mock eastern time to the hour/minute we want to test
        fake_eastern         = MagicMock()
        fake_eastern.hour    = eastern_hour
        fake_eastern.minute  = eastern_minute
        fake_eastern.strftime.return_value = (
            f"{eastern_hour:02d}:{eastern_minute:02d} EDT"
        )

        mock_dt      = MagicMock(wraps=datetime)
        mock_dt.now  = MagicMock(
            side_effect=lambda tz=None: (
                fake_eastern if tz is not None
                else datetime.now(timezone.utc)
            )
        )
        mocker.patch("src.snapshot.datetime", mock_dt)

        # reset module globals so handler runs fresh each test
        import src.snapshot
        src.snapshot._devex_requestor = None
        src.snapshot._agent_data      = None

        return mock_redis, cleanup_mock

    @responses.activate
    def test_cleanup_runs_at_exactly_midnight(self, mocker, mock_boto3_client):
        """
        cleanup_stale_agents() IS called at hour=0, minute=0.
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
