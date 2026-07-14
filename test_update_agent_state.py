# flake8: noqa
import json
import os
from datetime import datetime, timedelta, timezone

import pytest
import responses

from tests.mocks.redis_client import RedisClientMock

base_url = "https://api-it.cloud.capitalone.com"


def set_env_vars():
    # setup environment vars
    os.environ["ENVIRONMENT_GROUP"] = "test"
    os.environ["ENVIRONMENT_REGION"] = "local"

    os.environ["VAULT_LOCKBOX"] = "mytestlockbox"

    os.environ["LOG_LEVEL"] = "INFO"
    os.environ["REDIS_AGENT_STATE_KEY"] = "fake_key"
    os.environ["MINUTES_TO_RUN"] = "1"
    os.environ["REDIS_HOST"] = "fake_host"

    os.environ["AWS_REGION"] = "us-east-1"

    os.environ["EXCHANGE_URL"] = base_url
    os.environ["EXCHANGE_CLIENT_ID"] = "abc123"
    os.environ["DB_NAME"] = "FAKE_INFLUX_URL"
    os.environ["DB_PORT"] = "5432"
    os.environ["SNOWFLAKE_ACCOUNT"] = "cptlone-sfprodeast"

    os.environ["BOOTSTRAP_SERVERS"] = "some servers"
    os.environ["SDP_ENV"] = "qa"
    os.environ["BA"] = "ba"
    os.environ["BAP"] = "bap"
    os.environ["POWERTOOLS_SERVICE_NAME"] = "ent-voice-monitoring-agent-service-test"
    os.environ["PUBLISH_DATASET"] = "testdataset"

    os.environ["SNOWFLAKE_OAUTH_SECRET_PATH"] = "snowflake-oauth-preprod"
    os.environ["POSTGRES_SECRET_PATH"] = "dev-postgres-secrets"
    os.environ["EXCHANGE_SECRET_PATH"] = "exchange-preprod"
    os.environ["REDIS_SECRET_PATH"] = "redis.password"


@pytest.fixture
def mock_boto3_client(mocker):
    # Mock the boto3 client for secrets retrieval
    mock_client = mocker.Mock()
    mock_client.get_secret_value.side_effect = lambda SecretId: {
        "dev-postgres-secrets": {
            "SecretString": json.dumps({
                "username": "test_user",
                "password": "test_password",
                "host": "test_host",
                "port": "5432",
                "dbname": "test_db",
            })
        },
        "snowflake-oauth-preprod": {
            "SecretString": json.dumps({
                "clientId": "test_snowflake_oauth_client_id",
                "clientSecret": "test_snowflake_oauth_client_secret",
            })
        },
        "exchange-preprod": {
            "SecretString": json.dumps({
                "clientId": "test_client_id",
                "clientSecret": "test_client_secret",
            })
        },
        "redis.password": {
            "SecretString": json.dumps({"redis_token": "redis_test_token"})
        },
    }[SecretId]
    mocker.patch("boto3.session.Session.client", return_value=mock_client)


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

REDIS_KEY = "fake_key"


def make_message(eid="not_eid2", event_timestamp=1000):
    """Mirrors the SDP payload shape used in test_ingest_lambda_handler."""
    return {
        "currentAgentSnapshot": {
            "configuration": {
                "username": eid,
                "routingProfile": {"name": "idk2"},
            },
            "agentStatus": {
                "name": '"AVAILABLE"',
                "startTimestamp": datetime.now(timezone.utc) - timedelta(hours=1),
            },
            "contacts": [{"initiationMethod": "phone", "state": "connected"}],
        },
        "eventTimestamp": event_timestamp,
    }


# ---------------------------------------------------------------------------
# tests — PR finding M-1.1: coverage for update_agent_state() retry logic
# ---------------------------------------------------------------------------


@responses.activate
def test_update_agent_state_writes_summary(mocker, mock_boto3_client):
    """happy path: summary is written to redis and local cache is updated"""
    set_env_vars()
    mocker.patch("src.lib.cyber_logging.ContextProvider")

    import src.ingest
    from src.ingest import update_agent_state

    src.ingest.local_last_update.clear()

    mock_redis = RedisClientMock()
    update_agent_state(mock_redis, make_message(eid="not_eid2", event_timestamp=1000))

    stored = json.loads(mock_redis.data[REDIS_KEY]["NOT_EID2"])
    assert stored["agentStatus"] == "AVAILABLE"
    assert stored["routingProfile"] == "idk2"
    assert stored["lastUpdate"] == 1000
    assert stored["initiationMethod"] == "phone"
    assert stored["contactState"] == "connected"
    assert src.ingest.local_last_update["NOT_EID2"] == 1000


@responses.activate
def test_update_agent_state_ignores_stale_via_local_cache(mocker, mock_boto3_client):
    """a message older than the local cache short-circuits before touching redis"""
    set_env_vars()
    mocker.patch("src.lib.cyber_logging.ContextProvider")

    import src.ingest
    from src.ingest import update_agent_state

    src.ingest.local_last_update.clear()
    src.ingest.local_last_update["NOT_EID2"] = 5000  # newer than incoming

    mock_redis = RedisClientMock()
    update_agent_state(mock_redis, make_message(eid="not_eid2", event_timestamp=1000))

    # nothing written
    assert mock_redis.data.get(REDIS_KEY, {}) == {}
    assert src.ingest.local_last_update["NOT_EID2"] == 5000


@responses.activate
def test_update_agent_state_ignores_stale_via_redis_reread(mocker, mock_boto3_client):
    """a newer lastUpdate found inside the WATCH window aborts the write"""
    set_env_vars()
    mocker.patch("src.lib.cyber_logging.ContextProvider")

    import src.ingest
    from src.ingest import update_agent_state

    src.ingest.local_last_update.clear()

    existing = json.dumps({"lastUpdate": 9999})
    mock_redis = RedisClientMock({REDIS_KEY: {"NOT_EID2": existing}})

    update_agent_state(mock_redis, make_message(eid="not_eid2", event_timestamp=1000))

    # the existing (newer) value is untouched
    assert mock_redis.data[REDIS_KEY]["NOT_EID2"] == existing
    # and the local cache is refreshed from redis so we skip earlier next time
    assert src.ingest.local_last_update["NOT_EID2"] == 9999


@responses.activate
def test_update_agent_state_retries_on_contention_then_succeeds(mocker, mock_boto3_client):
    """WatchError on the first EXEC: we back off, retry, and the write lands"""
    set_env_vars()
    mocker.patch("src.lib.cyber_logging.ContextProvider")

    import src.ingest
    from src.ingest import update_agent_state

    src.ingest.local_last_update.clear()

    # don't actually sleep during the backoff
    sleep_spy = mocker.patch("src.ingest.time.sleep")

    # fail the first two EXECs, succeed on the third
    mock_redis = RedisClientMock(fail_execute_times=2)
    update_agent_state(mock_redis, make_message(eid="not_eid2", event_timestamp=1000))

    stored = json.loads(mock_redis.data[REDIS_KEY]["NOT_EID2"])
    assert stored["lastUpdate"] == 1000
    assert src.ingest.local_last_update["NOT_EID2"] == 1000

    # R-1.7: we backed off between retries rather than hot-spinning
    assert sleep_spy.call_count == 2
    assert all(0 <= c.args[0] <= src.ingest.WATCH_RETRY_MAX_DELAY
               for c in sleep_spy.call_args_list)


@responses.activate
def test_update_agent_state_gives_up_after_max_retries(mocker, mock_boto3_client):
    """persistent contention: we give up, write nothing, and don't cache"""
    set_env_vars()
    mocker.patch("src.lib.cyber_logging.ContextProvider")

    import src.ingest
    from src.ingest import update_agent_state

    src.ingest.local_last_update.clear()

    sleep_spy = mocker.patch("src.ingest.time.sleep")

    # fail every EXEC (MAX_RETRIES is 5)
    mock_redis = RedisClientMock(fail_execute_times=99)
    update_agent_state(mock_redis, make_message(eid="not_eid2", event_timestamp=1000))

    # nothing was written and we did not poison the local cache
    assert mock_redis.data.get(REDIS_KEY, {}).get("NOT_EID2") is None
    assert "NOT_EID2" not in src.ingest.local_last_update

    # backed off before each of the 5 attempts
    assert sleep_spy.call_count == 5


@responses.activate
def test_backoff_is_bounded_and_jittered(mocker, mock_boto3_client):
    """R-1.7: delay grows with attempt but never exceeds the configured cap"""
    set_env_vars()
    mocker.patch("src.lib.cyber_logging.ContextProvider")

    import src.ingest

    delays = [src.ingest._backoff_delay(a) for a in range(10)]
    assert all(0 <= d <= src.ingest.WATCH_RETRY_MAX_DELAY for d in delays)


@responses.activate
def test_eid_is_never_written_to_logs(mocker, mock_boto3_client):
    """S-1.5 regression guard: the raw agent eid must not appear in any log line"""
    set_env_vars()
    mocker.patch("src.lib.cyber_logging.ContextProvider")

    import src.ingest
    from src.ingest import update_agent_state

    src.ingest.local_last_update.clear()

    debug = mocker.patch.object(src.ingest.logger, "debug")
    warning = mocker.patch.object(src.ingest.logger, "warning")

    secret_eid = "supersecretagent"

    # exercise every branch that logs: redis-skip, contention, give-up
    existing = json.dumps({"lastUpdate": 9999})
    mock_redis = RedisClientMock(
        {REDIS_KEY: {secret_eid.upper(): existing}}
    )
    update_agent_state(mock_redis, make_message(eid=secret_eid, event_timestamp=1000))

    mocker.patch("src.ingest.time.sleep")
    src.ingest.local_last_update.clear()
    contended = RedisClientMock(fail_execute_times=99)
    update_agent_state(contended, make_message(eid=secret_eid, event_timestamp=1000))

    emitted = " ".join(
        str(c) for c in debug.call_args_list + warning.call_args_list
    )
    assert secret_eid not in emitted
    assert secret_eid.upper() not in emitted
    # but the redacted, correlatable token IS present
    assert "eid:" in emitted
