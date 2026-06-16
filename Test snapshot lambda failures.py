# flake8: noqa
import os
from datetime import datetime, timedelta, timezone
from json import dumps
from unittest.mock import patch

import redis
import snowflake
import json
import pytest

import responses

from tests.mocks.redis_client import RedisClientMock
from tests.mocks.snowflake_context import SnowflakeContextMock

base_url = "https://api-it.cloud.capitalone.com"


def set_env_vars():
    # setup environment vars
    os.environ["ENVIRONMENT_GROUP"] = "test"
    os.environ["ENVIRONMENT_REGION"] = "local"

    os.environ["VAULT_LOCKBOX"] = "mytestlockbox"

    os.environ["AWS_REGION"] = "us-east-1"

    os.environ["LOG_LEVEL"] = "INFO"
    os.environ["REDIS_AGENT_STATE_KEY"] = "fake_key"
    os.environ["MINUTES_TO_RUN"] = "1"
    os.environ["REDIS_HOST"] = "fake_host"

    os.environ["EXCHANGE_URL"] = base_url
    os.environ["EXCHANGE_CLIENT_ID"] = "abc123"
    os.environ["DB_NAME"] = "FAKE_INFLUX_URL"
    os.environ["DB_PORT"] = "5432"
    os.environ[
        "VAULT_SNOWFLAKE_USERNAME_PATH"
    ] = "managed/04de47d0-405a-4205-bdc4-2cd52875c38d/ent_voice_user_key"
    os.environ[
        "VAULT_SNOWFLAKE_PASSWORD_PATH"
    ] = "managed/04de47d0-405a-4205-bdc4-2cd52875c38d/ent_voice_pass_key"
    os.environ["SNOWFLAKE_USERNAME"] = "imc_snow"
    os.environ["SNOWFLAKE_ACCOUNT"] = "cptlone-sfprodeast"

    os.environ["BOOTSTRAP_SERVERS"] = "some servers"
    os.environ["SDP_ENV"] = "qa"
    os.environ["BA"] = "ba"
    os.environ["BAP"] = "bap"
    os.environ["POWERTOOLS_SERVICE_NAME"] = "ent-voice-monitoring-agent-service-test"
    os.environ["PUBLISH_DATASET"] = "testdataset"

    os.environ["SNOWFLAKE_SECRET_PATH"] = "qa-snowflake"
    os.environ["POSTGRES_SECRET_PATH"] = "dev-postgres-secrets"
    os.environ["EXCHANGE_SECRET_PATH"] = "exchange-preprod"
    os.environ["REDIS_SECRET_PATH"] = "redis.password"
    os.environ["AGENT_STATE_TTL_SECONDS"] = "36000"


def set_vault_responses():
    for (key, path) in [
        ("redis.username", "redis.username"),
        ("redis.password", "redis.password"),
        ("password", "snowflake/password"),
        ("secret", "exchange/secret"),
        ("enterpriseVoiceDevx.clientId", "enterpriseVoiceDevx.clientId"),
        ("enterpriseVoiceDevx.clientSecret", "enterpriseVoiceDevx.clientSecret"),
        ("enterpriseVoice.rdsPassword", "enterpriseVoice.rdsPassword"),
        ("enterpriseVoice.rdsUserName", "enterpriseVoice.rdsUserName"),
        ("enterpriseVoice.rdsHost", "enterpriseVoice.rdsHost"),
        (
            "ent_voice_user_key",
            "managed/04de47d0-405a-4205-bdc4-2cd52875c38d/ent_voice_user_key",
        ),
        (
            "ent_voice_pass_key",
            "managed/04de47d0-405a-4205-bdc4-2cd52875c38d/ent_voice_pass_key",
        ),
    ]:
        responses.add(
            responses.GET,
            f"http://127.0.0.1:8200/v1/mytestlockbox/{path}",
            json={"data": {key: "fake"}},
            status=200,
        )

    # mock exchange token
    responses.add(
        responses.POST,
        f"{base_url}/oauth2/token",
        json={
            "access_token": "FAKE_ACCESS_TOKEN",
            "issued_at": datetime.now().second,
            "expires_in": 100000,
        },
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
                "host": "test_host",
                "port": "5432",
                "dbname": "test_db",
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
                "clientId": "test_client_id",
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


def _build_mock_redis():
    """Redis hash mock with two stale agents (lastUpdate=0 => older than any
    stale cutoff => flagged LAG_STATUS=True)."""
    return RedisClientMock(
        {
            "NOT_EID1": dumps(
                {
                    "snapshot_data": "fake1",
                    "routingProfile": "idk1",
                    "agentStatus": "AVAILABLE",
                    "initiationMethod": "test",
                    "startTimestamp": "2022-05-02T05:43:22Z",
                    "lastUpdate": 0,
                }
            ),
            "NOT_EID3": dumps(
                {
                    "snapshot_data": "fake3",
                    "routingProfile": "idk3",
                    "agentStatus": "AVAILABLE",
                    "startTimestamp": "2022-05-02T05:43:22Z",
                    "lastUpdate": 0,
                }
            ),
        }
    )


def _snowflake_mock():
    return SnowflakeContextMock(
        [
            ("NOT_EID1", "MySupplier1", "MySite1", "l1", "l2", "l3", "l4"),
            ("NOT_EID2", "MySupplier2", "MySite2", "l1", "l2", "l3", "l4"),
            ("NOT_EID3", "MySupplier3", "MySite3", "l1", "l2", "l3", "l4"),
            ("NOT_EID4", "MySupplier4", "MySite4", "l1", "l2", "l3", "l4"),
        ]
    )


def _build_event():
    one_minute_ago = datetime.now(timezone.utc) - timedelta(seconds=60)
    return {
        "version": "0",
        "time": one_minute_ago.isoformat().split(".")[0] + "Z",
    }


def _fresh_last_update_ms():
    """A lastUpdate recent enough to be NON-stale (active branch)."""
    return datetime.now(timezone.utc).timestamp() * 1000


def _build_redis_with_active_and_stale():
    """One fresh agent (active) + one old agent (stale) so BOTH branches run."""
    return RedisClientMock(
        {
            "ACTIVE_EID": dumps(
                {
                    "routingProfile": "idk_active",
                    "agentStatus": "AVAILABLE",
                    "initiationMethod": "test",
                    "contactState": "CONNECTED",
                    "startTimestamp": "2022-05-02T05:43:22Z",
                    "lastUpdate": _fresh_last_update_ms(),   # recent -> NOT stale
                }
            ),
            "STALE_EID": dumps(
                {
                    "routingProfile": "idk_stale",
                    "agentStatus": "AVAILABLE",
                    "startTimestamp": "2022-05-02T05:43:22Z",
                    "lastUpdate": 0,                          # epoch -> stale
                }
            ),
        }
    )


@responses.activate
def test_snapshot(mocker, mock_boto3_client):
    """Happy path: handler runs end-to-end and returns success."""
    set_env_vars()
    set_vault_responses()
    responses.add(
        responses.POST,
        f"{base_url}/private/728256/voice-queue/retrieveconfig",
        json={"ISACTIVE": True},
    )

    mocker.patch("redis.Redis", return_value=_build_mock_redis())
    mocker.patch("snowflake.connector.connect", return_value=_snowflake_mock())
    mocker.patch("src.lib.cyber_logging.ContextProvider")

    from src.snapshot import lambda_handler

    with patch("src.snapshot.post_to_db"):
        resp = lambda_handler(_build_event(), context=None)

    assert resp["message"] == "success"


@responses.activate
def test_snapshot_failures(mocker, mock_boto3_client):
    """Failure path: when the Postgres write fails, send_to_postgres re-raises
    and lambda_handler has no try/except around process_agent_state, so the
    exception must propagate out of the handler."""
    set_env_vars()
    set_vault_responses()
    responses.add(
        responses.POST,
        f"{base_url}/private/728256/voice-queue/retrieveconfig",
        json={"ISACTIVE": True},
    )

    mocker.patch("redis.Redis", return_value=_build_mock_redis())
    mocker.patch("snowflake.connector.connect", return_value=_snowflake_mock())
    mocker.patch("src.lib.cyber_logging.ContextProvider")

    from src.snapshot import lambda_handler

    with patch(
        "src.snapshot.post_to_db",
        side_effect=Exception("simulated postgres failure"),
    ):
        with pytest.raises(Exception, match="simulated postgres failure"):
            lambda_handler(_build_event(), context=None)


@responses.activate
def test_snapshot_inactive_region(mocker, mock_boto3_client):
    set_env_vars()
    set_vault_responses()
    os.environ["AWS_REGION"] = "us-west-2"
    responses.add(
        responses.POST,
        f"{base_url}/private/728256/voice-queue/retrieveconfig",
        json={"ISACTIVE": False},
    )

    mocker.patch("redis.Redis", return_value=_build_mock_redis())
    mocker.patch("snowflake.connector.connect", return_value=_snowflake_mock())
    mocker.patch("src.lib.cyber_logging.ContextProvider")
    mocker.patch("src.snapshot.check_active_region", return_value=False)

    from src.snapshot import lambda_handler

    with patch("src.snapshot.post_to_db"):
        resp = lambda_handler(_build_event(), context=None)

    assert resp["message"] == "stopped_run_not_active_region"


@responses.activate
def test_snapshot_active_and_stale_branches(mocker, mock_boto3_client):
    set_env_vars()
    set_vault_responses()
    responses.add(
        responses.POST,
        f"{base_url}/private/728256/voice-queue/retrieveconfig",
        json={"ISACTIVE": True},
    )

    mocker.patch("redis.Redis", return_value=_build_redis_with_active_and_stale())
    mocker.patch("snowflake.connector.connect", return_value=_snowflake_mock())
    mocker.patch("src.lib.cyber_logging.ContextProvider")

    from src.snapshot import lambda_handler

    captured = {}

    def _capture(records, *args, **kwargs):
        captured["records"] = records

    # Real process_agent_state runs; only the DB write is intercepted so we can
    # assert lag_status was set correctly for each branch.
    with patch("src.snapshot.post_to_db", side_effect=_capture):
        resp = lambda_handler(_build_event(), context=None)

    assert resp["message"] == "success"

    records = captured["records"]
    lag_by_eid = {r["ent_user_id"]: r["lag_status"] for r in records}
    assert lag_by_eid["ACTIVE_EID"] is False   # active branch covered
    assert lag_by_eid["STALE_EID"] is True     # stale branch covered


@responses.activate
def test_snapshot_eid_log_cap_overflow(mocker, mock_boto3_client):
    set_env_vars()
    set_vault_responses()
    responses.add(
        responses.POST,
        f"{base_url}/private/728256/voice-queue/retrieveconfig",
        json={"ISACTIVE": True},
    )

    # 60 stale agents (> EID_LOG_CAP of 50) -> exercises the suffix branch
    stale_hash = {
        f"STALE_{i}": dumps(
            {
                "routingProfile": "idk",
                "agentStatus": "AVAILABLE",
                "startTimestamp": "2022-05-02T05:43:22Z",
                "lastUpdate": 0,
            }
        )
        for i in range(60)
    }

    mocker.patch("redis.Redis", return_value=RedisClientMock(stale_hash))
    mocker.patch("snowflake.connector.connect", return_value=_snowflake_mock())
    mocker.patch("src.lib.cyber_logging.ContextProvider")

    from src.snapshot import lambda_handler

    with patch("src.snapshot.post_to_db"):
        resp = lambda_handler(_build_event(), context=None)

    assert resp["message"] == "success"
