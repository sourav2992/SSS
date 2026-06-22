# ═════════════════════════════════════════════════════════════════════════════
# Failure path — Snowflake query fails.
# ═════════════════════════════════════════════════════════════════════════════
@responses.activate
def test_snapshot_snowflake_failure(mocker, mock_boto3_client):
    set_env_vars()
    set_vault_responses()
    responses.add(
        responses.POST,
        f"{base_url}/private/728256/voice-queue/retrieveconfig",
        json={"ISACTIVE": True},
    )

    mocker.patch("redis.Redis", return_value=_build_mock_redis())
    mocker.patch(
        "snowflake.connector.connect",
        side_effect=Exception("simulated snowflake failure"),
    )
    mocker.patch("src.lib.cyber_logging.ContextProvider")

    from src.snapshot import lambda_handler

    with patch("src.snapshot.post_to_db"):
        resp = lambda_handler(_build_event(), context=None)

    # Snowflake failure is swallowed by get_agent_data -> 0 agents enriched,
    # but the snapshot still runs to completion.
    assert resp["message"] == "success"

# ═════════════════════════════════════════════════════════════════════════════
# Failure path — Redis connection fails.
# ═════════════════════════════════════════════════════════════════════════════
@responses.activate
def test_snapshot_redis_failure(mocker, mock_boto3_client):
    set_env_vars()
    set_vault_responses()

    mocker.patch(
        "redis.Redis",
        side_effect=Exception("simulated redis failure"),
    )
    mocker.patch("snowflake.connector.connect", return_value=_snowflake_mock())
    mocker.patch("src.lib.cyber_logging.ContextProvider")

    with pytest.raises(Exception, match="simulated redis failure"):
        from src.snapshot import lambda_handler
        lambda_handler(_build_event(), context=None)
