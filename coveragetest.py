# ═════════════════════════════════════════════════════════════════════════════
# COVERAGE: active + stale branches of process_agent_state in one run.
# Covers the lag_status=False (active) AND lag_status=True (stale) branches.
# ═════════════════════════════════════════════════════════════════════════════
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
