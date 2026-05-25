import json
import os
from datetime import datetime, timedelta, timezone
from json import loads
from typing import Dict
from zoneinfo import ZoneInfo   # built-in Python 3.9+ — no extra install needed

import redis
from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext
from dateutil.parser import isoparse

from src.lib.aws_connect_region import check_active_region
from src.lib.exchange import create_devex_requestor

from src.get_secrets import get_all_secrets
from src.lib.pgdb_client import post_to_db
from src.lib.snowflake import get_agent_data, get_snowflake_context
from src.lib.utils import is_lambda, wait_until
from src.lib.cyber_logging import CyberLogger

SECRETS = get_all_secrets()
EXCHANGE_CLIENT_ID = SECRETS["EXCHANGE_CLIENT_ID"]
EXCHANGE_CLIENT_SECRET = SECRETS["EXCHANGE_CLIENT_SECRET"]
REDIS_PASSWORD = SECRETS["REDIS_PASSWORD"]
SNOWFLAKE_USERNAME = SECRETS["SNOWFLAKE_USERNAME"]
SNOWFLAKE_PASSWORD = SECRETS["SNOWFLAKE_PASSWORD"]

LOG_LEVEL = os.environ.get("LOG_LEVEL", default="INFO")
REDIS_HOST = os.environ["REDIS_HOST"]
REDIS_AGENT_STATE_KEY = os.environ["REDIS_AGENT_STATE_KEY"]
SNOWFLAKE_ACCOUNT = os.environ["SNOWFLAKE_ACCOUNT"]
EXCHANGE_URL = os.environ["EXCHANGE_URL"]
logger = Logger()
_devex_requestor = None

# cyber logger
cyber_logger: CyberLogger = None

# ─────────────────────────────────────────────────────────────────────────────
# CHANGE 1 — Line 47
# Threshold changed from 24 hours → 10 hours
# Requested by Crystal Bennetch + Edward McCorkle on 12/May/26.
# After 10 hours continuously in any logged-in state, agent gets LAG_STATUS=True.
# Override via AGENT_STALE_THRESHOLD_SECONDS env var without a code deploy.
# ─────────────────────────────────────────────────────────────────────────────
AGENT_STALE_THRESHOLD_SECONDS = int(
    os.environ.get("AGENT_STALE_THRESHOLD_SECONDS", str(10 * 60 * 60))  # was: 24 * 60 * 60
)

_agent_data = None


def init_cyber_logger(context: LambdaContext):
    """
    This function initializes the cyber logger instance
    :param context: lambda context
    """
    global cyber_logger
    if cyber_logger is None:
        cyber_logger = CyberLogger(
            ba=os.environ["BA"],
            service_name=os.environ["BAP"],
            event_name=os.environ["POWERTOOLS_SERVICE_NAME"],
        )
    cyber_logger.initialize(context, os.environ["PUBLISH_DATASET"])


# ─────────────────────────────────────────────────────────────────────────────
# CHANGE 8 — NEW FUNCTION (inserted after init_cyber_logger)
#
# cleanup_stale_agents()
#
# WHY THIS EXISTS:
#   Redis version is 6.2.6 (confirmed by TL).
#   Per-field TTL on a hash requires Redis 7.4+ (HEXPIRE command).
#   AWS only supports up to Redis 7.1 then moved to ValKey — upgrade
#   not viable because the Redis instance is shared by other teams.
#
# TL'S SUGGESTED SOLUTION (21/May/26):
#   "A daily cleanup — if time = Midnight Eastern, grab the entire hash,
#    remove keys that haven't been updated in X time, re-push the hash,
#    log everyone that gets removed, continue regular processing."
#
# HOW IT WORKS:
#   1. Read entire Redis hash (hgetall)
#   2. For each agent field, check lastUpdate timestamp
#   3. If older than AGENT_STALE_THRESHOLD_SECONDS → mark for deletion
#   4. Delete all stale fields in one hdel call
#   5. Log every removed EID for audit trail
#   6. Return count of removed agents
#
# CALLED FROM: lambda_handler() at midnight Eastern time only
# ─────────────────────────────────────────────────────────────────────────────
def cleanup_stale_agents(r: redis.Redis) -> int:
    """
    Reads the entire Redis hash, removes agent fields that have not
    been updated within AGENT_STALE_THRESHOLD_SECONDS, and logs every
    removal for audit purposes.

    Called once per day at midnight Eastern (TL).

    :param r: active Redis connection
    :return: number of stale agent fields removed
    """
    logger.info("CLEANUP: Starting daily stale agent cleanup")

    now_ms    = datetime.now(timezone.utc).timestamp() * 1000
    cutoff_ms = now_ms - (AGENT_STALE_THRESHOLD_SECONDS * 1000)

    all_fields  = r.hgetall(REDIS_AGENT_STATE_KEY)
    stale_eids  = []

    logger.info(f"CLEANUP: Total agents in Redis hash: {len(all_fields)}")

    for eid, raw in all_fields.items():
        try:
            data        = loads(raw)
            last_update = data.get("lastUpdate", 0)
            age_hours   = (now_ms - last_update) / 1000 / 3600

            if last_update < cutoff_ms:
                # agent has not sent an event within the threshold window
                stale_eids.append(eid)
                logger.info(
                    f"CLEANUP: Marking agent {eid} for removal "
                    f"(age={age_hours:.1f}h, "
                    f"lastUpdate={last_update}, "
                    f"cutoff={cutoff_ms})"
                )
            else:
                logger.debug(
                    f"CLEANUP: Agent {eid} is active "
                    f"(age={age_hours:.1f}h) — keeping"
                )

        except Exception as e:
            # unparseable record — treat as stale to keep Redis clean
            logger.error(
                f"CLEANUP: Could not parse agent {eid} — "
                f"treating as stale. Error: {e}"
            )
            stale_eids.append(eid)

    if stale_eids:
        # remove all stale fields in a single hdel command
        removed = r.hdel(REDIS_AGENT_STATE_KEY, *stale_eids)
        logger.info(
            f"CLEANUP: Removed {removed} stale agent(s) from Redis. "
            f"EIDs removed: {stale_eids}"
        )
    else:
        logger.info("CLEANUP: No stale agents found — Redis hash is clean.")
        removed = 0

    remaining = r.hlen(REDIS_AGENT_STATE_KEY)
    logger.info(f"CLEANUP: Agents remaining in Redis after cleanup: {remaining}")

    return removed


def send_to_postgres(enriched_data, ts, cyber_logger):
    def _sanitize(s) -> str:
        """replace spaces with underscores to fit postgres format"""
        return str(s).replace(" ", "_")

    def _build_data_point(record, timestamp) -> dict:
        p = {
            "measurements": {
                _sanitize(record["agentStatus"]): float(1)
            },
            "level_1": _sanitize(record["levels"]["level1"]),
            "level_2": _sanitize(record["levels"]["level2"]),
            "level_3": _sanitize(record["levels"]["level3"]),
            "level_4": _sanitize(record["levels"]["level4"]),
            "routing_profile": _sanitize(record["routingProfile"]),
            "ent_user_id": record["eid"],
            "splyr_nm": _sanitize(record["supplier"]),
            "site_nm": _sanitize(record["siteName"]),
            "initiation_method": None,
            # ── CHANGE 2 — Line 152 ──────────────────────────────────────────
            # NEW field: lag_status
            # True  = agent in same state for 10+ hours (stale)
            # False = agent updated within last 10 hours (active)
            # Was: field did not exist
            # ─────────────────────────────────────────────────────────────────
            "lag_status": record.get("LAG_STATUS", False),
            "time": timestamp,
        }
        if "initiationMethod" in record:
            p["initiation_method"] = _sanitize(record["initiationMethod"])
        if "contactState" in record and record["contactState"] in [
            "CONNECTED",
            "CONNECTING",
            "CONNECTED_ONHOLD",
            "ENDED",
        ]:
            p["measurements"][record["contactState"]] = float(1)

        p["measurements"] = json.dumps(p["measurements"])
        return p

    # convert to postgres format
    records = [_build_data_point(d, ts) for d in enriched_data]

    # ── CHANGE 3 — INSERT query ───────────────────────────────────────────────
    # Added lag_status to the column list AND the VALUES list
    # Was: neither had lag_status
    # ─────────────────────────────────────────────────────────────────────────
    QUERY = """
        INSERT INTO monitoring.AGENTMETRICS (
            time, measurements, level_1, level_2, level_3, level_4,
            routing_profile, ent_user_id, splyr_nm, site_nm,
            initiation_method, lag_status
        )
        VALUES (
            %(time)s, %(measurements)s, %(level_1)s, %(level_2)s,
            %(level_3)s, %(level_4)s, %(routing_profile)s,
            %(ent_user_id)s, %(splyr_nm)s, %(site_nm)s,
            %(initiation_method)s, %(lag_status)s
        )
    """

    try:
        post_to_db(records, QUERY, cyber_logger)
    except Exception as e:
        logger.error(f"Error writing records to Postgres: {e}", stack_info=True)
        raise e


def process_agent_state(agent_state, ts, cyber_logger):
    """sends a snapshot of the current agent state to postgres"""
    now_ms          = datetime.now(timezone.utc).timestamp() * 1000
    stale_cutoff_ms = now_ms - (AGENT_STALE_THRESHOLD_SECONDS * 1000)

    stale_agents  = []
    active_agents = []

    # ── CHANGE 4 ─────────────────────────────────────────────────────────────
    # BEFORE: stale agents were skipped and never written to Postgres
    # AFTER:  stale agents get LAG_STATUS=True and ARE written to Postgres
    #         active agents get LAG_STATUS=False explicitly
    # Both lists store (eid, snapshot) TUPLES — not plain strings
    # ─────────────────────────────────────────────────────────────────────────
    for eid, snapshot in agent_state.items():
        last_update = snapshot.get("lastUpdate", 0)
        age_hours   = (now_ms - last_update) / 1000 / 3600
        if last_update < stale_cutoff_ms:
            snapshot["LAG_STATUS"] = True
            stale_agents.append((eid, snapshot))
            logger.info(
                f"Flagging agent {eid} as LAG_STATUS: "
                f"lastUpdate={last_update}, "
                f"threshold={stale_cutoff_ms} "
                f"(age={age_hours:.1f}h)"
            )
        else:
            snapshot["LAG_STATUS"] = False
            active_agents.append((eid, snapshot))

    if stale_agents:
        stale_eids = [eid for eid, _ in stale_agents]
        logger.info(
            f"LAG_STATUS flagged {len(stale_agents)} agent(s) "
            f"(threshold={AGENT_STALE_THRESHOLD_SECONDS}s). "
            f"Stale EIDs: {stale_eids}"
        )

    # ── CHANGE 5 ─────────────────────────────────────────────────────────────
    # BEFORE: enriched_data used only active_agents
    # AFTER:  all_agents = active_agents + stale_agents
    #         Both groups go to Postgres (stale ones with lag_status=True)
    # ─────────────────────────────────────────────────────────────────────────
    all_agents    = active_agents + stale_agents
    enriched_data = [
        {
            "_id": eid,
            "eid": eid,
            **snapshot,
            **_agent_data.get(
                eid,
                {
                    "supplier": "",
                    "siteName": "",
                    "levels": {
                        "level1": "",
                        "level2": "",
                        "level3": "",
                        "level4": "",
                    },
                },
            ),
        }
        for (eid, snapshot) in all_agents   # was: active_agents
    ]

    # send to postgres
    send_to_postgres(enriched_data, ts, cyber_logger)


def lambda_handler(event: Dict, context: Dict) -> Dict:
    global _devex_requestor
    logger.info("Lambda invoked!")
    init_cyber_logger(context)
    """
    event looks something like this
    {
        'version': '0',
        'id': '9gace6a4-a0d9-940c-3aaf-c810fa1c773b',
        'detail-type': 'Scheduled Event',
        'source': 'aws.events',
        'account': ...,
        'time': '2022-05-02T05:33:22Z',
        'region': 'us-east-1',
        'resources': ['arn...'],
        'detail': {}
    }
    """

    trigger_time = isoparse(event["time"])

    # load exchange access token
    if _devex_requestor is None:
        _devex_requestor = create_devex_requestor(
            client_id=EXCHANGE_CLIENT_ID,
            client_secret=EXCHANGE_CLIENT_SECRET,
            base_url=EXCHANGE_URL,
        )

    _is_cur_aws_region_active = check_active_region(
        _devex_requestor,
        cyber_logger
    )

    if _is_cur_aws_region_active == False:
        logger.info("My lambda is not in the currently active connect region. I am not running.")
        return {"message": "stopped_run_not_active_region", "trigger_time": str(trigger_time)}
    else:
        logger.info("Running lambda because we are in the active connect region.")

    # set start time as 2 minutes from now
    snapshot_time = datetime.now(timezone.utc).replace(second=0, microsecond=0) + timedelta(minutes=2)
    current_time  = datetime.now(timezone.utc)

    logger.info("Lambda current time: %s", current_time)
    logger.info("Lambda cron trigger time: %s", trigger_time)
    logger.info("Scheduled snapshot time: %s", snapshot_time)

    # do not send snapshot if we've already passed the expected time
    if current_time > snapshot_time:
        logger.error("Current time > snapshot time. Exiting.")
        return {
            "message": "failure",
            "snapshot_time": str(snapshot_time),
        }

    # wait until start time
    wait_until(lambda time: time >= snapshot_time)
    logger.info("Starting snapshot at %s", datetime.now(timezone.utc))

    # ── CHANGE 6 — Redis connection (NEW BLOCK) ───────────────────────────────
    # FIX: Redis connection was MISSING — r was never defined so
    # r.hgetall() crashed with NameError. Added full connection block.
    # Was: this entire block did not exist
    # ─────────────────────────────────────────────────────────────────────────
    r = redis.Redis(
        host=REDIS_HOST,
        port=6379,
        password=REDIS_PASSWORD,
        ssl=True,
        ssl_cert_reqs=None,
        decode_responses=True,
        socket_timeout=15,
        socket_connect_timeout=15,
        health_check_interval=30,  # background PING every 30s
        retry_on_timeout=True,     # reconnects on network blips
        socket_keepalive=True,     # prevents NAT gateway dropping idle connections
    )
    logger.info(f"Redis connection created {r.ping()}")

    # ── CHANGE 8 — Midnight Eastern cleanup (NEW BLOCK) ──────────────────────
    # TL confirmed:
    #   Redis is version 6.2.6 — per-field TTL (HEXPIRE) requires 7.4+.
    #   Upgrade not viable — instance shared by other teams in same AWS account.
    #   Solution: daily cleanup at midnight Eastern instead.
    #
    # If the Lambda fires at midnight Eastern time, run cleanup_stale_agents()
    # BEFORE the regular snapshot so stale agents are removed from Redis first,
    # then the snapshot runs on the clean hash.
    #
    # Midnight Eastern = 05:00 UTC (EST) or 04:00 UTC (EDT)
    # We check hour == 0 in Eastern time to catch both.
    # ─────────────────────────────────────────────────────────────────────────
    eastern     = ZoneInfo("America/New_York")
    now_eastern = datetime.now(eastern)

    if now_eastern.hour == 0:
        logger.info(
            f"Midnight Eastern detected ({now_eastern.strftime('%Y-%m-%d %H:%M %Z')}) "
            f"— running daily stale agent cleanup before snapshot"
        )
        removed_count = cleanup_stale_agents(r)
        logger.info(f"Daily cleanup complete — {removed_count} agent(s) removed from Redis")
    else:
        logger.info(
            f"Not midnight Eastern ({now_eastern.strftime('%H:%M %Z')}) "
            f"— skipping daily cleanup"
        )

    # get agent state and unmarshall
    agent_state = r.hgetall(REDIS_AGENT_STATE_KEY)
    agent_state = {k: loads(v) for k, v in agent_state.items()}
    logger.info(f"Retrieved agent state, count={len(agent_state)}")

    # ── CHANGE 7 — Snowflake agent data load (NEW BLOCK) ─────────────────────
    # FIX: _agent_data was never loaded — process_agent_state() calls
    # _agent_data.get(eid) but it was still None, causing AttributeError.
    # Was: this entire block did not exist
    # ─────────────────────────────────────────────────────────────────────────
    global _agent_data
    if _agent_data is None:
        logger.info("Calling get_snowflake_context")
        snowflake_context = get_snowflake_context(
            username=SNOWFLAKE_USERNAME,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
        )
        logger.info("Calling get_agent_data")
        _agent_data = get_agent_data(snowflake_context)
    logger.info("Loaded %d agents", len(_agent_data))

    # process agent state and exit
    process_agent_state(agent_state, ts=snapshot_time, cyber_logger=cyber_logger)
    logger.info("Finished processing agent state")
    return {
        "message": "success",
        "snapshot_time": str(snapshot_time),
    }
