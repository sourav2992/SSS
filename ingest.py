import os
import time
from datetime import datetime, timedelta, timezone
from json import dumps, loads
from typing import Dict

import redis
from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext
from confluent_kafka import Consumer
from dateutil.parser import isoparse
from src.lib.aws_connect_region import check_active_region
from src.lib.exchange import create_devex_requestor
from sdpv4_sdk.util.parser_helper import parse_consumer_props

from src.get_secrets import get_all_secrets
from src.lib.utils import is_lambda, wait_until
from src.sdp_conf import props, sdp_deserializer, topic
from src.lib.cyber_logging import CyberLogger

SECRETS = get_all_secrets()
EXCHANGE_CLIENT_ID = SECRETS["EXCHANGE_CLIENT_ID"]
EXCHANGE_CLIENT_SECRET = SECRETS["EXCHANGE_CLIENT_SECRET"]
REDIS_PASSWORD = SECRETS["REDIS_PASSWORD"]

LOG_LEVEL = os.environ.get("LOG_LEVEL", default="INFO")
REDIS_AGENT_STATE_KEY = os.environ["REDIS_AGENT_STATE_KEY"]
MINUTES_TO_RUN = int(os.environ["MINUTES_TO_RUN"])
REDIS_HOST = os.environ["REDIS_HOST"]
EXCHANGE_URL = os.environ["EXCHANGE_URL"]
logger = Logger()
_devex_requestor = None

# cyber logger
cyber_logger: CyberLogger = None
# postgres 10 hours TTL
AGENT_STATE_TTL_SECONDS = int(os.environ.get("AGENT_STATE_TTL_SECONDS", str(10*60*60)))

""" Because reading from redis is an expensive operation, we can cache some `lastUpdate` statuses locally
to check against. Only if the event timestamp is greater than local do we check redis.
"""
local_last_update = {}

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

def update_agent_state(r: redis.Redis, message: Dict) -> str:
    def _datetime_to_iso_str(x: datetime) -> str:
        """Converts a datetime object into an ISO8601 string with a Z suffix for UTC"""
        return x.isoformat().split(".")[0] + "Z"

    """processes sdp message and uploads to redis"""
    # TODO: consider breaking out the generate payload and the redis checking / update step
    # TODO: try/catch this step
    snapshot = message["currentAgentSnapshot"]
    event_timestamp = message["eventTimestamp"] # ms since epoch
    eid = snapshot["configuration"]["username"].upper()

    # check with local cache that we're not processing an outdated message
    if eid in local_last_update and local_last_update[eid] > event_timestamp:
        # ignore this message because agent state was updated after this current event
        logger.debug(f"ignoring update for {eid} [local]")
        return

    summary = {
        "routingProfile": snapshot["configuration"]["routingProfile"]["name"],
        "agentStatus": snapshot["agentStatus"]["name"].replace('"', ""), # strip the " from the status
        # TODO: check if the .replace is needed anymore
        "startTimestamp": _datetime_to_iso_str(
            snapshot["agentStatus"]["startTimeStamp"]
        ),
        "lastUpdate": event_timestamp,
    }

    contacts_list = snapshot.get("contacts")
    if contacts_list and len(contacts_list) > 0:
        contacts = contacts_list[0]
        if contacts is not None:
            summary["initiationMethod"] = contacts.get("initiationMethod")
            summary["contactState"] = contacts.get("state")

    # ── atomic read-modify-write: re-read lastUpdate inside a WATCH window so a
    #    concurrent same-eid writer can't be clobbered by a stale message ──
    MAX_RETRIES = 5
    with r.pipeline() as pipe:
        for attempt in range(MAX_RETRIES):
            try:
                pipe.watch(REDIS_AGENT_STATE_KEY)
                existing = pipe.hget(REDIS_AGENT_STATE_KEY, eid)
                if existing:
                    try:
                        redis_last_update = loads(existing).get("lastUpdate", 0)
                        if redis_last_update > event_timestamp:
                            local_last_update[eid] = redis_last_update
                            logger.debug(f"ignoring update for {eid} [redis]")
                            pipe.unwatch()
                            return
                    except Exception:
                        pass
                pipe.multi()
                pipe.hset(REDIS_AGENT_STATE_KEY, key=eid, value=dumps(summary))
                pipe.execute()
                break
            except redis.WatchError:
                logger.debug(f"contention on {eid}, retry {attempt + 1}/{MAX_RETRIES}")
                continue
        else:
            logger.warning(f"gave up updating {eid} after {MAX_RETRIES} retries [contention]")
            return
    local_last_update[eid] = event_timestamp

def lambda_handler(event: Dict, context: LambdaContext) -> Dict:
    global _devex_requestor
    logger.info("Lambda invoked!")
    # initialize the cyber logger
    init_cyber_logger(context)
    """
    event looks something like this
    {
        'version': '0',
        'id': '9eace6a4-a0d9-940c-3aaf-c810fa1c773b',
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

    #TODO: Need to get devex_requestor here
    _is_cur_aws_region_active = check_active_region(
        _devex_requestor,
        cyber_logger
    )

    if _is_cur_aws_region_active == False:
        logger.info("My lambda is not in the currently active connect region. I am not running.")
        return {"message": "stopped_run_not_active_region", "trigger_time": str(trigger_time)}

    logger.info("Running lambda because we are in the active connect region.")

    # set start time as the upcoming minute
    start_time = trigger_time.replace(second=0, microsecond=0) + timedelta(minutes=1)
    end_time = start_time + timedelta(minutes=MINUTES_TO_RUN)

    logger.info("Lambda current time: %s", datetime.now(timezone.utc))
    logger.info("Lambda cron trigger time: %s", trigger_time)
    logger.info("Scheduled start time: %s", start_time)
    logger.info("Scheduled end time: %s", end_time)

    logger.info(f"consumer group.id {props['group.id']}")

    # create SDP consumer
    consumer = Consumer(parse_consumer_props(props))
    consumer.subscribe([topic])

    # create Redis connection
    r = redis.Redis(
        host=REDIS_HOST,
        port=6379,
        password=REDIS_PASSWORD,
        ssl=True,
        ssl_cert_reqs=None,
        decode_responses=True,
        socket_timeout=15,
        socket_connect_timeout=15,
        health_check_interval=30,
        retry_on_timeout=True,
        socket_keepalive=True
    )
    logger.info(f"Redis connection created {r.ping()}")

    # wait until start time
    wait_until(lambda t: t >= start_time)

    # consume messages from SDP until end time
    logger.info("Processing messages until %s", end_time)
    number_of_messages_processed = 0
    start_time_native = time.time()
    while datetime.now(timezone.utc) < end_time:
        try:
            message = consumer.poll(timeout=10)
            if message is None:
                continue
            if message:
                if not message.error():
                    _, payload = sdp_deserializer.decode(message.value())
                    logger.debug(
                        f"Message received: {message.key()}={payload}"
                    ) # message.key() is usually None
                    update_agent_state(r, payload)
                    number_of_messages_processed += 1

                    if number_of_messages_processed % 200 == 0:
                        elapsed_time = time.time() - start_time_native
                        average_elapsed_time = (
                            elapsed_time / number_of_messages_processed
                        )
                        logger.info(
                            f"Processed {number_of_messages_processed:,d} messages in {elapsed_time:,.0f}s (avg. {average_elapsed_time:0.3f}s)"
                        )
                    last_event_timestamp = payload["eventTimestamp"]
                    logger.info(f"Last timestamp {last_event_timestamp}")

                elif message.error().code():
                    logger.error(f"Error received: {message.error()}")
                    break

        except Exception as e:
            logger.error(e, exc_info=True)
            cyber_logger.error_event(
                event_action="process_SDP_messages",
                event_reason="Error processing messages",
                event_detail={"error": str(e)},
            )

    # close SDP consumer and exit
    consumer.close()
    logger.info(f"Processed {number_of_messages_processed} messages")
    cyber_logger.success_event(
        event_action="process_SDP_messages",
        event_reason="processed all messages",
        event_detail={
            "messages_processed": number_of_messages_processed,
            "start_time": str(start_time),
            "end_time": str(end_time),
        },
    )

    return {
        "message": "success",
        "events": number_of_messages_processed,
        "start_time": str(start_time),
        "end_time": str(end_time),
    }
