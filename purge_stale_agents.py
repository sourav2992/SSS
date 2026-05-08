"""
purge_stale_agents.py
─────────────────────
One-time (or scheduled) script to delete stale agent records from the
REDIS_AGENT_STATE_KEY hash.

Run ONCE after deploying the TTL + staleness-filter changes to clear
the existing backlog of old agent records.

Can also be run on a cron (e.g. daily) as an extra safety net.

Usage
-----
    # dry run — prints what WOULD be deleted, touches nothing
    python purge_stale_agents.py --dry-run

    # live run — actually deletes from Redis
    python purge_stale_agents.py

    # custom threshold (e.g. 48 hours)
    python purge_stale_agents.py --threshold-hours 48

Environment variables required (same as ingest.py / snapshot.py)
-----------------------------------------------------------------
    REDIS_HOST
    REDIS_AGENT_STATE_KEY
    REDIS_PASSWORD          (from secrets — set before running)
"""

import argparse
import os
import sys
from datetime import datetime, timezone
from json import loads

import redis

# ── config ────────────────────────────────────────────────────────────────────

REDIS_HOST          = os.environ["REDIS_HOST"]
REDIS_AGENT_STATE_KEY = os.environ["REDIS_AGENT_STATE_KEY"]
REDIS_PASSWORD      = os.environ["REDIS_PASSWORD"]

DEFAULT_THRESHOLD_HOURS = 24  # same default as snapshot.py staleness filter

# ── helpers ───────────────────────────────────────────────────────────────────

def connect() -> redis.Redis:
    r = redis.Redis(
        host=REDIS_HOST,
        port=6379,
        password=REDIS_PASSWORD,
        ssl=True,
        ssl_cert_reqs=None,
        decode_responses=True,
        socket_timeout=15,
        socket_connect_timeout=15,
    )
    r.ping()  # fail fast if creds are wrong
    return r


def find_stale_eids(r: redis.Redis, threshold_hours: float) -> list[dict]:
    """
    Returns a list of dicts describing every agent field in the hash whose
    lastUpdate timestamp is older than `threshold_hours` from now.
    """
    now_ms         = datetime.now(timezone.utc).timestamp() * 1000
    cutoff_ms      = now_ms - (threshold_hours * 3600 * 1000)

    all_fields = r.hgetall(REDIS_AGENT_STATE_KEY)

    stale = []
    for eid, raw in all_fields.items():
        try:
            data = loads(raw)
        except Exception:
            # unparseable record — treat as stale
            stale.append({
                "eid": eid,
                "last_update_ms": 0,
                "age_hours": None,
                "agent_status": "UNKNOWN (parse error)",
            })
            continue

        last_update = data.get("lastUpdate", 0)
        if last_update < cutoff_ms:
            age_hours = (now_ms - last_update) / 1000 / 3600
            stale.append({
                "eid": eid,
                "last_update_ms": last_update,
                "age_hours": round(age_hours, 1),
                "agent_status": data.get("agentStatus", "UNKNOWN"),
            })

    return stale


def print_report(stale: list[dict], threshold_hours: float) -> None:
    print(f"\n{'─'*60}")
    print(f"  Stale agents (no update in >{threshold_hours}h)")
    print(f"{'─'*60}")
    if not stale:
        print("  ✓ No stale records found.")
    else:
        print(f"  {'EID':<30} {'Age (h)':>8}  {'Last status'}")
        print(f"  {'─'*28} {'─'*8}  {'─'*20}")
        for rec in sorted(stale, key=lambda x: x["age_hours"] or 0, reverse=True):
            age = f"{rec['age_hours']:.1f}" if rec["age_hours"] is not None else "N/A"
            print(f"  {rec['eid']:<30} {age:>8}  {rec['agent_status']}")
    print(f"{'─'*60}\n")


def purge(r: redis.Redis, stale: list[dict], dry_run: bool) -> None:
    if not stale:
        print("Nothing to delete.")
        return

    eids_to_delete = [rec["eid"] for rec in stale]

    if dry_run:
        print(f"[DRY RUN] Would delete {len(eids_to_delete)} field(s) from hash '{REDIS_AGENT_STATE_KEY}':")
        for eid in eids_to_delete:
            print(f"  - {eid}")
        print("\nRe-run without --dry-run to apply.")
        return

    # hdel accepts multiple fields in one call
    deleted = r.hdel(REDIS_AGENT_STATE_KEY, *eids_to_delete)
    print(f"✓ Deleted {deleted} stale agent record(s) from Redis.")

    # If the hash is now empty, the key itself is gone automatically.
    remaining = r.hlen(REDIS_AGENT_STATE_KEY)
    print(f"  Remaining agents in hash: {remaining}")


# ── main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Purge stale agent records from Redis.")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be deleted without making any changes.",
    )
    parser.add_argument(
        "--threshold-hours",
        type=float,
        default=DEFAULT_THRESHOLD_HOURS,
        help=f"Age threshold in hours (default: {DEFAULT_THRESHOLD_HOURS}).",
    )
    args = parser.parse_args()

    print(f"\nConnecting to Redis at {REDIS_HOST}...")
    try:
        r = connect()
        print("Connection OK.")
    except Exception as e:
        print(f"ERROR: Could not connect to Redis — {e}")
        sys.exit(1)

    total_agents = r.hlen(REDIS_AGENT_STATE_KEY)
    print(f"Total agents in hash: {total_agents}")

    stale = find_stale_eids(r, args.threshold_hours)
    print_report(stale, args.threshold_hours)

    purge(r, stale, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
