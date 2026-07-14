"""Shared configuration and logging helpers for the agent-status service.

This module is the single source of truth for constants that were previously
duplicated across ``ingest.py`` and ``snapshot.py`` (PR finding M-2.8), and it
provides the EID redaction helper used to keep agent identifiers out of logs
(PR finding S-1.5).
"""
import hashlib
import os
from typing import Iterable, List

# ---------------------------------------------------------------------------
# M-2.8 fix: define AGENT_STATE_TTL_SECONDS ONCE here and import it everywhere.
# Previously defined identically in ingest.py:37 and snapshot.py:39.
# postgres 10 hours TTL
# ---------------------------------------------------------------------------
AGENT_STATE_TTL_SECONDS = int(
    os.environ.get("AGENT_STATE_TTL_SECONDS", str(10 * 60 * 60))
)

# Other Redis constants that both modules share can live here too.
REDIS_AGENT_STATE_KEY = os.environ.get("REDIS_AGENT_STATE_KEY", "")

# How many redacted EID tokens to include in batch/summary log lines before
# collapsing to a "+N more" suffix. Keeps summary logs bounded.
EID_LOG_CAP = int(os.environ.get("EID_LOG_CAP", "50"))


def redact_eid(eid: str) -> str:
    """Return a stable, non-reversible token for an agent EID.

    S-1.5: agent EIDs (uppercased usernames) are PII and must not be written to
    logs in cleartext. We still want log lines to be *correlatable* across
    events for the same agent, so we emit a short, deterministic hash rather
    than dropping the identifier entirely.

    Example: "AGENT007" -> "eid:3f1a9c2b"
    """
    if eid is None:
        return "eid:none"
    digest = hashlib.sha256(str(eid).encode("utf-8")).hexdigest()
    return f"eid:{digest[:8]}"


def redact_eids(eids: Iterable[str], cap: int = EID_LOG_CAP) -> str:
    """Render an iterable of EIDs as a bounded, redacted, log-safe string.

    Produces something like: ``eid:3f1a9c2b, eid:9d0e1f22 (+5 more)``.
    Never emits a raw EID and never emits more than ``cap`` tokens.
    """
    eids = list(eids)
    shown: List[str] = [redact_eid(e) for e in eids[:cap]]
    body = ", ".join(shown)
    extra = len(eids) - cap
    if extra > 0:
        return f"{body} (+{extra} more)"
    return body
