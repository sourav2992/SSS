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
