# ─────────────────────────────────────────────────────────────────
    # ATOMIC READ-MODIFY-WRITE
    # The old hget→check→hset sequence spanned two round-trips, so two
    # workers handling the same eid could both read an old lastUpdate, both
    # pass the staleness guard, and the older message could win — regressing
    # lastUpdate. WATCH/MULTI/EXEC closes that: watch the key, re-read the
    # field inside the watched window, and only commit if nothing changed
    # since. If a peer wrote first, EXEC aborts (WatchError) and we retry.
    # ─────────────────────────────────────────────────────────────────
    MAX_RETRIES = 5
    with r.pipeline() as pipe:
        for attempt in range(MAX_RETRIES):
            try:
                pipe.watch(REDIS_AGENT_STATE_KEY)

                # re-read THIS agent's field inside the watched window
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
                        # unparseable existing record — fall through and overwrite
                        pass

                # commit: write our field + refresh the hash TTL atomically
                pipe.multi()
                pipe.hset(REDIS_AGENT_STATE_KEY, key=eid, value=dumps(summary))
                pipe.expire(REDIS_AGENT_STATE_KEY, AGENT_STATE_TTL_SECONDS)
                pipe.execute()
                break  # success

            except redis.WatchError:
                logger.debug(f"contention on {eid}, retry {attempt + 1}/{MAX_RETRIES}")
                continue
        else:
            # exhausted retries — key is hot, fresher data is already landing;
            # drop this older message rather than force a write
            logger.warning(f"gave up updating {eid} after {MAX_RETRIES} retries [contention]")
            return

    local_last_update[eid] = event_timestamp
