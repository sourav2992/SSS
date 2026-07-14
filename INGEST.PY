# R-1.7 fix: bounded exponential backoff with jitter for the WATCH retry loop.
# These knobs keep the retry loop from hot-spinning under Redis contention.
# ---------------------------------------------------------------------------
WATCH_RETRY_BASE_DELAY = float(os.environ.get("WATCH_RETRY_BASE_DELAY", "0.05"))  # 50ms
WATCH_RETRY_MAX_DELAY = float(os.environ.get("WATCH_RETRY_MAX_DELAY", "1.0"))     # 1s cap
 
 
def _backoff_delay(attempt: int) -> float:
    """Full-jitter exponential backoff.
 
    delay = random_between(0, min(max_delay, base * 2**attempt))
    Jitter avoids thundering-herd re-collisions when multiple writers
    retry the same eid at once.
    """
    ceiling = min(WATCH_RETRY_MAX_DELAY, WATCH_RETRY_BASE_DELAY * (2 ** attempt))
    return random.uniform(0, ceiling)
