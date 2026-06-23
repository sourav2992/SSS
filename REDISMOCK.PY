

    def pipeline(self):
        # Shares self.data so reads/writes through the pipeline are visible
        # to the client mock and vice versa.
        return RedisPipelineMock(self.data)


class RedisPipelineMock(object):
    """Mimics redis-py WATCH/MULTI/EXEC semantics for tests.

    Before multi(): commands run immediately (watched-pipeline mode) and
    return real values. After multi(): commands queue and run on execute().
    Single-threaded tests never trigger WatchError, so the happy path is
    exercised; true concurrency isn't unit-testable without a real broker.
    """

    def __init__(self, data):
        self.data = data          # same dict the client mock uses
        self._queued = []
        self._in_multi = False

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.reset()
        return False

    def watch(self, *names):
        return self

    def unwatch(self):
        return True

    def multi(self):
        self._in_multi = True

    def hget(self, state: str, key: str):
        if self._in_multi:
            self._queued.append(("hget", state, key))
            return self
        if state not in self.data:
            self.data[state] = {}
        return self.data[state].get(key)

    def hset(self, state: str, key: str = None, value: str = None):
        if self._in_multi:
            self._queued.append(("hset", state, key, value))
            return self
        self._apply_hset(state, key, value)
        return 1

    def execute(self):
        results = []
        for op in self._queued:
            if op[0] == "hset":
                _, state, key, value = op
                self._apply_hset(state, key, value)
                results.append(1)
            elif op[0] == "hget":
                _, state, key = op
                if state not in self.data:
                    self.data[state] = {}
                results.append(self.data[state].get(key))
        self.reset()
        return results

    def reset(self):
        self._queued = []
        self._in_multi = False

    def _apply_hset(self, state, key, value):
        if state not in self.data:
            self.data[state] = {}
        if key is not None:
            self.data[state][key] = value
