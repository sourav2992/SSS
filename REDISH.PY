from typing import Optional

import redis


class RedisClientMock(object):
    def __init__(self, data=None, fail_execute_times: int = 0):
        super(RedisClientMock, self).__init__()

        self.data = {} if data is None else data

        self.fail_execute_times = fail_execute_times

    def ping(self) -> bool:
        return True

    def hgetall(self, key: str) -> dict:
        return self.data

    def hget(self, state: str, key: str) -> Optional[str]:
        if state not in self.data:
            self.data[state] = {}

        return self.data[state].get(key)

    def hset(self, state: str, key: str, value: str):
        if state not in self.data:
            self.data[state] = {}

        self.data[state][key] = value

    def hdel(self, state: str, *keys) -> int:
        # supports snapshot.cleanup_stale_agents
        if state not in self.data:
            return 0
        removed = 0
        for k in keys:
            if k in self.data[state]:
                del self.data[state][k]
                removed += 1
        return removed

    def hlen(self, state: str) -> int:
        return len(self.data.get(state, {}))

    def pipeline(self):
        return RedisPipelineMock(self.data, fail_execute_times=self.fail_execute_times)


class RedisPipelineMock(object):
    def __init__(self, data, fail_execute_times: int = 0):
        self.data = data
        self._queued = []
        self._in_multi = False
        # NEW: countdown of execute() calls that should raise WatchError.
        self._fail_execute_times = fail_execute_times
        # NEW: observability for tests — how many times execute() was attempted.
        self.execute_attempts = 0
        self.watch_calls = 0
        self.unwatch_calls = 0

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.reset()
        return False

    def watch(self, *names):
        self.watch_calls += 1
        return self

    def unwatch(self):
        self.unwatch_calls += 1
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
        self.execute_attempts += 1

        if self._fail_execute_times > 0:
            self._fail_execute_times -= 1
            self.reset()
            raise redis.WatchError()

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
