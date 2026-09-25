"""resilient_messages/SyncResilientPubSub: a disconnect is a backoff and a
resubscribe, never a silent end (bar_aggregator.py:484-518 and
live/supervisor.py:512-525 are the two bugs this fixes)."""

from __future__ import annotations

import asyncio

import redis
import redis.asyncio as aioredis

from trading.streaming.resilient_pubsub import SyncResilientPubSub, resilient_messages


class _FakePubSub:
    """One async pubsub whose listen() does something different each time
    it is called -- exactly the seam resilient_messages watches."""

    def __init__(self, behaviors):
        # shared with the client: consumption must advance across reconnects
        self._behaviors = behaviors
        self.subscribed_patterns: list[str] = []
        self.closed = 0

    async def psubscribe(self, *patterns):
        self.subscribed_patterns.extend(patterns)

    async def listen(self):
        behavior = self._behaviors.pop(0)
        if behavior == "raise":
            raise ConnectionError("connection reset")
        for message in behavior:
            yield message
        # "listen() that returns" -- a real disconnect that redis-py
        # itself does not raise on (bar_aggregator.py:484-518's bug).

    async def punsubscribe(self):
        pass

    async def aclose(self):
        self.closed += 1


class _FakeRedis:
    def __init__(self, behaviors):
        self._behaviors = behaviors
        self.pubsub_calls = 0

    def pubsub(self):
        self.pubsub_calls += 1
        return _FakePubSub(self._behaviors)


async def _collect(agen, count):
    out = []
    async for message in agen:
        out.append(message)
        if len(out) >= count:
            return out
    return out


async def _fake_sleep_records(calls):
    async def _sleep(seconds):
        calls.append(seconds)
    return _sleep


def test_a_raised_connection_error_backs_off_and_resubscribes():
    calls: list[float] = []

    async def _sleep(seconds):
        calls.append(seconds)

    fake = _FakeRedis(["raise", [{"type": "pmessage", "data": "after-reconnect"}]])
    agen = resilient_messages(fake, patterns=["ticks:*"], sleep=_sleep)

    out = asyncio.run(_collect(agen, 1))
    assert out == [{"type": "pmessage", "data": "after-reconnect"}]
    assert calls == [1.0]  # first backoff step
    assert fake.pubsub_calls == 2  # one pubsub per (re)connect attempt


def test_listen_returning_is_also_treated_as_a_disconnect():
    """bar_aggregator's actual production bug: pubsub.listen() returning
    normally on a Redis connection loss, silently ending the consumer."""
    calls: list[float] = []

    async def _sleep(seconds):
        calls.append(seconds)

    fake = _FakeRedis([[], [{"type": "pmessage", "data": "after-empty-listen"}]])
    agen = resilient_messages(fake, patterns=["ticks:*"], sleep=_sleep)

    out = asyncio.run(_collect(agen, 1))
    assert out == [{"type": "pmessage", "data": "after-empty-listen"}]
    assert calls == [1.0]


def test_backoff_resets_after_a_message_is_delivered():
    calls: list[float] = []

    async def _sleep(seconds):
        calls.append(seconds)

    fake = _FakeRedis(
        [
            "raise",
            "raise",
            [{"type": "pmessage", "data": "m1"}],
            "raise",
            [{"type": "pmessage", "data": "m2"}],
        ]
    )
    agen = resilient_messages(fake, patterns=["ticks:*"], sleep=_sleep)

    out = asyncio.run(_collect(agen, 2))
    assert [m["data"] for m in out] == ["m1", "m2"]
    # 1s, 2s (doubled -- no message delivered yet), then reset to 1s
    # after m1 before the third failure.
    assert calls == [1.0, 2.0, 1.0]


def test_resilient_messages_uses_a_pre_subscribed_pubsub_without_resubscribing():
    """A caller that already subscribed (to guarantee a message published
    right after startup isn't lost -- paper/engine.py's run_engine does
    this before creating any consumer task, exactly restoring the
    synchronous-subscribe-before-anything-else guarantee the old direct
    `pubsub.listen()` code had) can hand that pubsub straight to
    resilient_messages. It must be consumed as-is, with no extra
    `redis_client.pubsub()`/psubscribe() call before the first listen()
    -- an unconditional resubscribe here would race the caller's own
    subscription against whatever it published right after handing off,
    the exact bug that caused paper/engine.py's tests to hang."""
    calls: list[float] = []

    async def _sleep(seconds):
        calls.append(seconds)

    behaviors = [[{"type": "pmessage", "data": "m1"}]]
    fake = _FakeRedis(behaviors)
    pre_subscribed = _FakePubSub(behaviors)  # the caller already subscribed this one itself

    agen = resilient_messages(fake, patterns=["ticks:*"], sleep=_sleep, pubsub=pre_subscribed)

    out = asyncio.run(_collect(agen, 1))
    assert out == [{"type": "pmessage", "data": "m1"}]
    assert fake.pubsub_calls == 0  # the caller's own pubsub was used, not a fresh one


def test_resilient_messages_against_real_redis(redis_client) -> None:
    from trading.config import get_settings

    async def _run():
        client = aioredis.Redis.from_url(get_settings().redis_url, decode_responses=True)
        agen = resilient_messages(client, patterns=["resilient-test:*"])
        task = asyncio.create_task(_collect(agen, 1))
        await asyncio.sleep(0.2)  # let the psubscribe land
        redis_client.publish("resilient-test:1", "hello")
        out = await asyncio.wait_for(task, timeout=5)
        await client.connection_pool.disconnect()
        return out

    out = asyncio.run(_run())
    assert out[0]["data"] == "hello"


def test_sync_resilient_pubsub_reconnects_on_a_connection_error():
    calls: list[float] = []

    class _FakeSyncPubSub:
        def __init__(self, behaviors):
            # shared with the client: consumption must advance across reconnects
            self._behaviors = behaviors

        def psubscribe(self, *patterns):
            pass

        def get_message(self, timeout=None):
            behavior = self._behaviors.pop(0)
            if behavior == "raise":
                raise redis.exceptions.ConnectionError("reset")
            return behavior

    class _FakeSyncClient:
        def __init__(self, behaviors):
            self._behaviors = behaviors

        def pubsub(self):
            return _FakeSyncPubSub(self._behaviors)

    behaviors = ["raise", {"type": "pmessage", "data": "ok"}]
    sub = SyncResilientPubSub(
        lambda: _FakeSyncClient(behaviors), patterns=["ticks:*"], sleep=calls.append
    )

    assert sub.get_message(timeout=1.0) is None  # the reconnect attempt itself
    assert sub.get_message(timeout=1.0) == {"type": "pmessage", "data": "ok"}
    assert calls == [1.0]


def test_resubscribe_retries_with_backoff_instead_of_raising_when_redis_is_still_down():
    """I4: the old code called `_subscribe` for the reconnect outside any
    try/except, so a psubscribe that fails because Redis is still down
    (not yet the listen() call, the resubscribe itself) escaped
    resilient_messages entirely and killed the consumer. psubscribe fails
    twice here before succeeding; no exception must escape, and each
    failed attempt must still back off and escalate like a listen()
    failure does."""
    calls: list[float] = []

    class _FlakyResubscribePubSub(_FakePubSub):
        def __init__(self, behaviors, fail_counter):
            super().__init__(behaviors)
            self._fail_counter = fail_counter

        async def psubscribe(self, *patterns):
            if self._fail_counter[0] > 0:
                self._fail_counter[0] -= 1
                raise ConnectionError("still down")
            await super().psubscribe(*patterns)

    class _FlakyResubscribeRedis:
        def __init__(self, behaviors, fail_counter):
            self._behaviors = behaviors
            self._fail_counter = fail_counter
            self.pubsub_calls = 0

        def pubsub(self):
            self.pubsub_calls += 1
            return _FlakyResubscribePubSub(self._behaviors, self._fail_counter)

    async def _sleep(seconds):
        calls.append(seconds)

    behaviors = ["raise", [{"type": "pmessage", "data": "after-reconnect"}]]
    fail_counter = [2]  # the reconnect's psubscribe fails twice, then succeeds
    fake = _FlakyResubscribeRedis(behaviors, fail_counter)
    # Hand in an already-subscribed pubsub for the *first* connection (same
    # seam the "pre-subscribed" test above uses) so only the reconnect's
    # resubscribe goes through the flaky fake.pubsub() -- isolating the one
    # thing this test is about.
    pre_subscribed = _FakePubSub(behaviors)
    agen = resilient_messages(fake, patterns=["ticks:*"], sleep=_sleep, pubsub=pre_subscribed)

    out = asyncio.run(_collect(agen, 1))
    assert out == [{"type": "pmessage", "data": "after-reconnect"}]
    # 1.0 for the original disconnect, then 2.0 and 4.0 for the two failed
    # resubscribe attempts -- one continuously escalating backoff, not a
    # reset in between.
    assert calls == [1.0, 2.0, 4.0]


def test_confirm_only_sessions_still_back_off_and_escalate():
    """A subscribe/psubscribe confirmation is not a real message. A session
    that only ever produces a confirmation and then dies must be scored the
    same as a session that produced nothing at all -- it must still sleep
    and escalate the backoff, not be treated as 'delivered'. Reuses the
    module-level _FakePubSub/_FakeRedis fakes unmodified: a confirmation-only
    session is just a `listen()` behavior list containing one confirmation
    dict instead of a pmessage."""
    calls: list[float] = []

    async def _sleep(seconds):
        calls.append(seconds)

    confirm = {"type": "psubscribe", "pattern": None, "channel": "ticks:*", "data": 1}
    fake = _FakeRedis(
        [
            [confirm],  # session 1: confirmation only, then dies
            [confirm],  # session 2: confirmation only, then dies
            [confirm],  # session 3: confirmation only, then dies
            [{"type": "pmessage", "data": "finally"}],
        ]
    )
    agen = resilient_messages(fake, patterns=["ticks:*"], sleep=_sleep)

    out = asyncio.run(_collect(agen, 1))
    assert out == [{"type": "pmessage", "data": "finally"}]
    assert calls == [1.0, 2.0, 4.0]  # each confirm-only session still backs off and escalates


def test_sync_resilient_pubsub_confirmation_does_not_reset_an_escalated_backoff():
    """The sync twin of the confirm-only bug: a subscribe/psubscribe
    confirmation from get_message() must return None like a "nothing right
    now" result, but it must NOT reset an already-escalated backoff back
    down to the initial step -- that would turn a flaky connection into a
    tight reconnect loop. A new fake class, not the existing test's local
    ones, since those are scoped inside that other test function."""
    calls: list[float] = []

    class _FakeConfirmSyncPubSub:
        def __init__(self, behaviors):
            # shared with the client: consumption must advance across reconnects
            self._behaviors = behaviors

        def psubscribe(self, *patterns):
            pass

        def get_message(self, timeout=None):
            behavior = self._behaviors.pop(0)
            if behavior == "raise":
                raise redis.exceptions.ConnectionError("reset")
            return behavior

    class _FakeConfirmSyncClient:
        def __init__(self, behaviors):
            self._behaviors = behaviors

        def pubsub(self):
            return _FakeConfirmSyncPubSub(self._behaviors)

    confirm = {"type": "psubscribe", "pattern": None, "channel": "ticks:*", "data": 1}
    behaviors = ["raise", confirm, "raise"]
    sub = SyncResilientPubSub(
        lambda: _FakeConfirmSyncClient(behaviors), patterns=["ticks:*"], sleep=calls.append
    )

    assert sub.get_message(timeout=1.0) is None  # first reconnect attempt
    assert sub.get_message(timeout=1.0) is None  # a confirmation, not a real message
    assert sub.get_message(timeout=1.0) is None  # second reconnect attempt
    # backoff kept escalating (1.0 -> 2.0) -- the confirmation must not have
    # reset it back to 1.0 between the two failures.
    assert calls == [1.0, 2.0]


def test_sync_reconnect_retries_with_backoff_instead_of_raising_when_redis_is_still_down():
    """I4's sync twin: the old code called `self._connect()` in the
    except branch outside any try, so a reconnect attempt failing because
    Redis is still down (not yet the next get_message()) raised straight
    out of get_message() and killed the supervisor. `client.pubsub()`
    fails twice here before succeeding; no exception must escape."""
    calls: list[float] = []

    class _FlakyReconnectSyncPubSub:
        def __init__(self, behaviors):
            # shared with the client: consumption must advance across reconnects
            self._behaviors = behaviors

        def psubscribe(self, *patterns):
            pass

        def get_message(self, timeout=None):
            behavior = self._behaviors.pop(0)
            if behavior == "raise":
                raise redis.exceptions.ConnectionError("reset")
            return behavior

    class _FlakyReconnectSyncClient:
        def __init__(self, behaviors, fail_counter):
            self._behaviors = behaviors
            self._fail_counter = fail_counter

        def pubsub(self):
            if self._fail_counter[0] > 0:
                self._fail_counter[0] -= 1
                raise redis.exceptions.ConnectionError("still down")
            return _FlakyReconnectSyncPubSub(self._behaviors)

    behaviors = ["raise", {"type": "pmessage", "data": "ok"}]
    fail_counter = [0]  # construction itself must succeed
    sub = SyncResilientPubSub(
        lambda: _FlakyReconnectSyncClient(behaviors, fail_counter),
        patterns=["ticks:*"],
        sleep=calls.append,
    )
    fail_counter[0] = 2  # only the reconnect after the first failure struggles

    assert sub.get_message(timeout=1.0) is None  # the reconnect attempt itself, twice-flaky
    assert sub.get_message(timeout=1.0) == {"type": "pmessage", "data": "ok"}
    assert calls == [1.0, 2.0, 4.0]
