from typing import override

from iomanager.counter.counter import Counter
from iomanager.hasher.hasher import Hasher
from iomanager.input.poison_handling.retry_tracker import RetryTracker
from iomanager.io_manager import IOManager
from iomanager.message import Message


class CounterRetryTracker(RetryTracker, IOManager):
    def __init__(self, counter: Counter, hasher: Hasher):
        self._counter = counter
        self._hasher = hasher

    @override
    def connect(self) -> None:
        if isinstance(self._counter, IOManager):
            self._counter.connect()
        if isinstance(self._hasher, IOManager):
            self._hasher.connect()

    @override
    def close(self) -> None:
        if isinstance(self._counter, IOManager):
            self._counter.close()
        if isinstance(self._hasher, IOManager):
            self._hasher.close()

    def _get_message_key(self, message: Message) -> str:
        message.data.seek(0)
        key = self._hasher.hash_message(message)
        return key

    @override
    def record_attempt(self, message: Message) -> int:
        key = self._get_message_key(message)
        prior_attempts = self._counter.increment(key) - 1
        return prior_attempts

    @override
    def clear(self, message: Message) -> None:
        key = self._get_message_key(message)
        self._counter.reset(key)
