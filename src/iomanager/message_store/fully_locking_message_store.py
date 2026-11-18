from typing import override

from iomanager.lock.lock import Lock
from iomanager.message import Message
from iomanager.message_store.message_store import MessageStore
from iomanager.transaction import LockedTransaction


class FullyLockingMessageStore(MessageStore):
    def __init__(self, lock: Lock, message_store: MessageStore):
        self._lock = lock
        self._message_store = message_store

    @override
    def get_message(self, key: str) -> Message:
        self._lock.lock()
        message = self._message_store.get_message(key)
        locked_transaction = LockedTransaction(self._lock, message.transaction)
        message.transaction = locked_transaction

    @override
    def put_message(self, message: Message) -> str:
        self._lock.lock()
        message = self._message_store.put_message(message)
        locked_transaction = LockedTransaction(self._lock, message.transaction)
        message.transaction = locked_transaction
