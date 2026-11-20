from typing import override

from iomanager.lock.lock_factory import LockFactory
from iomanager.message import Message
from iomanager.message_store.message_store import MessageStore
from iomanager.transaction.locked_transaction import LockedTransaction


class WriteLockMessageStore(MessageStore):
    def __init__(self, lock_factory: LockFactory, message_store: MessageStore):
        self._lock_factory = lock_factory
        self._message_store = message_store

    @override
    def get_message(self, key: str) -> Message:
        return self._message_store.get_message(key)

    @override
    def put_message(self, message: Message) -> str:
        key = self._message_store.put_message(message)
        lock = self._lock_factory.get_lock(key)
        locked_transaction = LockedTransaction(lock, message.transaction)
        message.transaction = locked_transaction
        return key

    @override
    def delete_message(self, key: str) -> None:
        self._message_store.delete_message(key)
