from typing import override

from iomanager.lock.lock import Lock
from iomanager.transaction.transaction import Transaction


class LockedTransaction(Transaction):
    def __init__(self, lock: Lock, transaction: Transaction):
        self._lock = lock
        self._transaction = transaction

    @override
    def commit(self) -> None:
        self._lock.release()
        self._transaction.commit()

    @override
    def rollback(self) -> None:
        self._lock.release()
        self._transaction.rollback()
