from typing import override

from iomanager.counter.counter import Counter
from iomanager.transaction.transaction import Transaction


class PoisonHandlingTransaction(Transaction):
    def __init__(
        self,
        counter: Counter,
        transaction: Transaction,
        message_key: str,
        max_backout_count: int,
    ):
        self._counter = counter
        self._transaction = transaction
        self._message_key = message_key

        try:
            if self._counter.get_count(self._message_key) > max_backout_count:
                self.commit()
            else:
                self._counter.increment(self._message_key)
        except:
            pass  # We do not want to fail if we cannot count. it is best-effort

    @override
    def commit(self) -> None:
        try:
            self._counter.delete_key(self._message_key)
        except:
            pass  # Again, best effort
        self._transaction.commit()

    @override
    def rollback(self) -> None:
        self._transaction.rollback()
