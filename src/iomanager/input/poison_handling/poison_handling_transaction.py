import logging
from typing import override

from iomanager.input.poison_handling.retry_tracker import RetryTracker
from iomanager.message import Message
from iomanager.transaction.transaction import Transaction


class PoisonHandlingTransaction(Transaction):
    def __init__(
        self,
        transaction: Transaction,
        retry_tracker: RetryTracker,
        message: Message,
    ):
        self._transaction = transaction
        self._retry_tracker = retry_tracker
        self._message = message

    @override
    def commit(self) -> None:
        try:
            self._retry_tracker.clear(self._message)
        except Exception:
            logging.warning("Could not clear message from retry tracker", exc_info=True)
        self._transaction.commit()

    @override
    def rollback(self) -> None:
        self._transaction.rollback()
