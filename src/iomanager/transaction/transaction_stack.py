import logging
from typing import override

from iomanager.transaction.transaction import Transaction


class TransactionStack(Transaction):
    def __init__(self):
        self._stack: list[Transaction] = []

    @override
    def commit(self) -> None:
        while self._stack:
            pending_transaction = self._stack.pop()
            try:
                pending_transaction.commit()
            except:
                logging.warning("Could not commit transaction", exc_info=True)

    @override
    def rollback(self) -> None:
        while self._stack:
            pending_transaction = self._stack.pop()
            try:
                pending_transaction.rollback()
            except:
                logging.warning("Could not rollback transaction", exc_info=True)

    def push_transaction(self, transaction: Transaction):
        self._stack.append(transaction)
