from typing import override

from iomanager.transaction.transaction import Transaction


class TransactionStack(Transaction):
    def __init__(self):
        self._stack: list[Transaction] = []

    @override
    def commit(self) -> None:
        while self._stack:
            pending_transaction = self._stack.pop()
            pending_transaction.commit()

    @override
    def rollback(self) -> None:
        while self._stack:
            pending_transaction = self._stack.pop()
            pending_transaction.rollback()

    def push_transaction(self, transaction: Transaction):
        self._stack.append(transaction)
