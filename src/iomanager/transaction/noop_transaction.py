from typing import override

from iomanager.transaction.transaction import Transaction


class NoopTransaction(Transaction):
    @override
    def commit(self) -> None:
        pass

    @override
    def rollback(self) -> None:
        pass
