from dataclasses import dataclass
from io import BytesIO

from iomanager.transaction.transaction import Transaction


@dataclass
class Message:
    data: BytesIO
    metadata: dict[str, str]
    transaction: Transaction
