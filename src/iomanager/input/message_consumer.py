from abc import ABC, abstractmethod

from iomanager.message import Message
from iomanager.transaction import Transaction


class MessageConsumer(ABC):
    @abstractmethod
    async def get_message(self) -> tuple[Message, Transaction]:
        pass
