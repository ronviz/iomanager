from abc import ABC, abstractmethod

from iomanager.message import Message


class MessagePublisher(ABC):
    @abstractmethod
    async def send_message(self, message: Message) -> None:
        pass
