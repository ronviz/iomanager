from abc import ABC, abstractmethod

from iomanager.message import Message


class MessageTransformer(ABC):
    @abstractmethod
    async def transform_input(self, message: Message) -> None:
        pass

    @abstractmethod
    async def transform_output(self, message: Message) -> None:
        pass
