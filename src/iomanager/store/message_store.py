from abc import ABC, abstractmethod

from iomanager.exceptions.base_exception import IOManagerException
from iomanager.message import Message


class MessageNotFoundError(IOManagerException):
    pass


class MessageStore(ABC):
    @abstractmethod
    async def get_message(self, key: str) -> Message:
        pass

    @abstractmethod
    async def put_message(self, key: str, message: Message):
        pass

    @abstractmethod
    async def delete_message(self, key: str) -> None:
        pass
