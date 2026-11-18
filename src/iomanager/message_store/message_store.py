from abc import ABC, abstractmethod

from iomanager.exceptions.base_io_excpetion import BaseIOException
from iomanager.message import Message


class MessageStoreError(BaseIOException):
    pass


class MessageNotFoundError(MessageStoreError):
    pass


class MessageAlreadyExistsError(MessageStoreError):
    pass


class MessageStore(ABC):
    @abstractmethod
    def get_message(self, key: str) -> Message:
        pass

    @abstractmethod
    def put_message(self, message: Message) -> str:
        pass
