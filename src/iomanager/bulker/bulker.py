from abc import ABC, abstractmethod

from iomanager.exceptions.base_exception import IOManagerException
from iomanager.message import Message


class PackingError(IOManagerException):
    pass


class SplittingError(IOManagerException):
    pass


class Bulker(ABC):
    @abstractmethod
    def pack(self, messages: list[Message]) -> Message:
        pass

    @abstractmethod
    def split(self, message: Message) -> list[Message]:
        pass
