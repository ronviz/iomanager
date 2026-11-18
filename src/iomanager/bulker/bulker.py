from abc import ABC, abstractmethod

from iomanager.exceptions.base_io_excpetion import BaseIOException
from iomanager.message import Message


class PackingError(BaseIOException):
    pass


class SplittingError(BaseIOException):
    pass


class Bulker(ABC):
    @abstractmethod
    def pack(self, messages: list[Message]) -> Message:
        pass

    @abstractmethod
    def split(self, message: Message) -> list[Message]:
        pass
