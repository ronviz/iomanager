from abc import ABC, abstractmethod
from io import BytesIO

from iomanager.exceptions.base_io_excpetion import BaseIOException
from iomanager.message import Message


class SerializationError(BaseIOException):
    pass


class DeserializationError(BaseIOException):
    pass


class Serializer(ABC):
    @abstractmethod
    def serialize(self, message: Message) -> BytesIO:
        pass

    @abstractmethod
    def deserialize(self, data: BytesIO) -> Message:
        pass
