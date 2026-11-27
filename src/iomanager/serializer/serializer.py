from abc import ABC, abstractmethod
from io import BytesIO

from iomanager.exceptions.base_exception import IOManagerException
from iomanager.message import Message


class SerializationError(IOManagerException):
    pass


class DeserializationError(IOManagerException):
    pass


class Serializer(ABC):
    @abstractmethod
    def serialize(self, message: Message) -> BytesIO:
        pass

    @abstractmethod
    def deserialize(self, data: BytesIO) -> Message:
        pass
