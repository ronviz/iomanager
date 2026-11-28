from abc import abstractmethod

from iomanager.exceptions.base_exception import IOManagerException
from iomanager.input.message_consumer import MessageConsumer
from iomanager.iomanager import IOManager


class MessageConsumerNotFoundError(IOManagerException):
    pass


class InputManager(IOManager):
    @abstractmethod
    async def get_message_consumer(self, source: str) -> MessageConsumer:
        pass
