from abc import abstractmethod

from iomanager.exceptions.base_exception import IOManagerException
from iomanager.iomanager import IOManager
from iomanager.output.message_publisher import MessagePublisher


class MessagePublisherNotFoundError(IOManagerException):
    pass


class OutputManager(IOManager):
    @abstractmethod
    async def get_message_publisher(self, destination: str) -> MessagePublisher:
        pass
