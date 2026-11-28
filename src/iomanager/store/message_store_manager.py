from abc import abstractmethod

from iomanager.exceptions.base_exception import IOManagerException
from iomanager.iomanager import IOManager
from iomanager.store.message_store import MessageStore


class MessageStoreNotFoundError(IOManagerException):
    pass


class MessageStoreManager(IOManager):
    @abstractmethod
    async def get_message_store(self, name: str) -> MessageStore:
        pass
