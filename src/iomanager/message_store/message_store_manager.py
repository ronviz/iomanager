from abc import abstractmethod

from iomanager.exceptions.base_io_excpetion import BaseIOException
from iomanager.io_manager import IOManager
from iomanager.message_store.message_store import MessageStore


class MessageStoreNotFoundError(BaseIOException):
    pass


class MessageStoreManager(IOManager):
    @abstractmethod
    def get_message_store(self, message_store_name: str) -> MessageStore:
        pass
