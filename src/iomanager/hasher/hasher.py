from abc import ABC, abstractmethod

from iomanager.message import Message


class Hasher(ABC):
    """
    Should give a unique key to each message
    """

    @abstractmethod
    def hash_message(self, message: Message) -> str:
        pass
