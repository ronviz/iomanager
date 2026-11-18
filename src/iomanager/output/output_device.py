from abc import ABC, abstractmethod

from iomanager.message import Message


class OutputDevice(ABC):
    @abstractmethod
    def send_message(self, message: Message) -> None:
        pass
