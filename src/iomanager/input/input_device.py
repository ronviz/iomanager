from abc import ABC, abstractmethod

from iomanager.message import Message


class InputDevice(ABC):
    @abstractmethod
    def read_message(self) -> Message:
        pass
