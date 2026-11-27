from abc import ABC, abstractmethod
from dataclasses import dataclass

from iomanager.message import Message


@dataclass
class ProcessingResult:
    message: Message
    destination: str


class MessageProcessor(ABC):
    @abstractmethod
    def process_message(self, message: Message) -> list[ProcessingResult]:
        pass
