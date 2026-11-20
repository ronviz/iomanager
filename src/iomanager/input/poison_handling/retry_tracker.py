from abc import ABC, abstractmethod

from iomanager.message import Message


class RetryTracker(ABC):
    @abstractmethod
    def record_attempt(self, message: Message) -> int:
        """
        Records a delivery attempt and returns the number of PRIOR failed attempts 
        (e.g. 0 on first delivery, 1 on first retry, etc.).
        """

    @abstractmethod
    def clear(self, message: Message) -> None:
        """Cleans up tracking state after a message is successfully processed."""
