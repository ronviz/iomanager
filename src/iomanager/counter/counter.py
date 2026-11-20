from abc import ABC, abstractmethod


class Counter(ABC):
    @abstractmethod
    def get_count(self, key: str) -> int:
        pass

    @abstractmethod
    def reset(self, key: str) -> None:
        """
        Sets count to 0 on that specific key
        """
        pass

    @abstractmethod
    def increment(self, key: str, amount: int = 1) -> int:
        """
        increments count by 1 and returns final count
        e.g. if count was 3, will return 4
        """
        pass

