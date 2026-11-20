from abc import ABC, abstractmethod


class Counter(ABC):
    @abstractmethod
    def get_count(self, key: str) -> int:
        pass

    @abstractmethod
    def set_count(self, key: str) -> int:
        pass

    @abstractmethod
    def delete_key(self, key: str) -> None:
        pass

    @abstractmethod
    def increment(self, key: str, amount: int = 1) -> None:
        pass

    @abstractmethod
    def decrement(self, key: str, amount: int = 1) -> None:
        pass
