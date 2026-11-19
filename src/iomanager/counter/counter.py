from abc import ABC, abstractmethod


class Counter(ABC):
    @abstractmethod
    def get_count(key: str) -> int:
        pass

    @abstractmethod
    def set_count(key: str) -> int:
        pass

    @abstractmethod
    def delete_key(key: str) -> None:
        pass

    @abstractmethod
    def increment(key: str, amount: int = 1) -> None:
        pass

    @abstractmethod
    def decrement(key: str, amount: int = 1) -> None:
        pass
