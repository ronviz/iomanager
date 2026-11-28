from abc import ABC, abstractmethod


class IOManager(ABC):
    @abstractmethod
    async def connect(self) -> None:
        pass

    @abstractmethod
    async def close(self) -> None:
        pass
