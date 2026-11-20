from abc import ABC, abstractmethod


class IOManager(ABC):
    @abstractmethod
    def connect(self) -> None:
        pass

    @abstractmethod
    def close(self) -> None:
        pass

    def __enter__(self):
        self.connect()

    def __exit__(self, exc_type, exc, tb):
        self.close()
