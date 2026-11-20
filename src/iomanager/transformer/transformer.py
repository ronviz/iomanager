from abc import ABC, abstractmethod

from iomanager.message import Message


class Transformer(ABC):
    @abstractmethod
    def transform_input(self, message: Message):
        pass

    @abstractmethod
    def transform_output(self, message: Message):
        pass
