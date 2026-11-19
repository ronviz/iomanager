from typing import override

from iomanager.io_manager import IOManager
from iomanager.transformer.transformer import Transformer


class TransformerStack(Transformer, IOManager):
    def __init__(self, transformer_list: list[Transformer]):
        self._transformer_list = transformer_list

    @override
    def connect(self):
        for transformer in self._transformer_list:
            if isinstance(transformer, IOManager):
                transformer.connect()

    @override
    def close(self):
        for transformer in self._transformer_list:
            if isinstance(transformer, IOManager):
                transformer.close()

    @override
    def transform_input(self, message):
        for transformer in self._transformer_list:
            transformer.transform_input(message)

    @override
    def transform_output(self, message):
        for transformer in self._transformer_list:
            transformer.transform_output(message)
