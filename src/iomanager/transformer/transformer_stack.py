from typing import override

from iomanager.transformer.io_transformer import IOTransformer
from iomanager.transformer.transformer import Transformer


class TransformerStack(IOTransformer):
    def __init__(self, transformer_list: list[Transformer]):
        self._transformer_list = transformer_list

    @override
    def connect(self):
        for transformer in self._transformer_list:
            if isinstance(transformer, IOTransformer):
                transformer.connect()

    @override
    def close(self):
        for transformer in self._transformer_list:
            if isinstance(transformer, IOTransformer):
                transformer.close()

    @override
    def transform_input(self, message):
        for transformer in self._transformer_list:
            transformer.transform_input(message)

    @override
    def transform_output(self, message):
        for transformer in self._transformer_list:
            transformer.transform_output(message)
