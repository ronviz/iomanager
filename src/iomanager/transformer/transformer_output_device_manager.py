from typing import override

from iomanager.output.output_device_manager import OutputDeviceManager
from iomanager.transformer.io_transformer import IOTransformer
from iomanager.transformer.transformer import Transformer
from iomanager.transformer.transformer_output_device import TransformerOutputDevice


class TransformerOutputDeviceManager(OutputDeviceManager):
    def __init__(self, device_manager: OutputDeviceManager, transformer: Transformer):
        self._device_manager = device_manager
        self._transformer = transformer

    @override
    def connect(self) -> None:
        if isinstance(self._transformer, IOTransformer):
            self._transformer.connect()
        self._device_manager.connect()

    @override
    def close(self) -> None:
        if isinstance(self._transformer, IOTransformer):
            self._transformer.close()
        self._device_manager.close()

    @override
    def get_output_device(self, device_name: str) -> TransformerOutputDevice:
        return TransformerOutputDevice(
            inner_device=self._device_manager.get_output_device(device_name=device_name),
            transformer=self._transformer,
        )
