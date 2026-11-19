from typing import override

from iomanager.input.input_device_manager import InputDeviceManager
from iomanager.io_manager import IOManager
from iomanager.transformer.transformer import Transformer
from iomanager.transformer.transformer_input_device import TransformerInputDevice


class TransformerInputDeviceManager(InputDeviceManager):
    def __init__(self, device_manager: InputDeviceManager, transformer: Transformer):
        self._device_manager = device_manager
        self._transformer = transformer

    @override
    def connect(self) -> None:
        if isinstance(self._transformer, IOManager):
            self._transformer.connect()
        self._device_manager.connect()

    @override
    def close(self) -> None:
        if isinstance(self._transformer, IOManager):
            self._transformer.close()
        self._device_manager.close()

    @override
    def get_input_device(self, device_name: str) -> TransformerInputDevice:
        return TransformerInputDevice(
            inner_device=self._device_manager.get_input_device(device_name=device_name),
            transformer=self._transformer,
        )
