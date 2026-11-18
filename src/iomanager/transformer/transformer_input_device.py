from typing import override

from iomanager.input.input_device import InputDevice
from iomanager.message import Message
from iomanager.transformer.transformer import Transformer


class TransformerInputDevice(InputDevice):
    def __init__(self, inner_device: InputDevice, transformer: Transformer):
        self._inner_device = inner_device
        self._transformer = transformer

    @override
    def read_message(self) -> Message:
        message = self._inner_device.read_message()
        self._transformer.transform_input(message)
        return message
