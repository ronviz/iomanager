from typing import override

from iomanager.message import Message
from iomanager.output.output_device import OutputDevice
from iomanager.transformer.transformer import Transformer


class TransformerOutputDevice(OutputDevice):
    def __init__(self, inner_device: OutputDevice, transformer: Transformer):
        self._inner_device = inner_device
        self._transformer = transformer

    @override
    def send_message(self, message: Message) -> None:
        self._transformer.transform_output(message)
        self._inner_device.send_message(message)
