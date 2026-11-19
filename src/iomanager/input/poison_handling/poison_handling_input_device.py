from typing import override

from iomanager.counter.counter import Counter
from iomanager.hasher.hasher import Hasher
from iomanager.input.input_device import InputDevice
from iomanager.input.poison_handling.poison_handling_transaction import (
    PoisonHandlingTransaction,
)
from iomanager.message import Message


class PoisonHandlingInputDevice(InputDevice):
    def __init__(
        self,
        counter: Counter,
        input_device: InputDevice,
        hasher: Hasher,
        max_backout_count: int,
    ):
        self._counter = counter
        self._input_device = input_device
        self._hasher = hasher
        self._max_backuout_count = max_backout_count

    @override
    def read_message(self) -> Message:
        message = self._input_device.read_message()
        message_key = self._hasher.hash_message(message)
        message.transaction = PoisonHandlingTransaction(
            transaction=message.transaction,
            counter=self._counter,
            message_key=message_key,
            max_backout_count=self._max_backuout_count
        )
        return message
