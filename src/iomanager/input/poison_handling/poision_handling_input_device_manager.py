from typing import override

from iomanager.counter.counter import Counter
from iomanager.hasher.hasher import Hasher
from iomanager.input.input_device_manager import InputDeviceManager
from iomanager.input.poison_handling.poison_handling_input_device import (
    PoisonHandlingInputDevice,
)
from iomanager.io_manager import IOManager


class PoisonHandlingInputDeviceManager(InputDeviceManager):
    def __init__(
        self,
        counter: Counter,
        input_device_manager: InputDeviceManager,
        hasher: Hasher,
        max_backout_count: int,
    ):
        self._counter = counter
        self._input_device_manager = input_device_manager
        self._hasher = hasher
        self._max_backout_count = max_backout_count

    @override
    def connect(self):
        if isinstance(self._counter, IOManager):
            self._counter.connect()
        if isinstance(self._hasher, IOManager):
            self._hasher.connect()
        self._input_device_manager.connect()

    @override
    def close(self):
        if isinstance(self._counter, IOManager):
            self._counter.close()
        if isinstance(self._hasher, IOManager):
            self._hasher.close()
        self._input_device_manager.close()

    @override
    def get_input_device(self, device_name: str) -> PoisonHandlingInputDevice:
        input_device = self._input_device_manager.get_input_device(device_name)
        return PoisonHandlingInputDevice(
            counter=self._counter,
            input_device=input_device,
            hasher=self._hasher,
            max_backout_count=self._max_backout_count,
        )
