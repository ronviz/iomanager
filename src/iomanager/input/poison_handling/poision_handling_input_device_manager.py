from typing import override

from iomanager.input.input_device_manager import InputDeviceManager
from iomanager.input.poison_handling.poison_handling_input_device import (
    PoisonHandlingInputDevice,
)
from iomanager.input.poison_handling.retry_tracker import RetryTracker
from iomanager.io_manager import IOManager


class PoisonHandlingInputDeviceManager(InputDeviceManager):
    def __init__(
        self,
        input_device_manager: InputDeviceManager,
        retry_tracker: RetryTracker,
        max_backout_count: int,
    ):
        self._input_device_manager = input_device_manager
        self._retry_tracker = retry_tracker
        self._max_backout_count = max_backout_count

    @override
    def connect(self):
        if isinstance(self._retry_tracker, IOManager):
            self._retry_tracker.connect()
        self._input_device_manager.connect()

    @override
    def close(self):
        if isinstance(self._retry_tracker, IOManager):
            self._retry_tracker.close()
        self._input_device_manager.close()

    @override
    def get_input_device(self, device_name: str) -> PoisonHandlingInputDevice:
        input_device = self._input_device_manager.get_input_device(device_name)
        return PoisonHandlingInputDevice(
            input_device=input_device,
            retry_tracker=self._retry_tracker,
            max_backout_count=self._max_backout_count,
        )
