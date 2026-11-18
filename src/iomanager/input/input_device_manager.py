from abc import abstractmethod

from iomanager.exceptions.base_io_excpetion import BaseIOException
from iomanager.input.input_device import InputDevice
from iomanager.io_manager import IOManager


class InputDeviceNotFoundError(BaseIOException):
    pass


class InputDeviceManager(IOManager):
    @abstractmethod
    def get_input_device(self, device_name: str) -> InputDevice:
        pass
