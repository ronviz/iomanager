from abc import abstractmethod

from iomanager.exceptions.base_io_excpetion import BaseIOException
from iomanager.io_manager import IOManager
from iomanager.output.output_device import OutputDevice


class OutputDeviceNotFoundError(BaseIOException):
    pass


class OutputDeviceManager(IOManager):
    @abstractmethod
    def get_output_device(self, device_name: str) -> OutputDevice:
        pass
