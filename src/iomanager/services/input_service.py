from abc import ABC, abstractmethod

from iomanager.input.input_device_manager import InputDeviceManager
from iomanager.message import Headers


class InputHandler(ABC):
    @abstractmethod
    def handle_message(self, stream: bytes, headers: Headers) -> None:
        pass


class InputService:
    def __init__(
        self,
        input_device_manager: InputDeviceManager,
        input_device_name: str,
        input_handler: InputHandler,
    ):
        self._input_device_manager = input_device_manager
        self._input_device_name = input_device_name
        self._input_handler = input_handler
        self._is_running = True

    def _run_once(self) -> None:
        device = self._input_device_manager.get_input_device(self._input_device_name)
        message = device.read_message()
        try:
            self._input_handler.handle_message(message.stream, message.headers)
        except:
            message.transaction.rollback()
            raise
        finally:
            message.transaction.commit()

    def start(self) -> None:
        with self._input_device_manager:
            while self._is_running:
                try:
                    self._run_once()
                except:
                    pass  # TODO add logging

    def stop(self) -> None:
        self._is_running = False
