from abc import ABC, abstractmethod

from iomanager.input.input_device_manager import InputDeviceManager
from iomanager.message import Headers, Message
from iomanager.output.output_device_manager import OutputDeviceManager


class PipelineHandler(ABC):
    @abstractmethod
    def process_pipeline(
        self, stream: bytes, headers: Headers
    ) -> dict[str, tuple[bytes, Headers]]:
        pass


class PipelineService:
    def __init__(
        self,
        input_device_manager: InputDeviceManager,
        output_device_manager: OutputDeviceManager,
        input_device_name: str,
        pipeline_handler: PipelineHandler,
    ):
        self._input_device_manager = input_device_manager
        self._output_device_manager = output_device_manager
        self._input_device_name = input_device_name
        self._pipeline_handler = pipeline_handler
        self._is_running = True

    def _run_once(self) -> None:
        input_device = self._input_device_manager.get_input_device(
            self._input_device_name
        )
        raw_message = input_device.read_message()

        try:
            result = self._pipeline_handler.process_pipeline(raw_message)
        except:
            raw_message.transaction.rollback()
            raise

        for output_device_name, (stream, headers) in result.items():
            output_device = self._output_device_manager.get_output_device(
                output_device_name
            )
            output_device.send_message(
                Message(stream, headers, raw_message.transaction)
            )

    def start(self) -> None:
        with self._input_device_manager, self._output_device_manager:
            while self._is_running:
                try:
                    self._run_once()
                except:
                    pass  # TODO add logging

    def stop(self) -> None:
        self._is_running = False
