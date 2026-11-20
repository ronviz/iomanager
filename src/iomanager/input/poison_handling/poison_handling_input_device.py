import logging
from typing import override

from iomanager.exceptions.base_io_excpetion import BaseIOException
from iomanager.input.input_device import InputDevice
from iomanager.input.poison_handling.poison_handling_transaction import (
    PoisonHandlingTransaction,
)
from iomanager.input.poison_handling.retry_tracker import RetryTracker
from iomanager.message import Message


class PoisonMessageError(BaseIOException):
    pass


class PoisonHandlingInputDevice(InputDevice):
    def __init__(
        self,
        input_device: InputDevice,
        retry_tracker: RetryTracker,
        max_backout_count: int,
    ):
        self._input_device = input_device
        self._retry_tracker = retry_tracker
        self._max_backout_count = max_backout_count

    @override
    def read_message(self) -> Message:
        message = self._input_device.read_message()

        try:
            prior_attempts = self._retry_tracker.record_attempt(message)
        except Exception:
            logging.warning(
                "RetryTracker failed; proceeding without poison check", exc_info=True
            )
            prior_attempts = 0

        if prior_attempts > self._max_backout_count:
            logging.error("Poison message detected. Dropping.")
            message.transaction.commit()
            raise PoisonMessageError("Poison message dropped")

        poison_transaction = PoisonHandlingTransaction(
            transaction=message.transaction,
            retry_tracker=self._retry_tracker,
            message=message,
        )

        return Message(
            data=message.data,
            metadata=message.metadata.copy(),
            transaction=poison_transaction,
        )
