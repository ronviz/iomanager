from typing import override

from iomanager.input.kafka.queues.kafka_queue_input_device import KafkaQueueInputDevice
from iomanager.input.poison_handling.retry_tracker import RetryTracker
from iomanager.message import Message


class KafkaQueueRetryTracker(RetryTracker):
    """Reads Kafka Share Group's native delivery_count from message metadata."""

    DEFAULT_DELIVERY_COUNT = 1

    @override
    def record_attempt(self, message: Message) -> int:
        delivery_count = int(
            message.metadata.get(
                KafkaQueueInputDevice.DELIVERY_COUNT_KEY,
                self.DEFAULT_DELIVERY_COUNT,
            )
        )
        return delivery_count - 1

    @override
    def clear(self, message: Message) -> None:
        pass
