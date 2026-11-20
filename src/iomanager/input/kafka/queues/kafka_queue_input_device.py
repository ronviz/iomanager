from io import BytesIO

from confluent_kafka import KafkaError, KafkaException, ShareConsumer

from iomanager.input.input_device import InputDevice
from iomanager.message import Message

from .kafka_queue_input_transaction import KafkaQueueInputTransaction


class KafkaQueueInputDevice(InputDevice):
    """Reads messages from a Kafka topic in Queue (Share Group) mode."""

    DELIVERY_COUNT_KEY = "delivery_count"

    def __init__(self, consumer: ShareConsumer, topic: str, poll_timeout: float = 1.0):
        self._consumer = consumer
        self._topic = topic
        self._poll_timeout = poll_timeout
        self._consumer.subscribe([self._topic])

    def read_message(self) -> Message:
        """Blocks until a valid message is received from the Kafka queue."""
        while True:
            raw_messages = self._consumer.poll(timeout=self._poll_timeout)

            if raw_messages is None:
                continue

            raw_msg = next(iter(raw_messages))

            error = raw_msg.error()
            if error:
                if error.code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(error)

            payload = raw_msg.value() or b""
            data_stream = BytesIO(payload)

            metadata: dict[str, str] = {}
            raw_headers = raw_msg.headers()
            if raw_headers:
                for k, v in raw_headers:
                    if v is not None:
                        metadata[str(k)] = (
                            v.decode("utf-8") if isinstance(v, bytes) else str(v)
                        )
            if raw_msg.delivery_count() is not None:
                metadata[self.DELIVERY_COUNT_KEY] = str(raw_msg.delivery_count())
            transaction = KafkaQueueInputTransaction(self._consumer, raw_msg)

            return Message(
                data=data_stream,
                metadata=metadata,
                transaction=transaction,
            )
