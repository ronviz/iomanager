from io import BytesIO
from typing import Dict
from confluent_kafka import Consumer, KafkaError, KafkaException

from iomanager.input.input_device import InputDevice
from iomanager.message import Message
from .kafka_queue_input_transaction import KafkaQueueInputTransaction


class KafkaQueueInputDevice(InputDevice):
    """Reads messages from a Kafka topic in Queue (Share Group) mode."""

    def __init__(self, consumer: Consumer, topic: str, poll_timeout: float = 1.0):
        self._consumer = consumer
        self._topic = topic
        self._poll_timeout = poll_timeout
        self._consumer.subscribe([self._topic])

    def read_message(self) -> Message:
        """Blocks until a valid message is received from the Kafka queue."""
        while True:
            raw_msg = self._consumer.poll(timeout=self._poll_timeout)

            if raw_msg is None:
                continue

            if raw_msg.error():
                if raw_msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(raw_msg.error())

            payload = raw_msg.value() or b""
            data_stream = BytesIO(payload)

            metadata: Dict[str, str] = {}
            if raw_msg.headers():
                for k, v in raw_msg.headers():
                    if v is not None:
                        metadata[k] = v.decode("utf-8") if isinstance(v, bytes) else str(v)

            transaction = KafkaQueueInputTransaction(self._consumer, raw_msg)

            return Message(
                data=data_stream,
                metadata=metadata,
                transaction=transaction,
            )