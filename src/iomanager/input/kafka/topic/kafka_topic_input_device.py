from io import BytesIO

from confluent_kafka import Consumer, KafkaError, KafkaException

from iomanager.input.input_device import InputDevice
from iomanager.input.kafka.topic.kafka_topic_input_transaction import (
    KafkaTopicInputTransaction,
)
from iomanager.message import Message


class KafkaTopicInputDevice(InputDevice):
    """Reads messages from a standard Kafka topic."""

    def __init__(self, consumer: Consumer, topic: str, poll_timeout: float = 1.0):
        self._consumer = consumer
        self._topic = topic
        self._poll_timeout = poll_timeout
        self._consumer.subscribe([self._topic])

    def read_message(self) -> Message:
        """Blocks until a valid message is received from the Kafka topic."""
        while True:
            raw_msg = self._consumer.poll(timeout=self._poll_timeout)

            if raw_msg is None:
                continue

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
                        metadata[str(k)] = v.decode("utf-8") if isinstance(v, bytes) else str(v)

            transaction = KafkaTopicInputTransaction(self._consumer, raw_msg)

            return Message(
                data=data_stream,
                metadata=metadata,
                transaction=transaction,
            )