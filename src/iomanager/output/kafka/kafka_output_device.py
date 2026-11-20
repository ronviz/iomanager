import logging

try:
    from confluent_kafka import KafkaError, KafkaException, Producer

    _HAS_KAFKA = True
except ImportError:
    _HAS_KAFKA = False

from iomanager.message import Message
from iomanager.output.output_device import OutputDevice


class KafkaOutputDevice(OutputDevice):
    """Publishes message payloads to a specific Kafka topic and commits/rolls back the incoming transaction."""

    def __init__(self, producer: Producer, topic: str, flush_timeout: float = 10.0):
        if not _HAS_KAFKA:
            raise ImportError(
                "Kafka support requires the 'kafka' extra. Install it with: pip install 'iomanager[kafka]'"
            )
        self._producer = producer
        self._topic = topic
        self._flush_timeout = flush_timeout

    def send_message(self, message: Message) -> None:
        """Sends a message payload to the Kafka topic and commits or rolls back its transaction."""
        delivery_error: KafkaError | None = None

        def _delivery_callback(err, _msg) -> None:
            nonlocal delivery_error
            if err is not None:
                delivery_error = err

        try:
            message.data.seek(0)
            payload = message.data.read()

            self._producer.produce(
                topic=self._topic,
                value=payload,
                on_delivery=_delivery_callback,
            )

            remaining = self._producer.flush(timeout=self._flush_timeout)
            if remaining > 0:
                raise TimeoutError(
                    f"Timed out after {self._flush_timeout}s waiting for Kafka delivery to '{self._topic}'."
                )

            if delivery_error is not None:
                raise KafkaException(delivery_error)

            message.transaction.commit()

        except:
            message.transaction.rollback()
            logging.error("Could not send to kafka queue", exc_info=True)
            raise
