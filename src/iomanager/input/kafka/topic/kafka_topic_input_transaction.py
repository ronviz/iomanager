try:
    from confluent_kafka import Consumer, TopicPartition
    from confluent_kafka import Message as KafkaRawMessage

    _HAS_KAFKA = True
except ImportError:
    _HAS_KAFKA = False

from iomanager.transaction.transaction import Transaction


class KafkaTopicInputTransaction(Transaction):
    """Handles manual offset committing and seeking rollback for a specific Kafka topic message."""

    def __init__(self, consumer: Consumer, raw_msg: KafkaRawMessage):
        if not _HAS_KAFKA:
            raise ImportError(
                "Kafka support requires the 'kafka' extra. Install it with: pip install 'iomanager[kafka]'"
            )
        self._consumer = consumer
        self._raw_msg = raw_msg
        self._completed = False

    def commit(self) -> None:
        if self._completed:
            return
        self._consumer.commit(message=self._raw_msg, asynchronous=False)
        self._completed = True

    def rollback(self) -> None:
        if self._completed:
            return
        topic = self._raw_msg.topic()
        partition = self._raw_msg.partition()
        offset = self._raw_msg.offset()

        if topic is None or partition is None or offset is None:
            raise RuntimeError(
                f"Cannot seek rollback: Kafka message is missing valid coordinates "
                f"(topic={topic!r}, partition={partition!r}, offset={offset!r}). "
                f"Expected a valid consumed record."
            )

        tp = TopicPartition(topic, partition, offset)
        self._consumer.seek(tp)
        self._completed = True
