from confluent_kafka import AcknowledgeType, ShareConsumer
from confluent_kafka import Message as KafkaRawMessage

from iomanager.transaction.transaction import Transaction


class KafkaQueueInputTransaction(Transaction):
    """Handles per-message acknowledgment (ACCEPT / RELEASE) for Kafka Queues (Share Groups)."""

    def __init__(self, consumer: ShareConsumer, raw_msg: KafkaRawMessage):
        self._consumer = consumer
        self._raw_msg = raw_msg
        self._completed = False

    def commit(self) -> None:
        if self._completed:
            return
        self._consumer.acknowledge(self._raw_msg, AcknowledgeType.ACCEPT)
        self._consumer.commit_sync()
        self._completed = True

    def rollback(self) -> None:
        if self._completed:
            return
        self._consumer.acknowledge(self._raw_msg, AcknowledgeType.RELEASE)
        self._consumer.commit_sync()
        self._completed = True