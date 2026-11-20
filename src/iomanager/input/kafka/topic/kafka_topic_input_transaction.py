from confluent_kafka import Consumer, Message as KafkaRawMessage, TopicPartition

from iomanager.transaction.transaction import Transaction


class KafkaTopicInputTransaction(Transaction):
    """Handles manual offset committing and seeking rollback for a specific Kafka topic message."""

    def __init__(self, consumer: Consumer, raw_msg: KafkaRawMessage):
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
        tp = TopicPartition(
            self._raw_msg.topic(),
            self._raw_msg.partition(),
            self._raw_msg.offset(),
        )
        self._consumer.seek(tp)
        self._completed = True