import logging

from aiokafka import AIOKafkaConsumer
from aiokafka.structs import OffsetAndMetadata, TopicPartition

from iomanager.transaction import Transaction

logger = logging.getLogger(__name__)


class KafkaQueueInputTransaction(Transaction):
    def __init__(
        self,
        consumer: AIOKafkaConsumer,
        topic: str,
        partition: int,
        offset: int,
    ) -> None:
        self._consumer = consumer
        self._tp = TopicPartition(topic, partition)
        self._offset = offset
        self._completed = False

    async def commit(self) -> None:
        if self._completed:
            return
        self._completed = True

        try:
            offsets = {self._tp: OffsetAndMetadata(self._offset + 1, "")}
            await self._consumer.commit(offsets)
        except Exception:
            logger.exception(
                "Failed to commit offset %d for topic '%s' partition %d",
                self._offset + 1,
                self._tp.topic,
                self._tp.partition,
            )

    async def rollback(self) -> None:
        if self._completed:
            return
        self._completed = True

        try:
            self._consumer.seek(self._tp, self._offset)
        except Exception:
            logger.exception(
                "Failed to rollback (seek) to offset %d for topic '%s' partition %d",
                self._offset,
                self._tp.topic,
                self._tp.partition,
            )
