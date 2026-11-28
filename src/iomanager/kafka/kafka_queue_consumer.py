from aiokafka import AIOKafkaConsumer

from iomanager.input.message_consumer import MessageConsumer
from iomanager.kafka.kafka_message_decoder import KafkaMessageDecoder
from iomanager.kafka.kafka_queue_input_transaction import KafkaQueueInputTransaction
from iomanager.message import Message


class KafkaQueueMessageConsumer(MessageConsumer):
    def __init__(
        self,
        consumer: AIOKafkaConsumer,
        decoder: KafkaMessageDecoder | None = None,
    ) -> None:
        self._consumer = consumer
        self._decoder = decoder or KafkaMessageDecoder()

    async def get_message(self) -> tuple[Message, KafkaQueueInputTransaction]:
        record = await self._consumer.getone()
        message = self._decoder.decode(record)
        transaction = KafkaQueueInputTransaction(
            consumer=self._consumer,
            topic=record.topic,
            partition=record.partition,
            offset=record.offset,
        )
        return message, transaction
