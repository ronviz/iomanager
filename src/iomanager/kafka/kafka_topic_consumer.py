from aiokafka import AIOKafkaConsumer

from iomanager.input.message_consumer import MessageConsumer
from iomanager.kafka.kafka_message_decoder import KafkaMessageDecoder
from iomanager.kafka.kafka_topic_input_transaction import KafkaTopicInputTransaction
from iomanager.message import Message


class KafkaTopicMessageConsumer(MessageConsumer):
    def __init__(
        self,
        consumer: AIOKafkaConsumer,
        decoder: KafkaMessageDecoder | None = None,
    ) -> None:
        self._consumer = consumer
        self._decoder = decoder or KafkaMessageDecoder()

    async def get_message(self) -> tuple[Message, KafkaTopicInputTransaction]:
        record = await self._consumer.getone()
        message = self._decoder.decode(record)
        transaction = KafkaTopicInputTransaction(
            consumer=self._consumer,
            topic=record.topic,
            partition=record.partition,
            offset=record.offset,
        )
        return message, transaction
