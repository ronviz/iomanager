from aiokafka import AIOKafkaProducer

from iomanager.message import Message
from iomanager.output.message_publisher import MessagePublisher


class KafkaMessagePublisher(MessagePublisher):
    def __init__(self, producer: AIOKafkaProducer, topic: str) -> None:
        self._producer = producer
        self._topic = topic

    async def send_message(self, message: Message) -> None:
        message.data.seek(0)
        headers = [
            (key, value.encode("utf-8")) for key, value in message.metadata.items()
        ]
        await self._producer.send_and_wait(
            topic=self._topic,
            value=message.data.read(),
            headers=headers,
        )
