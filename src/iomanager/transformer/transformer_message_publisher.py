from typing import override

from iomanager.message import Message
from iomanager.output.message_publisher import MessagePublisher
from iomanager.transformer.transformer import MessageTransformer


class TransformerMessagePublisher(MessagePublisher):
    def __init__(
        self, transformer: MessageTransformer, message_publisher: MessagePublisher
    ):
        self._transformer = transformer
        self._message_publisher = message_publisher

    @override
    async def send_message(self, message: Message) -> None:
        await self._transformer.transform_output(message)
        await self._message_publisher.send_message(message)
