import logging
from typing import override

from iomanager.input.message_consumer import MessageConsumer
from iomanager.message import Message
from iomanager.transaction import Transaction
from iomanager.transformer.transformer import MessageTransformer

logger = logging.getLogger(__name__)


class TransformerMessageConsumer(MessageConsumer):
    def __init__(
        self, transformer: MessageTransformer, message_consumer: MessageConsumer
    ):
        self._transformer = transformer
        self._message_consumer = message_consumer

    @override
    async def get_message(self) -> tuple[Message, Transaction]:
        message, transaction = await self._message_consumer.get_message()
        try:
            await self._transformer.transform_input(message)
        except Exception:
            logger.exception("Could not transfrom consumed message")
            transaction.rollback()
            raise
        return message, transaction
