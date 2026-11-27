from typing import override

from iomanager.input.input_manager import InputManager
from iomanager.input.message_consumer import MessageConsumer
from iomanager.iomanager import IOManager
from iomanager.transformer.transformer import MessageTransformer
from iomanager.transformer.transformer_message_consumer import (
    TransformerMessageConsumer,
)


class TransformerInputManager(InputManager):
    def __init__(self, transformer: MessageTransformer, input_manager: InputManager):
        self._transformer = transformer
        self._input_manager = input_manager

    @override
    async def connect(self) -> None:
        await self._input_manager.connect()
        if isinstance(self._transformer, IOManager):
            await self._transformer.connect()

    @override
    async def close(self) -> None:
        await self._input_manager.close()
        if isinstance(self._transformer, IOManager):
            await self._transformer.close()

    @override
    async def get_message_consumer(self, source: str) -> MessageConsumer:
        return TransformerMessageConsumer(
            transformer=self._transformer,
            message_consumer=await self._input_manager.get_message_consumer(source),
        )
