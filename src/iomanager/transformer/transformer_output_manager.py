from typing import override

from iomanager.iomanager import IOManager
from iomanager.output.output_manager import OutputManager
from iomanager.transformer.transformer import MessageTransformer
from iomanager.transformer.transformer_message_publisher import (
    TransformerMessagePublisher,
)


class TransformerOutputManager(OutputManager):
    def __init__(self, transformer: MessageTransformer, output_manager: OutputManager):
        self._transformer = transformer
        self._output_manager = output_manager

    @override
    async def connect(self) -> None:
        await self._output_manager.connect()
        if isinstance(self._transformer, IOManager):
            await self._transformer.connect()

    @override
    async def close(self) -> None:
        await self._output_manager.close()
        if isinstance(self._transformer, IOManager):
            await self._transformer.close()


    @override
    async def get_message_publisher(
        self, destination: str
    ) -> TransformerMessagePublisher:
        return TransformerMessagePublisher(
            transformer=self._transformer,
            message_publisher=await self._output_manager.get_message_publisher(
                destination
            ),
        )
