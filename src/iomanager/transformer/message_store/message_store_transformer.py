import json
import logging
from io import BytesIO
from typing import override

from iomanager.hasher.hasher import Hasher
from iomanager.hasher.sha_256_hasher import Sha256Hasher
from iomanager.iomanager import IOManager
from iomanager.message import Message
from iomanager.serializer.json_serializer import JsonSerializer
from iomanager.store.message_store_manager import MessageStoreManager
from iomanager.transformer.transformer import MessageTransformer

logger = logging.getLogger(__name__)


class MessageStoreTransformer(MessageTransformer, IOManager):
    def __init__(
        self,
        message_store_manager: MessageStoreManager,
        output_message_store_name: str,
        hasher: Hasher | None = None,
        should_store_headers: bool = False,
    ):
        self._message_store_manager = message_store_manager
        self._output_message_store_name = output_message_store_name
        self._hasher = hasher or Sha256Hasher(JsonSerializer())
        self._should_store_headers = should_store_headers

    @override
    async def connect(self) -> None:
        await self._message_store_manager.connect()

    @override
    async def close(self) -> None:
        await self._message_store_manager.close()

    def _encode_key(self, key: str, message_store_name: str) -> bytes:
        data = {"key": key, "message_store": message_store_name}
        return json.dumps(data).encode()

    def _decode_key(self, stream: bytes) -> tuple[str, str]:
        try:
            data = json.loads(stream.decode())
            return (
                data["key"],
                data["message_store"],
            )
        except Exception:
            logger.exception("Broken message encountered")
            raise

    @override
    async def transform_input(self, message: Message) -> None:
        message.data.seek(0)
        try:
            key, message_store_name = self._decode_key(message.data.read())
        except Exception:
            logger.exception("Could not decode message, assuming it's the original")
            return
        try:
            message_store = await self._message_store_manager.get_message_store(
                message_store_name
            )
            stored_message = await message_store.get_message(key)
            message.data = stored_message.data
            if self._should_store_headers:
                message.metadata = stored_message.metadata
        except Exception:
            logger.exception("Could not get message from message store")
            raise

    @override
    async def transform_output(self, message: Message) -> None:
        try:
            message_to_store = Message(
                data=message.data,
                metadata=message.metadata.copy(),
            )
            if not self._should_store_headers:
                message_to_store.metadata = {}

            message_store = await self._message_store_manager.get_message_store(
                self._output_message_store_name
            )
            key = self._hasher.hash_message(message_to_store)
            await message_store.put_message(key, message_to_store)
        except Exception:
            logger.exception("Could not put message in message store, keeping original message")
            return
        else:
            message.data = BytesIO(
                self._encode_key(key, self._output_message_store_name)
            )
            if self._should_store_headers:
                message.metadata = {}

        
