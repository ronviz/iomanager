from io import BytesIO
import json
from typing import override

from iomanager.message import Message
from iomanager.message_store.message_store import (
    MessageAlreadyExistsError,
    MessageNotFoundError,
)
from iomanager.message_store.message_store_manager import MessageStoreManager
from iomanager.transaction import TransactionStack
from iomanager.transformer.io_transformer import IOTransformer


class MessageStoreTransformer(IOTransformer):
    def __init__(
        self, message_store_manager: MessageStoreManager, output_message_store_name: str
    ):
        self._message_store_manager = message_store_manager
        self._output_message_store_name = output_message_store_name

    @override
    def connect(self) -> None:
        self._message_store_manager.connect()

    @override
    def close(self) -> None:
        self._message_store_manager.close()

    def _encode_key(key: str, message_store_name: str) -> bytes:
        data = {"key": key, "message_store": message_store_name}
        return json.dumps(data).encode()

    def _decode_key(stream: bytes) -> tuple[str, str]:
        data = json.loads(stream.decode())
        return (
            data["key"],
            data["message_store"],
        )  # TODO raise custom error on broken message

    @override
    def transform_input(self, message: Message):
        transaction_stack = TransactionStack()
        transaction_stack.push_transaction(message.transaction)
        message.data.seek(0)
        key, message_store_name = self._decode_key(message.data.read())
        try:
            stored_message = self._message_store_manager.get_message_store(
                message_store_name
            ).get_message(key)
            transaction_stack.push_transaction(stored_message.transaction)
            message.data = stored_message.data
            message.headers = stored_message.headers
            message.transaction = self._transaction_stack
        except MessageNotFoundError:
            transaction_stack.commit()
            raise
        except Exception:
            transaction_stack.rollback()
            raise

    @override
    def transform_output(self, message: Message) -> Message:
        transaction_stack = TransactionStack()
        transaction_stack.push_transaction(message.transaction)
        try:
            key = self._message_store_manager.get_message_store(
                self._output_message_store_name
            ).put_message(message)
            transaction_stack.push_transaction(message.transaction)
            message.data = BytesIO(self._encode_key(key, self._output_message_store_name))
            message.transaction = self._transaction_stack
        except MessageAlreadyExistsError:
            pass
        except Exception:
            transaction_stack.rollback()
            raise
