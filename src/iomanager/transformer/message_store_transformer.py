import json
import logging
from io import BytesIO
from typing import override

from iomanager.io_manager import IOManager
from iomanager.message import Message
from iomanager.message_store.message_store import (
    MessageAlreadyExistsError,
    MessageNotFoundError,
)
from iomanager.message_store.message_store_manager import MessageStoreManager
from iomanager.transaction.transaction_stack import TransactionStack
from iomanager.transformer.transformer import Transformer


class MessageStoreTransformer(Transformer, IOManager):
    def __init__(
        self,
        message_store_manager: MessageStoreManager,
        output_message_store_name: str,
        should_store_headers: bool,
    ):
        self._message_store_manager = message_store_manager
        self._output_message_store_name = output_message_store_name
        self._should_store_headers = should_store_headers

    @override
    def connect(self) -> None:
        self._message_store_manager.connect()

    @override
    def close(self) -> None:
        self._message_store_manager.close()

    def _encode_key(self, key: str, message_store_name: str) -> bytes:
        data = {"key": key, "message_store": message_store_name}
        return json.dumps(data).encode()

    def _decode_key(self, stream: bytes) -> tuple[str, str]:
        data = json.loads(stream.decode())
        return (
            data["key"],
            data["message_store"],
        )  # TODO raise custom error on broken message

    @override
    def transform_input(self, message: Message) -> None:
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
            if self._should_store_headers:
                message.metadata = stored_message.metadata
            message.transaction = transaction_stack
        except MessageNotFoundError:
            logging.warning("Message does not exist in message store. Dropping")
            transaction_stack.commit()
            raise
        except Exception:
            transaction_stack.rollback()
            raise

    @override
    def transform_output(self, message: Message) -> None:
        transaction_stack = TransactionStack()
        transaction_stack.push_transaction(message.transaction)
        try:
            message_to_store = Message(
                data=message.data,
                metadata=message.metadata.copy(),
                transaction=message.transaction,
            )
            if not self._should_store_headers:
                message_to_store.metadata = {}

            key = self._message_store_manager.get_message_store(
                self._output_message_store_name
            ).put_message(message_to_store)
            transaction_stack.push_transaction(message_to_store.transaction)

            message.data = BytesIO(
                self._encode_key(key, self._output_message_store_name)
            )
            message.transaction = transaction_stack
            if self._should_store_headers:
                message.metadata = {}

        except MessageAlreadyExistsError:
            logging.warning("Message already exists in message store", exc_info=True)
        except Exception:
            transaction_stack.rollback()
            raise
