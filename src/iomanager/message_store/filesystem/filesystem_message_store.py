import logging
from io import BytesIO
from pathlib import Path
from typing import override

from iomanager.hasher.hasher import Hasher
from iomanager.message import Message
from iomanager.message_store.message_store import MessageAlreadyExistsError, MessageNotFoundError, MessageStore
from iomanager.serializer.serializer import Serializer
from iomanager.transaction.noop_transaction import NoopTransaction


class FilesystemMessageStore(MessageStore):
    """Stores, retrieves, and deletes serialized Message objects in a local directory."""

    def __init__(
        self,
        store_dir: Path,
        hasher: Hasher,
        serializer: Serializer,
    ):
        self._store_dir = store_dir
        self._hasher = hasher
        self._serializer = serializer

    def _compute_key(self, message: Message) -> str:
        key = self._hasher.hash_message(message)
        return key

    @override
    def get_message(self, key: str) -> Message:
        """Fetches and deserializes a message from the directory by key."""
        file_path = self._store_dir / key

        try:
            raw_bytes = file_path.read_bytes()
        except FileNotFoundError as e:
            raise MessageNotFoundError(
                f"Message with key '{key}' not found in '{self._store_dir}'."
            ) from e

        message = self._serializer.deserialize(BytesIO(raw_bytes))
        message.data.seek(0)

        return Message(
            data=message.data,
            metadata=message.metadata,
            transaction=NoopTransaction(),
        )

    @override
    def put_message(self, message: Message) -> str:
        """Hashes and serializes the message, writing it to disk."""
        key = self._compute_key(message)
        file_path = self._store_dir / key

        if file_path.exists():
            raise MessageAlreadyExistsError(
                f"Message with key '{key}' already exists in '{self._store_dir}'."
            )

        serialized_stream = self._serializer.serialize(message)
        serialized_stream.seek(0)
        file_path.write_bytes(serialized_stream.read())

        return key

    @override
    def delete_message(self, key: str) -> None:
        """Deletes a message file from disk by key."""
        file_path = self._store_dir / key

        if not file_path.exists():
            logging.warning(
                f"Message with key '{key}' not found in '{self._store_dir}'; skipping delete."
            )
            return

        file_path.unlink(missing_ok=True)