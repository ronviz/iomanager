from pathlib import Path
from typing import Union, override
from .filesystem_message_store import FilesystemMessageStore
from .interfaces import (
    Hasher,
    IOManager,
    MessageStore,
    MessageStoreManager,
    MessageStoreNotFoundError,
    Serializer,
)


class FilesystemMessageStoreManager(MessageStoreManager):
    """Manages a root directory and spawns subdirectory-scoped FilesystemMessageStores."""

    def __init__(
        self,
        base_dir: Union[str, Path],
        hasher: Hasher,
        serializer: Serializer,
        verify_store_exists: bool = True,
        create_base_dir: bool = True,
    ):
        self._base_dir = Path(base_dir).resolve()
        self._hasher = hasher
        self._serializer = serializer
        self._verify_store_exists = verify_store_exists
        self._create_base_dir = create_base_dir
        self._is_connected = False

    @override
    def connect(self) -> None:
        """Validates (or creates) the root directory and initializes dependencies."""
        if isinstance(self._hasher, IOManager):
            self._hasher.connect()
        if isinstance(self._serializer, IOManager):
            self._serializer.connect()

        if self._create_base_dir:
            self._base_dir.mkdir(parents=True, exist_ok=True)
        elif not self._base_dir.is_dir():
            raise ConnectionError(f"Base directory '{self._base_dir}' does not exist.")

        self._is_connected = True

    @override
    def get_message_store(self, message_store_name: str) -> MessageStore:
        """Returns a FilesystemMessageStore backed by '<base_dir>/<message_store_name>'."""
        if not self._is_connected:
            raise RuntimeError(
                "FilesystemMessageStoreManager is not connected. Call connect() first."
            )

        store_dir = (self._base_dir / message_store_name).resolve()
        if not store_dir.is_relative_to(self._base_dir) or store_dir == self._base_dir:
            raise ValueError(
                f"Invalid message store name '{message_store_name}': escapes '{self._base_dir}'."
            )

        if self._verify_store_exists:
            if not store_dir.is_dir():
                raise MessageStoreNotFoundError(
                    f"Message store directory '{store_dir}' does not exist."
                )
        else:
            store_dir.mkdir(parents=True, exist_ok=True)

        return FilesystemMessageStore(
            store_dir=store_dir,
            hasher=self._hasher,
            serializer=self._serializer,
        )

    @override
    def close(self) -> None:
        """Cleans up resources."""
        if isinstance(self._hasher, IOManager):
            self._hasher.close()
        if isinstance(self._serializer, IOManager):
            self._serializer.close()
        self._is_connected = False
