import hashlib
from typing import override

from iomanager.hasher.hasher import Hasher
from iomanager.message import Message
from iomanager.serializer.serializer import Serializer


class Sha256Hasher(Hasher):
    """Generates a deterministic SHA-256 hex digest from a serialized Message."""

    def __init__(self, serializer: Serializer):
        self._serializer = serializer

    @override
    def hash_message(self, message: Message) -> str:
        message.data.seek(0)
        serialized = self._serializer.serialize(message)

        serialized.seek(0)
        digest = hashlib.sha256(serialized.read()).hexdigest()
        return digest