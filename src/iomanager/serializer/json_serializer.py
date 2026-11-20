import base64
import json
from io import BytesIO
from typing import override

from iomanager.message import Message
from iomanager.serializer.serializer import (
    DeserializationError,
    SerializationError,
    Serializer,
)
from iomanager.transaction.noop_transaction import NoopTransaction

ENCODING = "utf-8"
DATA_KEY = "data"
METADATA_KEY = "metadata"


class JsonSerializer(Serializer):
    """Serializes a Message (data + metadata) into canonical UTF-8 JSON bytes."""

    @override
    def serialize(self, message: Message) -> BytesIO:
        try:
            message.data.seek(0)
            raw_payload = message.data.read()

            envelope = {
                "metadata": {str(k): str(v) for k, v in message.metadata.items()},
                "data_b64": base64.b64encode(raw_payload).decode("ascii"),
            }

            serialized_bytes = json.dumps(
                envelope,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
            ).encode("utf-8")

            return BytesIO(serialized_bytes)
        except Exception as e:
            raise SerializationError("Message is not JSON serializeable") from e

    @override
    def deserialize(self, data: BytesIO) -> Message:
        try:
            data.seek(0)
            envelope = json.loads(data.read().decode("utf-8"))

            raw_payload = base64.b64decode(envelope["data_b64"])
            metadata = {str(k): str(v) for k, v in envelope.get("metadata", {}).items()}

            return Message(
                data=BytesIO(raw_payload),
                metadata=metadata,
                transaction=NoopTransaction(),
            )
        except Exception as e:
            raise DeserializationError("Could not desrialize json") from e
