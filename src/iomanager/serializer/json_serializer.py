from io import BytesIO
import json
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
    @override
    def serialize(self, message: Message) -> BytesIO:
        message.data.seek(0)
        data = {
            DATA_KEY: message.data.read(),
            METADATA_KEY: message.metadata,
        }
        try:
            return BytesIO(json.dumps(data).encode(encoding=ENCODING))
        except Exception as e:
            raise SerializationError("Could not serialize to json") from e

    def deserialize(self, data: BytesIO) -> Message:
        data.seek(0)
        try:
            loaded_dict = json.loads(data.read().decode(ENCODING))
            return Message(
                data=loaded_dict[DATA_KEY],
                metadata=loaded_dict[METADATA_KEY],
                transaction=NoopTransaction(),
            )
        except Exception as e:
            raise DeserializationError("Could not deserialize json") from e
