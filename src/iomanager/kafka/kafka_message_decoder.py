from io import BytesIO
from typing import Any

from aiokafka.structs import ConsumerRecord

from iomanager.message import Message


class KafkaMessageDecoder:
    def decode(self, record: ConsumerRecord[Any, Any]) -> Message:
        raw_bytes = record.value if isinstance(record.value, bytes) else b""
        metadata = {
            key: val.decode("utf-8") if val is not None else ""
            for key, val in (record.headers or ())
        }
        return Message(data=BytesIO(raw_bytes), metadata=metadata)
