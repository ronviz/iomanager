from dataclasses import dataclass
from io import BytesIO


@dataclass
class Message:
    data: BytesIO
    metadata: dict[str, str]
