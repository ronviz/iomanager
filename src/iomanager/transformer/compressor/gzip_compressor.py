import gzip
from io import BytesIO
from typing import override

from iomanager.message import Message
from iomanager.transformer.compressor.compression_error import (
    CompressionError,
    DecompressionError,
)
from iomanager.transformer.compressor.consts import (
    COMPRESSOR_TYPE_METADATA_KEY,
    GZIP_COMPRESSOR_TYPE,
)
from iomanager.transformer.transformer import Transformer


class GzipCompressor(Transformer):
    @override
    def transform_input(self, message: Message):
        buffer = BytesIO()
        message.data.seek(0)

        try:
            with gzip.GzipFile(fileobj=buffer, mode="wb") as gz:
                gz.write(message.data.read())
        except Exception as e:
            raise CompressionError("Could not gzip message") from e

        message.data = buffer
        message.metadata[COMPRESSOR_TYPE_METADATA_KEY] = GZIP_COMPRESSOR_TYPE

    @override
    def transform_output(self, message: Message):
        if message.metadata.get(COMPRESSOR_TYPE_METADATA_KEY) != GZIP_COMPRESSOR_TYPE:
            raise DecompressionError("Message is not gzip")
        try:
            message.data.seek(0)
            with gzip.GzipFile(fileobj=message.data, mode="rb") as gz:
                message.metadata.pop(COMPRESSOR_TYPE_METADATA_KEY)
                message.data = BytesIO(gz.read())
        except Exception as e:
            raise DecompressionError("Could not decompress gzip") from e
