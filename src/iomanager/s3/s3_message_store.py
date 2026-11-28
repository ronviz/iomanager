from io import BytesIO
from typing import TYPE_CHECKING

from botocore.exceptions import ClientError

from iomanager.message import Message
from iomanager.store.message_store import MessageNotFoundError, MessageStore

if TYPE_CHECKING:
    from types_aiobotocore_s3 import S3Client


class S3MessageStore(MessageStore):
    def __init__(self, client: "S3Client", bucket_name: str) -> None:
        self._client = client
        self._bucket_name = bucket_name

    async def get_message(self, key: str) -> Message:
        try:
            response = await self._client.get_object(
                Bucket=self._bucket_name,
                Key=key,
            )
        except ClientError as exc:
            if self._is_not_found(exc):
                raise MessageNotFoundError(
                    f"Message '{key}' not found in bucket '{self._bucket_name}'."
                ) from exc
            raise

        async with response["Body"] as stream:
            raw_data = await stream.read()

        return Message(
            data=BytesIO(raw_data),
            metadata=response.get("Metadata", {}),
        )

    async def put_message(self, key: str, message: Message) -> None:
        message.data.seek(0)
        await self._client.put_object(
            Bucket=self._bucket_name,
            Key=key,
            Body=message.data.read(),
            Metadata=message.metadata,
        )

    async def delete_message(self, key: str) -> None:
        try:
            await self._client.head_object(
                Bucket=self._bucket_name,
                Key=key,
            )
        except ClientError as exc:
            if self._is_not_found(exc):
                raise MessageNotFoundError(
                    f"Message '{key}' not found in bucket '{self._bucket_name}'."
                ) from exc
            raise

        await self._client.delete_object(
            Bucket=self._bucket_name,
            Key=key,
        )

    @staticmethod
    def _is_not_found(exc: ClientError) -> bool:
        error_code = exc.response.get("Error", {}).get("Code", "")
        return error_code in ("404", "NoSuchKey", "NotFound")
