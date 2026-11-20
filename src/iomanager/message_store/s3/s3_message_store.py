from io import BytesIO
import logging
from typing import TYPE_CHECKING, override
from botocore.exceptions import ClientError

from iomanager.hasher.hasher import Hasher
from iomanager.message import Message
from iomanager.message_store.message_store import (
    MessageAlreadyExistsError,
    MessageNotFoundError,
    MessageStore,
)
from iomanager.transaction.noop_transaction import NoopTransaction

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client


class S3MessageStore(MessageStore):
    """Stores and retrieves Message payloads (and optional metadata) in an S3 bucket."""

    def __init__(
        self,
        client: "S3Client",
        bucket_name: str,
        hasher: Hasher,
        save_metadata: bool = True,
    ):
        self._client = client
        self._bucket_name = bucket_name
        self._hasher = hasher
        self._save_metadata = save_metadata

    def _compute_key(self, message: Message) -> str:
        message.data.seek(0)
        key = self._hasher.hash_message(message)
        message.data.seek(0)
        return key

    def _object_exists(self, key: str) -> bool:
        try:
            self._client.head_object(Bucket=self._bucket_name, Key=key)
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code")
            if error_code in ("404", "NoSuchKey", "NotFound"):
                return False
        return True

    @override
    def get_message(self, key: str) -> Message:
        """Fetches a message from S3 by key."""
        try:
            response = self._client.get_object(Bucket=self._bucket_name, Key=key)
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code")
            if error_code in ("404", "NoSuchKey", "NotFound"):
                raise MessageNotFoundError(
                    f"Message with key '{key}' not found in S3 bucket '{self._bucket_name}'."
                ) from e
            raise

        body_bytes = response["Body"].read()
        data_stream = BytesIO(body_bytes)

        metadata: dict[str, str] = {}
        if self._save_metadata and "Metadata" in response:
            metadata = dict(response["Metadata"])

        return Message(
            data=data_stream,
            metadata=metadata,
            transaction=NoopTransaction(),
        )

    @override
    def put_message(self, message: Message) -> str:
        """Hashes the message to generate its key and uploads it to S3."""
        key = self._compute_key(message)

        if self._object_exists(key):
            raise MessageAlreadyExistsError(
                f"Message with key '{key}' already exists in S3 bucket '{self._bucket_name}'."
            )

        message.data.seek(0)
        payload = message.data.read()
        message.data.seek(0)

        put_kwargs: dict = {
            "Bucket": self._bucket_name,
            "Key": key,
            "Body": payload,
        }

        if self._save_metadata and message.metadata:
            put_kwargs["Metadata"] = {
                str(k): str(v) for k, v in message.metadata.items()
            }

        self._client.put_object(**put_kwargs)
        return key

    @override
    def delete_message(self, key: str) -> None:
        """Deletes a message from S3 by key."""
        if not self._object_exists(key):
            logging.warning(
                f"Message with key '{key}' not found in S3 bucket '{self._bucket_name}'; skipping delete."
            )
            return

        self._client.delete_object(Bucket=self._bucket_name, Key=key)
