from typing import TYPE_CHECKING, Optional, override
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from iomanager.hasher.hasher import Hasher
from iomanager.io_manager import IOManager
from iomanager.message_store.message_store_manager import (
    MessageStoreManager,
    MessageStoreNotFoundError,
)
from iomanager.message_store.s3.s3_message_store import S3MessageStore

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client


class S3MessageStoreManager(MessageStoreManager):
    """Manages the S3 client connection and spawns bucket-scoped S3MessageStores."""

    def __init__(
        self,
        hasher: Hasher,
        region_name: Optional[str] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        save_metadata: bool = True,
        connect_timeout: float = 5.0,
        read_timeout: float = 10.0,
        verify_store_exists: bool = True,
    ):
        self._hasher = hasher
        self._region_name = region_name
        self._endpoint_url = endpoint_url
        self._aws_access_key_id = aws_access_key_id
        self._aws_secret_access_key = aws_secret_access_key
        self._save_metadata = save_metadata
        self._connect_timeout = connect_timeout
        self._read_timeout = read_timeout
        self._verify_store_exists = verify_store_exists
        self._client: Optional["S3Client"] = None
        self._is_connected = False

    @override
    def connect(self) -> None:
        """Initializes the S3 client and verifies AWS credentials."""
        if isinstance(self._hasher, IOManager):
            self._hasher.connect()

        boto_config = Config(
            connect_timeout=self._connect_timeout,
            read_timeout=self._read_timeout,
        )

        self._client = boto3.client(
            "s3",
            region_name=self._region_name,
            endpoint_url=self._endpoint_url,
            aws_access_key_id=self._aws_access_key_id,
            aws_secret_access_key=self._aws_secret_access_key,
            config=boto_config,
        )
        self._is_connected = True

    @override
    def get_message_store(self, message_store_name: str) -> S3MessageStore:
        """Optionally validates bucket existence and returns an S3MessageStore for that bucket."""
        if not self._is_connected or self._client is None:
            raise RuntimeError(
                "S3MessageStoreManager is not connected. Call connect() first."
            )

        if self._verify_store_exists:
            try:
                self._client.head_bucket(Bucket=message_store_name)
            except ClientError as e:
                error_code = e.response.get("Error", {}).get("Code")
                if error_code in ("404", "NoSuchBucket", "NotFound"):
                    raise MessageStoreNotFoundError(
                        f"S3 bucket '{message_store_name}' does not exist."
                    ) from e
                raise ConnectionError(
                    f"Failed to verify S3 bucket '{message_store_name}'."
                ) from e

        return S3MessageStore(
            client=self._client,
            bucket_name=message_store_name,
            hasher=self._hasher,
            save_metadata=self._save_metadata,
        )

    @override
    def close(self) -> None:
        """Closes the underlying boto3 S3 client and cleans up resources."""
        if self._client is not None:
            self._client.close()
            self._client = None
        if isinstance(self._hasher, IOManager):
            self._hasher.close()
        self._is_connected = False
