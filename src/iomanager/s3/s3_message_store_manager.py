from contextlib import AsyncExitStack
from typing import TYPE_CHECKING, cast

import aioboto3
from botocore.exceptions import ClientError
from types_aiobotocore_s3 import S3Client

from iomanager.s3.s3_message_store import S3MessageStore
from iomanager.store.message_store_manager import (
    MessageStoreManager,
    MessageStoreNotFoundError,
)

if TYPE_CHECKING:
    from types_aiobotocore_s3.literals import BucketLocationConstraintType
    from types_aiobotocore_s3.type_defs import CreateBucketRequestTypeDef


class S3MessageStoreManager(MessageStoreManager):
    def __init__(
        self,
        endpoint_url: str | None = None,
        access_key: str | None = None,
        secret_key: str | None = None,
        region_name: str | None = None,
        check_bucket_exists: bool = True,
        create_new_buckets: bool = False,
    ) -> None:
        self._endpoint_url = endpoint_url
        self._access_key = access_key
        self._secret_key = secret_key
        self._region_name = region_name
        self._check_bucket_exists = check_bucket_exists
        self._create_new_buckets = create_new_buckets
        self._session = aioboto3.Session()
        self._exit_stack = AsyncExitStack()
        self._client: S3Client | None = None

    async def connect(self) -> None:
        if self._client is None:
            self._client = await self._exit_stack.enter_async_context(
                self._session.client(
                    "s3",
                    endpoint_url=self._endpoint_url,
                    aws_access_key_id=self._access_key,
                    aws_secret_access_key=self._secret_key,
                    region_name=self._region_name,
                )
            )

    async def close(self) -> None:
        await self._exit_stack.aclose()
        self._client = None

    def _require_client(self) -> S3Client:
        if self._client is None:
            raise RuntimeError(
                "S3MessageStoreManager is not connected. Call connect() first."
            )
        return self._client

    async def get_message_store(self, name: str) -> S3MessageStore:
        client = self._require_client()

        if self._check_bucket_exists or self._create_new_buckets:
            exists = await self._bucket_exists(name)
            if not exists:
                if self._create_new_buckets:
                    await self._create_bucket(name)
                elif self._check_bucket_exists:
                    raise MessageStoreNotFoundError(
                        f"S3 bucket '{name}' does not exist."
                    )

        return S3MessageStore(client=client, bucket_name=name)

    async def _bucket_exists(self, bucket_name: str) -> bool:
        client = self._require_client()
        try:
            await client.head_bucket(Bucket=bucket_name)
            return True
        except ClientError as exc:
            error_code = exc.response.get("Error", {}).get("Code", "")
            if error_code in ("404", "NoSuchBucket", "NotFound"):
                return False
            raise

    async def _create_bucket(self, bucket_name: str) -> None:
        client = self._require_client()
        region = self._region_name or self._session.region_name
        create_kwargs: CreateBucketRequestTypeDef = {"Bucket": bucket_name}

        if region and region != "us-east-1":
            create_kwargs["CreateBucketConfiguration"] = {
                "LocationConstraint": cast("BucketLocationConstraintType", region)
            }

        await client.create_bucket(**create_kwargs)
