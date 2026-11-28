from __future__ import annotations

import logging

from aiokafka import AIOKafkaConsumer
from aiokafka.admin import AIOKafkaAdminClient
from aiokafka.errors import KafkaError, UnknownTopicOrPartitionError
from aiokafka.structs import TopicPartition

from iomanager.input.input_manager import InputManager, MessageConsumerNotFoundError
from iomanager.kafka.kafka_message_decoder import KafkaMessageDecoder
from iomanager.kafka.kafka_queue_consumer import KafkaQueueMessageConsumer
from iomanager.kafka.kafka_topic_consumer import KafkaTopicMessageConsumer

logger = logging.getLogger(__name__)


class KafkaInputManager(InputManager):
    def __init__(
        self,
        host: str,
        port: int,
        check_topic_exists: bool = True,
    ) -> None:
        self._bootstrap_servers = f"{host}:{port}"
        self._check_topic_exists = check_topic_exists
        self._decoder = KafkaMessageDecoder()
        self._admin: AIOKafkaAdminClient | None = None
        self._consumers: list[AIOKafkaConsumer] = []

    async def connect(self) -> None:
        if self._admin is None:
            admin = AIOKafkaAdminClient(bootstrap_servers=self._bootstrap_servers)
            await admin.start()
            self._admin = admin

    async def close(self) -> None:
        for consumer in self._consumers:
            await consumer.stop()
        self._consumers.clear()

        if self._admin is not None:
            await self._admin.close()
            self._admin = None

    def _require_admin(self) -> AIOKafkaAdminClient:
        if self._admin is None:
            raise RuntimeError(
                "KafkaInputManager is not connected. Call connect() first."
            )
        return self._admin

    @staticmethod
    def _build_queue_group_id(topic_name: str) -> str:
        return f"{topic_name}-queue-group"

    async def get_message_consumer(
        self, source: str
    ) -> KafkaQueueMessageConsumer | KafkaTopicMessageConsumer:
        admin = self._require_admin()

        if self._check_topic_exists:
            await self._ensure_topic_exists(admin, source)

        if await self._is_queue(admin, source):
            return await self._create_queue_consumer(source)
        return await self._create_topic_consumer(source)

    async def _ensure_topic_exists(
        self, admin: AIOKafkaAdminClient, topic_name: str
    ) -> None:
        try:
            metadata = await admin.describe_topics([topic_name])
        except UnknownTopicOrPartitionError as exc:
            raise MessageConsumerNotFoundError(
                f"Kafka destination '{topic_name}' does not exist."
            ) from exc

        if not metadata or metadata[0].get("error_code", 0) != 0:
            raise MessageConsumerNotFoundError(
                f"Kafka destination '{topic_name}' does not exist."
            )

    async def _is_queue(self, admin: AIOKafkaAdminClient, topic_name: str) -> bool:
        target_group = self._build_queue_group_id(topic_name)

        try:
            responses = await admin.describe_consumer_groups([target_group])
            for response in responses:
                for group_entry in getattr(response, "groups", ()):
                    error_code, _, state, *_ = group_entry
                    if error_code == 0 and state != "Dead":
                        return True

            all_groups = await admin.list_consumer_groups()
            for existing_group_id, protocol_type in all_groups:
                if protocol_type not in ("consumer", ""):
                    continue
                offsets = await admin.list_consumer_group_offsets(existing_group_id)
                if any(tp.topic == topic_name for tp in offsets):
                    return True
        except KafkaError:
            logger.warning(
                "Failed to inspect consumer groups for '%s'; defaulting to topic mode.",
                topic_name,
                exc_info=True,
            )

        return False

    async def _create_queue_consumer(
        self, topic_name: str
    ) -> KafkaQueueMessageConsumer:
        consumer = AIOKafkaConsumer(
            bootstrap_servers=self._bootstrap_servers,
            group_id=self._build_queue_group_id(topic_name),
            enable_auto_commit=False,
            auto_offset_reset="earliest",
        )
        await consumer.start()
        try:
            consumer.subscribe([topic_name])
        except Exception:
            await consumer.stop()
            raise

        self._consumers.append(consumer)
        return KafkaQueueMessageConsumer(consumer=consumer, decoder=self._decoder)

    async def _create_topic_consumer(
        self, topic_name: str
    ) -> KafkaTopicMessageConsumer:
        consumer = AIOKafkaConsumer(
            bootstrap_servers=self._bootstrap_servers,
            group_id=None,
            enable_auto_commit=False,
        )
        await consumer.start()
        try:
            partition_ids = consumer.partitions_for_topic(topic_name) or set()
            partitions = [TopicPartition(topic_name, p) for p in partition_ids]
            consumer.assign(partitions)
            if partitions:
                await consumer.seek_to_end(*partitions)
        except Exception:
            await consumer.stop()
            raise

        self._consumers.append(consumer)
        return KafkaTopicMessageConsumer(consumer=consumer, decoder=self._decoder)
