import socket
from typing import Optional
from confluent_kafka import Consumer, KafkaException
from confluent_kafka.admin import AdminClient

from iomanager.input.input_device_manager import (
    InputDeviceManager,
    InputDeviceNotFoundError,
)
from iomanager.input.kafka.topic.kafka_topic_input_device import KafkaTopicInputDevice


class KafkaTopicInputDeviceManager(InputDeviceManager):
    """Manages the Kafka connection, spawns topic InputDevices, and owns Consumer lifecycles."""

    DEFAULT_GROUP_PREFIX = "kafka-topic-input-device"

    def __init__(
        self,
        host: str,
        port: int = 9092,
        group_id: Optional[str] = None,
        poll_timeout: float = 1.0,
        metadata_timeout: float = 5.0,
        verify_device_exists: bool = True,
    ):
        self._bootstrap_servers = f"{host}:{port}"
        self._group_id = group_id
        self._poll_timeout = poll_timeout
        self._metadata_timeout = metadata_timeout
        self._verify_device_exists = verify_device_exists
        self._pod_hostname = socket.gethostname()
        self._admin_client: Optional[AdminClient] = None
        self._consumers: list[Consumer] = []
        self._is_connected = False

    def connect(self) -> None:
        """Establishes connection to the Kafka cluster."""
        if self._verify_device_exists:
            self._admin_client = AdminClient(
                {
                    "bootstrap.servers": self._bootstrap_servers,
                    "client.id": f"{self._pod_hostname}-input-admin",
                }
            )
            try:
                self._admin_client.list_topics(timeout=self._metadata_timeout)
            except KafkaException as e:
                self._is_connected = False
                raise ConnectionError(
                    f"Failed to connect to Kafka at {self._bootstrap_servers}"
                ) from e

        self._is_connected = True

    def get_input_device(self, device_name: str) -> KafkaTopicInputDevice:
        """Optionally validates topic existence and returns a dedicated KafkaTopicInputDevice."""
        if not self._is_connected:
            raise RuntimeError(
                "KafkaTopicInputDeviceManager is not connected. Call connect() first."
            )

        if self._verify_device_exists and self._admin_client:
            try:
                cluster_metadata = self._admin_client.list_topics(
                    timeout=self._metadata_timeout
                )
            except KafkaException as e:
                raise ConnectionError(
                    f"Failed to verify topic '{device_name}' on Kafka."
                ) from e

            if device_name not in cluster_metadata.topics:
                raise InputDeviceNotFoundError(
                    f"Kafka topic '{device_name}' does not exist."
                )

        effective_group_id = (
            self._group_id or f"{self.DEFAULT_GROUP_PREFIX}-{device_name}"
        )

        consumer_config = {
            "bootstrap.servers": self._bootstrap_servers,
            "group.id": effective_group_id,
            "client.id": f"{self._pod_hostname}-{device_name}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }

        consumer = Consumer(consumer_config)
        self._consumers.append(consumer)
        return KafkaTopicInputDevice(
            consumer=consumer,
            topic=device_name,
            poll_timeout=self._poll_timeout,
        )

    def close(self) -> None:
        """Closes all active consumers and cleans up connections."""
        for consumer in self._consumers:
            consumer.close()
        self._consumers.clear()
        self._admin_client = None
        self._is_connected = False
