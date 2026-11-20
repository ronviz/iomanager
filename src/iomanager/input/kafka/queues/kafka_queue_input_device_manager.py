import socket

from confluent_kafka import KafkaException, ShareConsumer
from confluent_kafka.admin import AdminClient

from iomanager.input.input_device_manager import (
    InputDeviceManager,
    InputDeviceNotFoundError,
)
from iomanager.input.kafka.queues.kafka_queue_input_device import KafkaQueueInputDevice


class KafkaQueueInputDeviceManager(InputDeviceManager):
    """Manages the Kafka connection and spawns Queue-based (Share Group) InputDevices."""

    DEFAULT_SHARE_GROUP_PREFIX = "kafka-queue-input-device"

    def __init__(
        self,
        host: str,
        port: int = 9092,
        metadata_timeout: float = 5.0,
        verify_device_exists: bool = True,
    ):
        self._bootstrap_servers = f"{host}:{port}"
        self._metadata_timeout = metadata_timeout
        self._verify_device_exists = verify_device_exists
        self._pod_hostname = socket.gethostname()
        self._admin_client: AdminClient | None = None
        self._consumers: list[ShareConsumer] = []
        self._is_connected = False

    def connect(self) -> None:
        """Establishes connection to the Kafka cluster."""
        if self._verify_device_exists:
            self._admin_client = AdminClient(
                {
                    "bootstrap.servers": self._bootstrap_servers,
                    "client.id": f"{self._pod_hostname}-admin",
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

    def get_input_device(self, device_name: str) -> KafkaQueueInputDevice:
        """Optionally validates topic existence and returns a dedicated KafkaQueueInputDevice."""
        if not self._is_connected:
            raise RuntimeError(
                "KaQueueInputDeviceManager is not connected. Call connect() first."
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

        consumer_config = {
            "bootstrap.servers": self._bootstrap_servers,
            "share.group.id": f"{self.DEFAULT_SHARE_GROUP_PREFIX}-{device_name}",
            "client.id": f"{self._pod_hostname}-{device_name}",
            "enable.auto.commit": False,
        }

        consumer = ShareConsumer(consumer_config)
        self._consumers.append(consumer)
        return KafkaQueueInputDevice(consumer=consumer, topic=device_name)

    def close(self) -> None:
        """Closes all active consumers and cleans up connections."""
        for consumer in self._consumers:
            consumer.close()
        self._consumers.clear()
        self._admin_client = None
        self._is_connected = False