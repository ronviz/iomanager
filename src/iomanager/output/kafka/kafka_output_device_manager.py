import socket

try:
    from confluent_kafka import KafkaException, Producer
    from confluent_kafka.admin import AdminClient

    _HAS_KAFKA = True
except ImportError:
    _HAS_KAFKA = False

from iomanager.output.kafka.kafka_output_device import (
    KafkaOutputDevice,
)
from iomanager.output.output_device_manager import (
    OutputDeviceManager,
    OutputDeviceNotFoundError,
)


class KafkaOutputDeviceManager(OutputDeviceManager):
    """Manages the Kafka Producer connection and spawns topic-specific OutputDevices."""

    def __init__(
        self,
        host: str,
        port: int = 9092,
        flush_timeout: float = 10.0,
        metadata_timeout: float = 5.0,
        verify_device_exists: bool = True,
    ):
        if not _HAS_KAFKA:
            raise ImportError(
                "Kafka support requires the 'kafka' extra. Install it with: pip install 'iomanager[kafka]'"
            )
        self._bootstrap_servers = f"{host}:{port}"
        self._flush_timeout = flush_timeout
        self._metadata_timeout = metadata_timeout
        self._verify_device_exists = verify_device_exists
        self._pod_hostname = socket.gethostname()
        self._admin_client: AdminClient | None = None
        self._producer: Producer | None = None
        self._is_connected = False

    def connect(self) -> None:
        """Establishes connection to the Kafka cluster and initializes the Producer."""
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

        self._producer = Producer(
            {
                "bootstrap.servers": self._bootstrap_servers,
                "client.id": f"{self._pod_hostname}-producer",
                "acks": "all",
            }
        )
        self._is_connected = True

    def get_output_device(self, device_name: str) -> KafkaOutputDevice:
        """Optionally validates topic existence and returns a KafkaOutputDevice."""
        if not self._is_connected or not self._producer:
            raise RuntimeError(
                "KafkaOutputDeviceManager is not connected. Call connect() first."
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
                raise OutputDeviceNotFoundError(
                    f"Kafka topic '{device_name}' does not exist."
                )

        return KafkaOutputDevice(
            producer=self._producer,
            topic=device_name,
            flush_timeout=self._flush_timeout,
        )

    def close(self) -> None:
        """Flushes any remaining buffered messages and closes the shared Producer."""
        if self._producer is not None:
            self._producer.flush(timeout=self._flush_timeout)
            self._producer = None
        self._admin_client = None
        self._is_connected = False
