from aiokafka import AIOKafkaProducer

from iomanager.kafka.kafka_message_publisher import KafkaMessagePublisher
from iomanager.output.output_manager import MessagePublisherNotFoundError, OutputManager


class KafkaOutputManager(OutputManager):
    def __init__(
        self,
        bootstrap_servers: str | list[str],
        check_topic_exists: bool = True,
    ) -> None:
        self._bootstrap_servers = bootstrap_servers
        self._check_topic_exists = check_topic_exists
        self._producer: AIOKafkaProducer | None = None

    async def connect(self) -> None:
        if self._producer is None:
            producer = AIOKafkaProducer(bootstrap_servers=self._bootstrap_servers)
            await producer.start()
            self._producer = producer

    async def close(self) -> None:
        if self._producer is not None:
            await self._producer.stop()
            self._producer = None

    def _require_producer(self) -> AIOKafkaProducer:
        if self._producer is None:
            raise RuntimeError(
                "KafkaOutputManager is not connected. Call connect() first."
            )
        return self._producer

    async def get_message_publisher(self, destination: str) -> KafkaMessagePublisher:
        producer = self._require_producer()

        if self._check_topic_exists:
            partitions = await producer.partitions_for(destination)
            if not partitions:
                raise MessagePublisherNotFoundError(
                    f"Kafka topic '{destination}' does not exist."
                )

        return KafkaMessagePublisher(producer=producer, topic=destination)
