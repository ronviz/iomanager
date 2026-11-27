# IOManager

`IOManager` is an asynchronous framework designed to manage high-throughput message processing pipelines. It provides a robust, transaction-aware abstraction for consuming, transforming, and publishing data.

## Core Concepts

The system follows a classic **producer-consumer-distributor** model, ensuring data consistency throughout the lifecycle of a message.

### Pipeline Architecture

1.  **Ingestion (Input Phase):** Raw data is consumed from an input source, then "unpacked" (de-batched) into individual `Message` objects by a `Bulker`. Each message is associated with a `Transaction`.
2.  **Transformation (Processing Phase):** A `MessageProcessor` transforms each `Message` into one or more `ProcessingResult` objects. This is handled in an asynchronous context to maintain performance.
3.  **Publication (Output Phase):** Processed results are buffered and periodically "re-packed" (batched) by a `Bulker` before being published to their designated destinations via an `OutputManager`.

### Reliability and Transactions

The framework enforces data integrity through the `Transaction` interface. Throughout the pipeline, the original transaction is propagated.
- **Rollback:** If any stage of the processing fails, the system triggers a `rollback`.
- **Commit:** Once a batch of messages is successfully published, the transaction is `committed`.

## Key Components

- **`Service`**: The orchestrator that coordinates the fetcher, processor, and sender tasks.
- **`MessageProcessor`**: An interface for implementing custom business logic to transform incoming messages.
- **`Transaction`**: Ensures atomic operations across I/O boundaries.
- **`Bulker`**: Handles splitting (for input) and packing (for output) of data batches.
- **`InputManager` / `OutputManager`**: Manages the interfaces for reading from and writing to external systems.

# TODO
- subscribers:
  - Kafka
  - RabbitMQ
- publishers:
  - Kafka
  - RabbitMQ
  - HTTP
  - ElasticSearch
- stores:
  - s3
  - filesystem
  - redis
- lock:
  - redis
- transformers:
  - message store transformer
  - compressors:
    - gzip
    - lz4
    - adapting
- bulkers:
  - tar
  - adapting
- serializer:
  - json
- message inside transaction
- services:
  - Ingestor - gets message, returns list of message + destination
  - PipelineService - gets ingestor, subscriber factory, publisher factory
  - service runner (which will handle calling start and stop, interupts, timeouts, etc)
  - schematics (with pydantic):
    - `SchematicIngestor[BaseModel]`
    - `SchematicPipelineService[BaseModel]`
