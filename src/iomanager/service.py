import asyncio
import logging
import signal
from collections import defaultdict
from typing import Any

from iomanager.bulker.bulker import Bulker
from iomanager.input.input_manager import InputManager
from iomanager.message import Message
from iomanager.message_processor import MessageProcessor, ProcessingResult
from iomanager.output.output_manager import OutputManager
from iomanager.transaction import Transaction

logger = logging.getLogger(__name__)


class Service:
    def __init__(
        self,
        input_manager: InputManager,
        source: str,
        output_manager: OutputManager,
        message_processor: MessageProcessor,
        bulker: Bulker,
        batch_size: int,
        batch_timeout: int,
    ):
        self._input_manager = input_manager
        self._source = source
        self._output_manager = output_manager
        self._message_processor = message_processor
        self._bulker = bulker
        self._batch_size = batch_size
        self._batch_timeout = batch_timeout

        self._input_queue: asyncio.Queue[Any] = asyncio.Queue(maxsize=100)
        self._output_queue: asyncio.Queue[Any] = asyncio.Queue(maxsize=batch_size * 2)

        self._is_running = False

    async def send_batch(
        self, batch: list[tuple[list[ProcessingResult], Transaction]]
    ) -> None:
        destination_to_messages: dict[str, list[Message]] = defaultdict(list)
        transactions: list[Transaction] = []
        for result_list, transaction in batch:
            for result in result_list:
                destination_to_messages[result.destination].append(result.message)
                transactions.append(transaction)

        try:
            for destination, messages in destination_to_messages.items():
                message_block = self._bulker.pack(messages)
                message_publisher = await self._output_manager.get_message_publisher(
                    destination
                )
                await message_publisher.send_message(message_block)
                for transaction in transactions:
                    await transaction.commit()
        except Exception:
            logger.exception("Could not send batch")
            for transaction in transactions:
                await transaction.rollback()

    async def fetcher(self) -> None:
        message_consumer = await self._input_manager.get_message_consumer(self._source)
        while self._is_running:
            message, transaction = await message_consumer.get_message()
            items = await asyncio.to_thread(self._bulker.split, message)
            for item in items:
                await self._input_queue.put((item, transaction))

    async def processor(self) -> None:
        while self._is_running:
            message, transaction = await self._input_queue.get()
            try:
                result = await asyncio.to_thread(
                    self._message_processor.process_message, message
                )
                await self._output_queue.put((result, transaction))
            except Exception:
                await transaction.rollback()
                logger.exception("Could not process message")
            finally:
                self._input_queue.task_done()

    async def sender(self) -> None:
        loop = asyncio.get_running_loop()
        while self._is_running:
            batch = [await self._output_queue.get()]
            try:
                deadline = loop.time() + self._batch_timeout
                while len(batch) < self._batch_size:
                    remaining_time = deadline - loop.time()
                    if remaining_time <= 0:
                        break
                    try:
                        async with asyncio.timeout(remaining_time):
                            batch.append(await self._output_queue.get())
                    except TimeoutError:
                        break

                await self.send_batch(batch)
            finally:
                for _ in batch:
                    self._output_queue.task_done()

    async def run(self) -> None:
        loop = asyncio.get_running_loop()
        tasks: list[asyncio.Task[Any]] = []

        def _request_shutdown() -> None:
            self._is_running = False
            for task in tasks:
                task.cancel()

        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, _request_shutdown)

        try:
            async with asyncio.TaskGroup() as tg:
                tasks.append(tg.create_task(self.fetcher()))
                tasks.append(tg.create_task(self.processor()))
                tasks.append(tg.create_task(self.sender()))
        except* asyncio.CancelledError:
            pass

    async def start(self) -> None:
        try:
            await self._input_manager.connect()
        except Exception:
            logger.exception("Could not connect to input manager")
            raise

        try:
            await self._output_manager.connect()
        except Exception:
            logger.exception("Could not connect to output manager")
            raise

        self._is_running = True
        try:
            await self.run()
        finally:
            await self.stop()

    async def stop(self) -> None:
        self._is_running = False
        try:
            await self._input_manager.close()
        except Exception:
            logger.exception("Could not close connection to input manager")
            raise

        try:
            await self._output_manager.close()
        except Exception:
            logger.exception("Could not close connection to output manager")
            raise
