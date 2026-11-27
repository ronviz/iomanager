import asyncio
from typing import override

from iomanager.lock.lock import Lock, LockedError


class BlockingLock(Lock):
    def __init__(self, lock: Lock, interval: float = 0.1):
        self._lock = lock
        self._interval = interval

    @override
    async def lock(self) -> None:
        while True:
            try:
                await self._lock.lock()
                return
            except LockedError:
                await asyncio.sleep(self._interval)

    @override
    async def release(self) -> None:
        await self._lock.release()
