import logging
from typing import TYPE_CHECKING, override

from iomanager.lock.lock import Lock, LockedError

try:
    from redis.exceptions import LockError

    _HAS_REDIS = True
except ImportError:
    _HAS_REDIS = False

if TYPE_CHECKING:
    from redis.exceptions import LockError
    from redis.lock import Lock as NativeRedisLock



class RedisLock(Lock):
    """Lightweight wrapper around redis-py's built-in distributed Lock."""

    def __init__(self, native_lock: "NativeRedisLock"):
        if not _HAS_REDIS:
            raise ImportError(
                "Redis support requires the 'redis' extra. Install it with: pip install 'iomanager[redis]'"
            )
        self._lock = native_lock

    @override
    def lock(self) -> None:
        """Acquires the lock or raises LockedError if already held / timed out."""
        if not self._lock.acquire():
            raise LockedError(f"Lock '{self._lock.name}' is already locked.")

    @override
    def release(self) -> None:
        """Releases the lock, logging a warning if the lock was not owned or already expired."""
        try:
            self._lock.release()
        except LockError as e:
            logging.warning(f"Failed to release lock '{self._lock.name}': {e}")