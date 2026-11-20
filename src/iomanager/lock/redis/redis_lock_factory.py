from typing import Optional, override

from iomanager.io_manager import IOManager
from iomanager.lock.lock_factory import LockFactory
from iomanager.lock.redis.redis_lock import RedisLock

try:
    import redis
    from redis.exceptions import RedisError

    _HAS_REDIS = True
except ImportError:
    _HAS_REDIS = False



class RedisLockFactory(LockFactory, IOManager):
    """Manages a shared Redis connection and creates RedisLock instances."""

    def __init__(
        self,
        host: str = "localhost",
        port: int = 6379,
        db: int = 0,
        password: Optional[str] = None,
        ttl: float = 30.0,
        block_timeout: Optional[float] = -1,
        sleep: float = 0.05,
        socket_timeout: float = 5.0,
        key_prefix: str = "lock:",
    ):
        """
        Initializes the RedisLockFactory with connection and default lock settings.

        :param host: Hostname or IP address of the Redis server.
        :param port: TCP port of the Redis server.
        :param db: Redis logical database index to connect to.
        :param password: Optional password for Redis authentication.
        :param ttl: Maximum lifetime of a lock in seconds before it automatically expires. Must be greater than 0.
        :param block_timeout: Maximum time in seconds to wait when acquiring a lock.
            Use -1 for non-blocking mode (raises LockedError immediately if held),
            None to block indefinitely until acquired, or a non-negative float for a timed wait.
        :param sleep: Polling interval in seconds between lock acquisition retries while blocking.
        :param socket_timeout: Timeout in seconds for Redis socket connection and read/write operations.
        :param key_prefix: Namespace prefix prepended to lock names when storing keys in Redis.
        :raises ImportError: If the optional 'redis' dependency is not installed.
        :raises ValueError: If ttl is not positive or block_timeout is invalid.
        """
        if not _HAS_REDIS:
            raise ImportError(
                "Redis support requires the 'redis' extra. Install it with: pip install 'iomanager[redis]'"
            )
        if ttl <= 0:
            raise ValueError("ttl must be greater than 0.")
        if block_timeout is not None and block_timeout != -1 and block_timeout < 0:
            raise ValueError("block_timeout must be -1 (non-blocking), None (infinite), or >= 0.")

        self._host = host
        self._port = port
        self._db = db
        self._password = password
        self._ttl = ttl
        self._block_timeout = block_timeout
        self._sleep = sleep
        self._socket_timeout = socket_timeout
        self._key_prefix = key_prefix

        self._client: Optional[redis.Redis] = None
        self._is_connected = False

    @override
    def connect(self) -> None:
        """Establishes and verifies the connection to Redis."""
        client = redis.Redis(
            host=self._host,
            port=self._port,
            db=self._db,
            password=self._password,
            socket_connect_timeout=self._socket_timeout,
            socket_timeout=self._socket_timeout,
        )
        try:
            client.ping()
        except RedisError as e:
            client.close()
            raise ConnectionError(
                f"Failed to connect to Redis at {self._host}:{self._port}"
            ) from e

        self._client = client
        self._is_connected = True

    @override
    def get_lock(self, name: str) -> RedisLock:
        """Creates a RedisLock configured with the factory's shared client and settings."""
        if not self._is_connected or self._client is None:
            raise RuntimeError("RedisLockFactory is not connected. Call connect() first.")

        is_blocking = self._block_timeout != -1
        redis_blocking_timeout = None if self._block_timeout == -1 else self._block_timeout

        native_lock = self._client.lock(
            name=f"{self._key_prefix}{name}",
            timeout=self._ttl,
            sleep=self._sleep,
            blocking=is_blocking,
            blocking_timeout=redis_blocking_timeout,
            thread_local=False,
        )
        return RedisLock(native_lock)

    @override
    def close(self) -> None:
        """Closes the shared Redis client and connection pool."""
        if self._client is not None:
            self._client.close()
            self._client = None
        self._is_connected = False