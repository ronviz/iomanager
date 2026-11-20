from typing import Optional, override

from iomanager.counter.counter import Counter
from iomanager.io_manager import IOManager

try:
    import redis
    from redis.exceptions import RedisError

    _HAS_REDIS = True
except ImportError:
    _HAS_REDIS = False


class RedisCount(Counter, IOManager):
    """Distributed atomic counter backed by Redis."""

    def __init__(
        self,
        host: str = "localhost",
        port: int = 6379,
        db: int = 0,
        password: Optional[str] = None,
        socket_timeout: float = 5.0,
        key_prefix: str = "counter:",
        ttl: Optional[int] = None,
    ):
        """
        Initializes the RedisCount manager with Redis connection and key settings.

        :param host: Hostname or IP address of the Redis server.
        :param port: TCP port of the Redis server.
        :param db: Redis logical database index to connect to.
        :param password: Optional password for Redis authentication.
        :param socket_timeout: Timeout in seconds for Redis socket connection and read/write operations.
        :param key_prefix: Namespace prefix prepended to counter keys in Redis.
        :param ttl: Optional expiration time in seconds applied to keys when incremented.
        :raises ImportError: If the optional 'redis' dependency is not installed.
        :raises ValueError: If ttl is provided and is not greater than 0.
        """
        if not _HAS_REDIS:
            raise ImportError(
                "Redis support requires the 'redis' extra. Install it with: pip install 'iomanager[redis]'"
            )
        if ttl is not None and ttl <= 0:
            raise ValueError("ttl must be greater than 0.")

        self._host = host
        self._port = port
        self._db = db
        self._password = password
        self._socket_timeout = socket_timeout
        self._key_prefix = key_prefix
        self._ttl = ttl

        self._client: Optional[redis.Redis] = None
        self._is_connected = False

    def _format_key(self, key: str) -> str:
        return f"{self._key_prefix}{key}"

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
    def get_count(self, key: str) -> int:
        """Returns the current count for the given key (0 if the key does not exist)."""
        if not self._is_connected or self._client is None:
            raise RuntimeError("RedisCount is not connected. Call connect() first.")

        val = self._client.get(self._format_key(key))
        return int(val) if val is not None else 0

    @override
    def reset(self, key: str) -> None:
        """Deletes the counter key from Redis (resets count to 0)."""
        if not self._is_connected or self._client is None:
            raise RuntimeError("RedisCount is not connected. Call connect() first.")

        self._client.delete(self._format_key(key))

    @override
    def increment(self, key: str, amount: int = 1) -> int:
        """Atomically increments count by amount and returns the final count."""
        if not self._is_connected or self._client is None:
            raise RuntimeError("RedisCount is not connected. Call connect() first.")

        full_key = self._format_key(key)
        new_val = int(self._client.incrby(full_key, amount))

        if self._ttl is not None:
            self._client.expire(full_key, self._ttl)

        return new_val

    @override
    def close(self) -> None:
        """Closes the Redis client and connection pool."""
        if self._client is not None:
            self._client.close()
            self._client = None
        self._is_connected = False