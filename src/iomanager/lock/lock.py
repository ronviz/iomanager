from abc import ABC, abstractmethod

from iomanager.exceptions.base_exception import IOManagerException


class LockedError(IOManagerException):
    """
    Raised when a lock is already taken
    """


class Lock(ABC):
    @abstractmethod
    async def lock(self) -> None:
        """
        Tries to lock the lock.
        If already locked, raise `LockedError`
        """

    @abstractmethod
    async def release(self) -> None:
        pass
