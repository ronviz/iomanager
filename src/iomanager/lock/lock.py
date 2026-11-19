from abc import ABC, abstractmethod

from iomanager.exceptions.base_io_excpetion import BaseIOException


class LockedError(BaseIOException):
    """
    Raised when a lock is already locked
    """



class Lock(ABC):
    """
    Its a lock.
    can either be blocking on `lock()` or raise `LockedError`
    """

    @abstractmethod
    def lock(self) -> None:
        pass

    @abstractmethod
    def release(self) -> None:
        pass
