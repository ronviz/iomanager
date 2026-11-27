from abc import ABC, abstractmethod

from iomanager.exceptions.base_exception import IOManagerException
from iomanager.lock.lock import Lock


class LockNotFoundError(IOManagerException):
    pass


class LockFactory(ABC):
    @abstractmethod
    async def get_lock(self, name: str) -> Lock:
        pass
