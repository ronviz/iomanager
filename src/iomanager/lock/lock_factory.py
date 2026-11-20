from abc import ABC, abstractmethod

from iomanager.lock.lock import Lock


class LockFactory(ABC):
    @abstractmethod
    def get_lock(name: str) -> Lock:
        pass
