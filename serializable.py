from abc import ABC, abstractmethod


class Serializable(ABC):

    @abstractmethod
    def unsaved_to_bytes(self) -> bytes:
        pass

    @abstractmethod
    def append_from_bytes(self, data: bytes) -> None:
        pass
