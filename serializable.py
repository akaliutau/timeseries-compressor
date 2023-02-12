
class Serializable:

    def to_bytes(self) -> bytes:
        pass

    def from_bytes(self, data: bytes) -> None:
        pass
