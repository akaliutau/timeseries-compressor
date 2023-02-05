from typing import Dict, Set

from serializable import Serializable


class StringCache(Serializable):
    """Implements string cache storage

    """

    def __init__(self):
        self.dictionary: Dict[int, str] = dict()
        self.reverse_dictionary: Dict[str, int] = dict()
        self.index = 0

    def add(self, value: str) -> int:
        if value in self.reverse_dictionary:
            return self.reverse_dictionary[value]
        self.dictionary[self.index] = value
        self.reverse_dictionary[value] = self.index
        self.index += 1
        return self.index - 1

    def get(self, index: int) -> str:
        return self.dictionary.get(index)

    def to_bytes(self):
        pass
