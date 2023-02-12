from typing import Dict, Set, List

from serializable import Serializable


class SchemaCache(Serializable):
    """Implements cached schema storage
    """

    def __init__(self):
        self.schemas: Dict[int, List[str]] = dict()  # map: hash => list(col_name:col_type)
        self.saved_records = 0

    @property
    def size(self) -> int:
        return len(self.schemas)

    def add(self, schema_hash: int, schema: List[str]) -> None:
        self.schemas[schema_hash] = schema

    def has_unsaved(self) -> bool:
        return self.saved_records != self.size

    def unsaved_to_bytes(self) -> bytes:
        all_records = list(self.schemas.values())
        all_records.reverse()
        to_save = list()
        for i in range(self.size - self.saved_records):
            to_save.append(','.join(all_records[i]))
        ret_bytes = bytes('|'.join([s for s in to_save]), encoding='UTF-8')
        self.saved_records = self.size
        return ret_bytes

    def append_from_bytes(self, data: bytes) -> None:
        restored = data.decode('utf-8')
        schema_records = restored.split('|')
        for schema in schema_records:
            self.schemas[hash(schema)] = schema.split(',')
        self.saved_records = self.size


class StringCache(Serializable):
    """Implements cached string storage
    """

    def __init__(self):
        self.dictionary: Dict[int, str] = dict()
        self.reverse_dictionary: Dict[str, int] = dict()
        self.index = 0  # points to the last empty cell
        self.saved_ptr = 0

    def add(self, value: str) -> int:
        if value in self.reverse_dictionary:
            return self.reverse_dictionary[value]
        self.dictionary[self.index] = value
        self.reverse_dictionary[value] = self.index
        self.index += 1
        return self.index - 1

    def get(self, index: int) -> str:
        """Restores the string value by its index in cache
        """
        return self.dictionary.get(index)

    def has_unsaved(self) -> bool:
        return self.saved_ptr != self.index

    def unsaved_to_bytes(self) -> bytes:
        ret_bytes = bytes(','.join([s for idx, s in self.dictionary.items() if idx >= self.saved_ptr - 1]),
                          encoding='UTF-8')
        self.saved_ptr = self.index
        return ret_bytes

    def append_from_bytes(self, data: bytes) -> None:
        restored = data.decode('utf-8')
        arr = restored.split(',')
        for val in arr:
            self.add(val)
        self.saved_ptr = self.index
