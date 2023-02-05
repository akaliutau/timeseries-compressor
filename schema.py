import pickle
from typing import Dict

from serializable import Serializable


class Schema(Serializable):
    types_code_mapping = {
        'float64': 1,
        'int32': 2,
        'date': 3,
        'timestamp': 4,
        'string': 5,
        'array': 6
    }

    def __init__(self, hashcode: int):
        self.hashcode = hashcode
        self.columns: Dict[str, int] = dict()

    def add_column(self, col_name: str, col_type: str) -> None:
        self.columns[col_name] = Schema.types_code_mapping[col_type]

    def to_bytes(self) -> bytes:
        return bytes(','.join(name + ':' + str(code) for name, code in self.columns.items()) + '\n', encoding='utf8')


def to_bytes(schema: Schema) -> bytes:
    return pickle.dumps(schema)


def from_bytes(bytes_array: bytes) -> Schema:
    return pickle.loads(bytes_array)
