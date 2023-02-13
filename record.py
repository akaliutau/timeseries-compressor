from datetime import datetime, date
from typing import List, Tuple, Dict

from utils import datetime_to_microseconds_epoch, date_to_days_epoch, float_to_int


class Field:

    def __init__(self, value: any, stored_value: any = None, value_type: str = None, is_linking: bool = False):
        self.value = value  # original value
        if value_type:
            self.value, self.stored, self.value_type = value, stored_value, value_type
        else:
            self.stored, self.value_type = Field._infer_type(value)
        self.linking = is_linking

    @staticmethod
    def _infer_type(v: any) -> Tuple[any, str]:
        if isinstance(v, float):
            return float_to_int(v), 'float64'
        elif isinstance(v, int):
            return v, 'int32'
        elif isinstance(v, list):
            return v, 'array'
        else:
            try:
                dt = datetime.strptime(str(v), '%Y-%m-%d').date()
                if isinstance(dt, date):
                    return date_to_days_epoch(dt), 'date'
            except ValueError as e:
                pass
            try:
                dt = datetime.strptime(str(v), '%Y-%m-%dT%H-%M-%S.%fZ')
                if isinstance(dt, datetime):
                    return datetime_to_microseconds_epoch(dt), 'timestamp'
            except ValueError as e:
                pass
        return v, 'string'

    def __repr__(self):
        if self.value_type.startswith('float'):
            return f'({self.value_type},{hex(int(self.stored))})'
        if self.linking:
            return f'({self.value_type},{self.stored},{self.value})'
        return f'({self.value_type},{self.stored})'


class Record:

    def __init__(self, rec_id: int, linking_column: str = '', first_ref: int = 0):
        self.rec_id = rec_id  # absolute index of record in sequence - transient field
        self.linking_column = linking_column  # the name of key field in record - transient field
        self.first_ref = first_ref
        self.second_ref = 0

        self.columns: Dict[str, Field] = dict()
        self.schema_hash = 0

    @property
    def signature(self) -> int:
        return self.first_ref * 8 + self.second_ref

    def get_linking_column_value(self) -> str:
        return self.columns[self.linking_column].value  # always use non-transformed value for matching column (for 2x delta)

    def from_dict(self, rec: dict):
        """Creates a record with hard-typed fields from generic schemas,
        the order of fields is preserved
        """
        for col_name, value in rec.items():
            field = Field(value)
            self.columns[col_name] = field
        schema = ','.join([col_name + ':' + field.value_type for col_name, field in self.columns.items()])
        self.schema_hash = hash(schema)
        self.columns[self.linking_column].linking = True

    def from_vector(self, vector: List[any], schema: List[str]):
        for i in range(len(vector)):
            col = schema[i].split(':')
            col_name = col[0]
            col_type = col[1]
            self.columns[col_name] = Field(value=vector[i], stored_value=vector[i], value_type=col_type)

    def get_vector(self) -> List[any]:
        return [field.stored for field in self.columns.values()]

    def get_schema(self) -> Tuple[int, List[str]]:
        sch = [col_name + ':' + field.value_type for col_name, field in self.columns.items()]
        return self.schema_hash, sch

    def __repr__(self):
        return f'{self.rec_id}:[{self.first_ref},{self.second_ref}] {self.columns}'
