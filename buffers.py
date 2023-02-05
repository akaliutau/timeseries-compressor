from collections import deque
from datetime import datetime, date
from typing import Callable, Dict, Tuple, List

from cache import StringCache
from utils import date_to_days_epoch, datetime_to_microseconds_epoch, float_to_int, delta_float, delta_int, delta_str, \
    delta_array, float64_est, int32_est, int16_est, int64_est, string_est, array_est


class Field:

    def __init__(self, value: any, stored_value: any = None, value_type: str = None):
        self.value = value  # original value
        if value_type:
            self.value, self.stored, self.value_type = value, stored_value, value_type
        else:
            self.stored, self.value_type = Field._infer_type(value)

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
        return f'({self.value_type},{self.stored})'


class Record:

    def __init__(self, rec_id: int, matching_column: str = '', first_ref: int = 0):
        self.rec_id = rec_id  # absolute index of record in sequence - transient field
        self.matching_column = matching_column  # the name of key field in record - transient field
        self.first_ref = first_ref
        self.second_ref = 0

        self.columns: Dict[str, Field] = dict()
        self.schema_hash = 0

    def get_matching_column_value(self) -> str:
        return self.columns[self.matching_column].value  # always use non-transformed value for matching column

    def from_dict(self, rec: dict):
        schema = list()
        for col_name, value in rec.items():
            field = Field(value)
            schema.append(field.value_type)
            self.columns[col_name] = field
#        print(schema)
        self.schema_hash = hash(tuple(schema))

    def __repr__(self):
        return f'{self.rec_id}:[{self.first_ref},{self.second_ref}] {self.columns}'


delta_operators = {
    'float64': delta_float,
    'int32': delta_int,
    'date': delta_int,
    'timestamp': delta_int,
    'string': delta_str,
    'array': delta_array
}

size_operators = {
    'float64': float64_est,
    'int32': int32_est,
    'date': int16_est,
    'timestamp': int64_est,
    'string': string_est,
    'array': array_est
}


def size_bits(r: Record, verbose: bool = False) -> int:
    total = 0
    for field in r.columns.values():
        sz = size_operators[field.value_type](field.stored)
        total += sz
        if verbose:
            print(f'{field} -> {sz} bit')
    return total


def delta(our: Record, their: Record, iteration: int = 0) -> Record:
    if not their or our.schema_hash != their.schema_hash:
        return our
    delta_record = Record(our.rec_id, our.matching_column, our.first_ref)
    if iteration == 0:
        delta_record.first_ref = their.rec_id - our.rec_id
    else:
        delta_record.second_ref = their.rec_id - our.rec_id

    for col_name, this_field in our.columns.items():
        other_value = their.columns[col_name]
        col_type = this_field.value_type
        delta_value = delta_operators[col_type](this_field.stored, other_value.stored)
        delta_record.columns[col_name] = Field(this_field.value, delta_value, col_type)
    return delta_record


DEFAULT_SEARCH_DEPTH = 50


class RecordBuffer:

    def __init__(self, sink: Callable, string_cache: StringCache, iteration: int = 0, max_size: int = 1000):
        self.buffer: deque[Record] = deque()
        self.sink = sink
        self.max_size = max_size
        self.iteration = iteration
        # From Python 3.6 onwards, the standard dict type maintains insertion order by default.
        self.history: Dict[str, Dict[int, Record]] = dict()
        self.string_cache = string_cache

    def _memo(self, rec: Record) -> None:
        matching_column = rec.get_matching_column_value()
        if matching_column not in self.history:
            self.history[matching_column] = dict()
        self.history[matching_column][rec.rec_id] = rec

    def _forget(self, rec: Record) -> None:
        matching_column = rec.get_matching_column_value()
        if matching_column in self.history:
            if rec.rec_id in self.history[matching_column]:
                del self.history[matching_column][rec.rec_id]

    def index_string_values(self, rec: Record) -> None:
        for field in rec.columns.values():
            if field.value_type == 'string':
                field.stored = self.string_cache.add(field.value)

    def get_similar(self, matching_column: str, depth: int = DEFAULT_SEARCH_DEPTH) -> List[Record]:
        if matching_column in self.history:
            cached = list(self.history[matching_column].values())
            if self.iteration > 0:
                cached = [r for r in cached if r.first_ref != 0]  # interested only in deltas
            excess = len(cached) - depth
            if excess < 0:
                return cached
            return cached[excess:]  # return the latest SEARCH_DEPTH of all records only
        return list()

    def find_closest_to(self, cur_record: Record) -> Record:
        best_score = size_bits(cur_record)  # the smaller the better
        found = None
        similar = self.get_similar(matching_column=cur_record.get_matching_column_value())
        similar.reverse()
        for other in similar:
            if other.rec_id == cur_record.rec_id:
                continue
            dr = delta(our=cur_record, their=other, iteration=self.iteration)
            score = size_bits(dr)
            if score < best_score: # TODO leave as is if cur_record is better than any delta
                found = dr
                best_score = score

        return found or cur_record

    def add(self, rec: Record) -> None:
        delta_rec = self.find_closest_to(rec) # can be either delta (if similar was found) or the same record
        self.buffer.append(delta_rec)
        self._memo(rec)
        if len(self.buffer) > self.max_size:
            out_record = self.buffer.popleft()
            self._forget(out_record)
            self.sink(out_record)

    def __repr__(self):
        return f'hist={self.history.keys()}, records: {len(self.buffer)}'
