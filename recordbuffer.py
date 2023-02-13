from collections import deque
from typing import Dict, List

from cache import StringCache, SchemaCache
from datablock import Sinkable
from record import Record, Field
from utils import delta_float, delta_int, delta_str, \
    delta_array, float64_est, int32_est, int16_est, int64_est, string_est, array_est

delta_operators = {
    'float64': delta_float,
    'int32': delta_int,
    'date': delta_int,
    'timestamp': delta_int,
    'string': delta_str,
    'array': delta_array,
    'nullable': delta_str
}

size_operators = {
    'float64': float64_est,
    'int32': int32_est,
    'date': int16_est,
    'timestamp': int64_est,
    'string': string_est,
    'array': array_est,
    'nullable': string_est
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
    delta_record = Record(our.rec_id, our.linking_column, our.first_ref)
    if iteration == 0:
        delta_record.first_ref = their.rec_id - our.rec_id
    else:
        delta_record.second_ref = their.rec_id - our.rec_id

    for col_name, this_field in our.columns.items():
        other_value = their.columns[col_name]
        col_type = this_field.value_type
        delta_value = delta_operators[col_type](this_field.stored, other_value.stored)
        delta_record.columns[col_name] = Field(this_field.value, delta_value, col_type, this_field.linking)
    return delta_record


DEFAULT_SEARCH_DEPTH = 50


class RecordBuffer(Sinkable):

    def __init__(self,
                 sink: Sinkable,
                 string_cache: StringCache,
                 schema_cache: SchemaCache,
                 iteration: int = 0,
                 max_size: int = 1000):
        self.buffer: deque[Record] = deque()
        self.sink = sink
        self.max_size = max_size
        self.iteration = iteration
        # From Python 3.6 onwards, the standard dict type maintains insertion order by default.
        self.history: Dict[str, Dict[int, Record]] = dict()
        self.string_cache = string_cache
        self.schema_cache = schema_cache

    def _memo(self, rec: Record) -> None:
        linking_column = rec.get_linking_column_value()
        if linking_column not in self.history:
            self.history[linking_column] = dict()
        self.history[linking_column][rec.rec_id] = rec

    def _forget(self, rec: Record) -> None:
        linking_column = rec.get_linking_column_value()
        if linking_column in self.history:
            if rec.rec_id in self.history[linking_column]:
                del self.history[linking_column][rec.rec_id]

    def index_string_values(self, rec: Record) -> None:
        """replaces string values in record with index in string cache
        """
        for field in rec.columns.values():
            if field.value_type == 'string':
                field.stored = self.string_cache.add(field.value)

    def get_similar(self, linking_column: str, depth: int = DEFAULT_SEARCH_DEPTH) -> List[Record]:
        if linking_column in self.history:
            history = list(self.history[linking_column].values())
            if self.iteration > 0:
                history = [r for r in history if r.first_ref != 0]  # interested only in deltas
            more_than_needed = len(history) - depth
            if more_than_needed < 0:
                return history
            return history[more_than_needed:]  # return the tail of the list of found records
        return list()

    def find_closest_to(self, cur_record: Record) -> Record:
        best_score = size_bits(cur_record)  # the smaller, the better
        found = None
        similar = self.get_similar(linking_column=cur_record.get_linking_column_value())
        similar.reverse()  # start from the closest by offset
        for other in similar:
            if other.rec_id == cur_record.rec_id:
                continue
            dr = delta(our=cur_record, their=other, iteration=self.iteration)
            score = size_bits(dr)
            if score < best_score:
                found = dr
                best_score = score

        if not found and self.iteration == 1:
            sch_hash, sch = cur_record.get_schema()
            self.schema_cache.add(sch_hash, sch) # TODO faster: check hash first

        return found or cur_record

    def add(self, rec: Record) -> None:
        delta_rec = self.find_closest_to(rec)  # can be either delta (if similar was found) or the same record
        self.buffer.append(delta_rec)
        self._memo(rec)
        if len(self.buffer) > self.max_size:
            out_record = self.buffer.popleft()
            self._forget(out_record)
            self.sink.add(out_record)

    def close(self):
        pass

    def __repr__(self):
        return f'hist={self.history.keys()}, records: {len(self.buffer)}'
