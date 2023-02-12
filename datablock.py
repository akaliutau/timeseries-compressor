from typing import List

from cache import StringCache, SchemaCache
from record import Record
from writer import BlockWriter

RECORD_MAX_BLOCK_SIZE = 100


class Sinkable:

    def __init__(self, block_writer: BlockWriter, string_cache: StringCache, schema_cache: SchemaCache):
        self.block_writer = block_writer
        self.string_cache = string_cache
        self.schema_cache = schema_cache
        self.block: List[Record] = list()

    def _dump(self):
        unsaved_cache = self.string_cache.unsaved_to_bytes()
        self.block_writer.save_string_cache(unsaved_cache)
        unsaved_schema = self.schema_cache.unsaved_to_bytes()
        self.block_writer.save_schema(unsaved_schema)
        for r in self.block:
            self.block_writer.save_record(r)

    def consume(self, r: Record):
        self.block.append(r)
        if len(self.block) > RECORD_MAX_BLOCK_SIZE:
            self._dump()
            self.block.clear()

    def close(self):
        self._dump()
