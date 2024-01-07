import unittest

from datablock import DummySink
from record import Record
from recordbuffer import delta, size_bits, RecordBuffer
from cache import StringCache, SchemaCache
from utils import flatten


class TestingBuffers(unittest.TestCase):
    r = {
        "date": "2000-01-05",
        "timestamp": "2000-01-05T00-00-00.036258Z",
        "data_source": "free_tier",
        "data": {
            "open": 111.125, "high": 116.375, "low": 109.375, "close": 113.8125, "adjusted_close": 35.6219,
            "volume": 64047000, "symbol": "MSFT", "name": "Microsoft",
            "volume_array": [152, 71, 209, 3, 0, 0, 0, 0]
        }
    }

    def test_record_normalise(self):
        flat_record = flatten(TestingBuffers.r)
        print(flat_record)

    def test_record_creation(self):
        record = Record(1, linking_column='data.symbol')
        flat_record = flatten(TestingBuffers.r)
        record.from_dict(flat_record)
        print(record)

    def test_record_buffer(self):
        edict = StringCache()
        schema_cache = SchemaCache()
        sink = DummySink()
        buf = RecordBuffer(sink=sink, string_cache=edict, schema_cache=schema_cache, max_size=2)
        record_1 = Record(1, linking_column='data.symbol')
        flat_record = flatten(TestingBuffers.r)
        record_1.from_dict(flat_record)
        buf.add(record_1)
        print(buf)

        record_2 = Record(2, linking_column='data.symbol')
        flat_record = flatten(TestingBuffers.r)
        record_2.from_dict(flat_record)
        buf.add(record_2)
        print(buf)

        record_3 = Record(3, linking_column='data.symbol')
        flat_record = flatten(TestingBuffers.r)
        record_3.from_dict(flat_record)
        buf.add(record_3)
        print(buf)
        print(sink)

        latest = buf.get_similar('MSFT', depth=1)
        self.assertEqual(len(latest), 1)
        latest = buf.get_similar('MSFT')
        self.assertEqual(len(latest), 2)
        self.assertEqual(latest[0].rec_id, 2)
        self.assertEqual(latest[1].rec_id, 3)
        latest = buf.get_similar('AAPL')
        self.assertEqual(len(latest), 0)


class TestingDeltas(unittest.TestCase):
    r_1 = {
        "date": "2000-01-05",
        "timestamp": "2000-01-05T00-00-00.036258Z",
        "data_source": "free_tier",
        "data": {
            "open": 111.125, "high": 116.375, "low": 109.375, "close": 113.8125, "adjusted_close": 35.6219,
            "volume": 64047000, "symbol": "MSFT", "name": "Microsoft",
            "volume_array": [152, 71, 209, 3, 0, 0, 0, 0]
        }
    }

    r_2 = {
        "date": "2000-01-06",
        "timestamp": "2000-01-06T00-00-00.024625Z",
        "data_source": "free_tier",
        "data": {
            "open": 112.1875, "high": 113.875, "low": 108.375, "close": 110.12, "adjusted_close": 34.4286,
            "volume": 54966602, "symbol": "MSFT", "name": "Microsoft",
            "volume_array": [74, 185, 70, 3, 0, 0, 0, 0]
        }
    }

    def test_delta_different_records(self):
        edict = StringCache()
        schema_cache = SchemaCache()
        sink = DummySink()
        buf = RecordBuffer(sink=sink, string_cache=edict, schema_cache=schema_cache, max_size=2)
        record_1 = Record(1, linking_column='data.symbol')
        flat_record = flatten(TestingDeltas.r_1)
        record_1.from_dict(flat_record)
        buf.index_string_values(record_1)
        record_2 = Record(2, linking_column='data.symbol')
        flat_record = flatten(TestingDeltas.r_2)
        record_2.from_dict(flat_record)
        buf.index_string_values(record_2)
        d = delta(our=record_2, their=record_1, iteration=0)
        print('=== input ===')
        print(record_1)
        print(record_2)
        print('schema_hash for 1: %s' % record_1.schema_hash)
        print('schema_hash for 2: %s' % record_2.schema_hash)

        print('=== delta ===')
        print(d)
        print('bits: %s' % size_bits(d, verbose=False))

    def test_k_record(self):
        record_1 = Record(1, linking_column='data.symbol')
        flat_record = flatten(TestingDeltas.r_1)
        record_1.from_dict(flat_record)
        print('=== input ===')
        print(record_1)
        print('schema_hash for 1: %s' % record_1.schema_hash)

        print('=== key record (no matches) ===')
        print('bits: %s' % size_bits(record_1, verbose=False))

    def test_find_closest_records(self):
        edict = StringCache()
        schema_cache = SchemaCache()
        sink = DummySink()
        buf = RecordBuffer(sink=sink, string_cache=edict, schema_cache=schema_cache, max_size=2)

        # creating a sample record
        record_1 = Record(1, linking_column='data.symbol')
        flat_record = flatten(TestingDeltas.r_1)
        record_1.from_dict(flat_record)
        buf.index_string_values(record_1)
        buf.add(record_1)

        # creating another sample record
        record_2 = Record(2, linking_column='data.symbol')
        flat_record = flatten(TestingDeltas.r_2)
        record_2.from_dict(flat_record)
        buf.index_string_values(record_2)
        buf.add(record_2)

        found = buf.find_closest_to(record_2)
        print('=== input ===')
        print(record_1)
        print(record_2)
        print('schema_hash for 1: %s' % record_1.schema_hash)
        print('schema_hash for 2: %s' % record_2.schema_hash)

        print('=== found ===')
        print(found)
        print('bits: %s' % size_bits(found, verbose=False))


class TestingDoubleDeltas(unittest.TestCase):
    r_1 = {
        "date": "2000-01-05",
        "timestamp": "2000-01-05T00-00-00.036258Z",
        "data_source": "free_tier",
        "data": {
            "open": 111.125, "high": 116.375, "low": 109.375, "close": 113.8125, "adjusted_close": 35.6219,
            "volume": 64047000, "symbol": "MSFT", "name": "Microsoft",
            "volume_array": [152, 71, 209, 3, 0, 0, 0, 0]
        }
    }

    r_2 = {
        "date": "2000-01-06",
        "timestamp": "2000-01-06T00-00-00.024625Z",
        "data_source": "free_tier",
        "data": {
            "open": 112.1875, "high": 113.875, "low": 108.375, "close": 110.12, "adjusted_close": 34.4286,
            "volume": 54966602, "symbol": "MSFT", "name": "Microsoft",
            "volume_array": [74, 185, 70, 3, 0, 0, 0, 0]
        }
    }

    r_3 = {
        "date": "2000-01-07",
        "timestamp": "2000-01-07T00-00-00.060895Z",
        "data_source": "free_tier",
        "data": {
            "open": 108.625, "high": 112.25, "low": 107.3125, "close": 111.4375, "adjusted_close": 34.8785,
            "volume": 62013398, "symbol": "MSFT", "name": "Microsoft",
            "volume_array": [214, 63, 178, 3, 0, 0, 0, 0]
        }
    }

    def test_double_delta_different_records(self):
        str_cache = StringCache()
        sch_cache = SchemaCache()
        sink = DummySink()  # TODO should be a writer which buffers ready records before dump + shares *Cache objects
        buf = RecordBuffer(sink=sink, string_cache=str_cache, schema_cache=sch_cache, iteration=0, max_size=10)

        record_1 = Record(1, linking_column='data.symbol')
        flat_record = flatten(TestingDoubleDeltas.r_1)
        record_1.from_dict(flat_record)
        buf.index_string_values(record_1)
        buf.add(record_1)

        record_2 = Record(2, linking_column='data.symbol')
        flat_record = flatten(TestingDoubleDeltas.r_2)
        record_2.from_dict(flat_record)
        buf.index_string_values(record_2)
        buf.add(record_2)

        record_3 = Record(3, linking_column='data.symbol')
        flat_record = flatten(TestingDoubleDeltas.r_3)
        record_3.from_dict(flat_record)
        buf.index_string_values(record_3)
        buf.add(record_3)
        print(record_1)
        print(record_2)
        print(record_3)

        print('=== 1st iteration ===')
        collected = list(buf.buffer)
        for d in collected:
            print(d)

        buf = RecordBuffer(sink=sink, string_cache=str_cache, schema_cache=sch_cache, iteration=1, max_size=10)

        for d in collected:
            buf.add(d)

        print('=== 2nd iteration ===')
        collected = list(buf.buffer)
        for d in collected:
            print(d)
            print(size_bits(d))


if __name__ == '__main__':
    unittest.main()
