import unittest

from cache import SchemaCache
from record import Record
from utils import flatten


class TestingSchemaCacheCls(unittest.TestCase):
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
            "volume_array": [74, 185, 70, 3, 0, 0, 0, 0],
            "extra_field": "dummy_data"
        }
    }

    def test_cache_methods(self):
        record_1 = Record(1, linking_column='data.symbol')
        flat_record = flatten(TestingSchemaCacheCls.r_1)
        record_1.from_dict(flat_record)
        schema_hash, schema_data = record_1.get_schema()

        print(record_1)
        print(schema_data)
        schema_cache = SchemaCache()
        schema_cache.add(schema_hash, schema_data)
        print('has_unsaved %s' % schema_cache.has_unsaved())
        print('size %s' % schema_cache.size)
        print('saved_records %s' % schema_cache.saved_records)
        print('saved_records, dump [%s] bytes' % len(schema_cache.unsaved_to_bytes()))
        print('saved_records %s' % schema_cache.saved_records)

    def test_restore_cache_methods(self):
        schema_cache = SchemaCache()

        record_1 = Record(1, linking_column='data.symbol')
        flat_record = flatten(TestingSchemaCacheCls.r_1)
        record_1.from_dict(flat_record)
        schema_hash, schema_data = record_1.get_schema()
        schema_cache.add(schema_hash, schema_data)

        record_2 = Record(2, linking_column='data.symbol')
        flat_record = flatten(TestingSchemaCacheCls.r_2)
        record_2.from_dict(flat_record)
        schema_hash, schema_data = record_2.get_schema()
        schema_cache.add(schema_hash, schema_data)

        print('has_unsaved %s' % schema_cache.has_unsaved())
        print('size %s' % schema_cache.size)
        print('saved_records %s' % schema_cache.saved_records)
        print('saved schema:')
        print(schema_cache.schemas)
        dump = schema_cache.unsaved_to_bytes()
        print('saved_records, dump [%s] bytes' % len(dump))
        print('saved_records %s' % schema_cache.saved_records)

        restored_schema_cache = SchemaCache()
        restored_schema_cache.append_from_bytes(dump)
        print('restored schema:')
        print(restored_schema_cache.schemas)


if __name__ == '__main__':
    unittest.main()
