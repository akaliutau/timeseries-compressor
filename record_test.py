import unittest

from record import Record
from utils import flatten, float_to_int


class TestingRecordCls(unittest.TestCase):
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

    def test_record_methods(self):
        record_1 = Record(1, linking_column='data.symbol')
        flat_record = flatten(TestingRecordCls.r_1)
        record_1.from_dict(flat_record)
        schema_hash, schema_data = record_1.get_schema()

        print(record_1)
        print(schema_data)

    def test_record_constructors(self):
        schema = ['date:date', 'timestamp:timestamp', 'data_source:string', 'data.open:float64']
        vector = [36723, 298378283723, 'test_string', float_to_int(3.14)]
        rec = Record(rec_id=1, linking_column='data_source')
        rec.from_vector(vector, schema)
        print(rec)
        extract_vector = rec.get_vector()
        print(extract_vector)


if __name__ == '__main__':
    unittest.main()
