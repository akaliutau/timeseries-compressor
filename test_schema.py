import unittest

from schema import Schema, to_bytes


class TestingSchema(unittest.TestCase):

    def test_schema_serialising(self):
        schema = Schema(hashcode=123)
        schema.add_column('date', 'date')
        schema.add_column('data.open', 'float64')

        bytes_array = to_bytes(schema)
        print(len(bytes_array))
        bytes_array = schema.to_bytes()
        print(len(bytes_array))
        print(bytes_array)


if __name__ == '__main__':
    unittest.main()
