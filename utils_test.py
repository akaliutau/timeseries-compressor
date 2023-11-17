import unittest

from utils import short_hash


class Testing(unittest.TestCase):
    def test_string(self):
        a = 'some'
        b = 'some'
        self.assertEqual(a, b)

    def test_boolean(self):
        a = True
        b = True
        self.assertEqual(a, b)

    def test_lookup(self):
        val = 255
        val = val << 24
        val = 0xff000000
        print(val)

    def test_bytes(self):
        data = bytes('str_a,str_2,str_3', encoding='utf-8')
        print(data)

        str_restored = data.decode('utf-8')
        print(str_restored.split(','))

    def test_short_hash(self):
        val = 'abc123'
        hash_sum = short_hash(val)
        print('hash(%s)=%s' %(val, hash_sum))

    def test_byte_array(self):
        b_0 = bytes()
        b_1 = bytes('a', encoding='UTF-8')
        print('size of empty bytearray %s' % len(b_0))
        print('size of empty bytearray %s' % len(b_1))




if __name__ == '__main__':
    unittest.main()
