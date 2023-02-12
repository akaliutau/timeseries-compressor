import unittest

from bitbuffer import BitBufferWriter, BitBufferReader


class TestingBitBuffer(unittest.TestCase):

    def test_functional_saving(self):
        with open('test.bin', 'wb') as f:
            bb = BitBufferWriter(f)
            print('mask')
            print(bb._mask)

            bb.add_value(1, 1)
            bb.add_value(7, 3)
            bb.add_value(0, 1)
            bb.add_value(8, 4)
            bb.add_value(1, 1)
            # should write 11110100|01000000 = F440
            bb.close()

        with open('test.bin', 'rb') as f:
            bb = BitBufferReader(f)
            print(bb.get_value(1))
            print(bb.get_value(3))
            print(bb.get_value(1))
            print(bb.get_value(4))
            print(bb.get_value(1))

    def test_special_cases_saving(self):
        with open('test1.bin', 'wb') as f:
            bb = BitBufferWriter(f)
            print('mask')
            print(bb._mask)

            bb.add_value(1, 1)
            bb.add_value(7, 32)
            bb.add_value(0, 1)
            bb.add_value(8, 4)
            bb.add_value(1, 1)
            # should write 11110100|01000000 = F440
            bb.close()

        with open('test1.bin', 'rb') as f:
            bb = BitBufferReader(f)
            print(bb.get_value(1))
            print(bb.get_value(32))
            print(bb.get_value(1))
            print(bb.get_value(4))
            print(bb.get_value(1))


if __name__ == '__main__':
    unittest.main()
