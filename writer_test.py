import unittest

from bitbuffer import DummyBufferWriter, BitBufferWriter
from writer import BlockWriter


class TestingBitBuffer(unittest.TestCase):

    def test_bytes_streams(self):
        dbw = DummyBufferWriter()
        bw = BlockWriter(bit_buffer=dbw)
        data = bytes('test:data_as_a_sequence', encoding='UTF-8')
        bw.save_schema(data)

    def test_bytes_real_test(self):

        with open('test2.bin', 'wb') as f:
            bb = BitBufferWriter(f)
            bw = BlockWriter(bit_buffer=bb)
            data = bytes('test:data_as_a_sequence', encoding='UTF-8')
            bw.save_schema(data)
            bb.close()


if __name__ == '__main__':
    unittest.main()
