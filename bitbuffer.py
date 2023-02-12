from abc import ABC, abstractmethod
from typing import List, Dict
from typing.io import BinaryIO

import numpy as np

FIRST_BIT_OF_BYTE = 1 << 7
CHUNK_SIZE = 512


class BufferWriter(ABC):
    @abstractmethod
    def add_value(self, value: int, bits_in_value=1):
        pass

    @abstractmethod
    def set_metric(self, metric: str):
        pass

    @abstractmethod
    def close(self):
        pass


class Statistics:

    def __init__(self):
        self.counter: Dict[str, int] = dict()
        self.volume: Dict[str, int] = dict()
        self.metric = ''

    def set_metric(self, metric: str):
        self.metric = metric
        if metric not in self.counter:
            self.counter[metric] = 0
        if metric not in self.volume:
            self.volume[metric] = 0
        self.counter[self.metric] += 1

    def measure(self, bits: int):
        if not self.metric:
            raise Exception('invoke set_metric before measuring')
        self.volume[self.metric] += bits

    def show(self):
        total_vol_bytes = 0
        for metric, vol in self.volume.items():
            print("%s : %s bytes" % (metric, vol / 8))
            total_vol_bytes += vol / 8
        print("total %s bytes" % total_vol_bytes)
        for metric, count in self.counter.items():
            print("%s : %s block(s), avg size = %s bytes/block" % (metric, count, self.volume.get(metric)/(8 * count)))


class BitBufferReader:

    def __init__(self, io: BinaryIO, size: int = CHUNK_SIZE):
        self.default_buffer_size = size
        self.unused_bits_in_last_byte_bit_length = 0
        self._capacity = size
        self._buffer = np.zeros(self._capacity, dtype=np.uint8)
        self._length = 0  # length in bytes
        self._bit_position = FIRST_BIT_OF_BYTE
        self._cur_byte = 0
        self._offset = 0
        self.io = io
        self._mask: List[int] = list()
        for shift in range(64):
            self._mask.append(1 << shift)
        self._mask.reverse()
        self._load()
        self._cur_byte = self._buffer.item(self._length)
        self._length += 1

    def _load(self):
        self._buffer = np.fromfile(self.io, dtype=np.uint8, count=self._capacity, offset=self._offset)
        print(self._buffer)
        self._offset += self._capacity

    def _get_byte(self):
        self._cur_byte = self._buffer.item(self._length)
        self._length += 1
        if self._length >= self._capacity:
            self._load()
            self._length = 0

    def get_value(self, bits_in_value: int = 1) -> int:
        if bits_in_value == 0:
            return 0
        ret = 0
        for bit_pos in range(bits_in_value):
            if self._cur_byte & self._bit_position:
                ret += 1
            ret = ret << 1
            if self._bit_position == 1:
                self._bit_position = FIRST_BIT_OF_BYTE
                self._get_byte()
            else:
                self._bit_position = self._bit_position >> 1
        ret = ret >> 1
        return ret


class BitBufferWriter(BufferWriter):

    def __init__(self, io: BinaryIO, size: int = CHUNK_SIZE):
        self.default_buffer_size = size
        self.unused_bits_in_last_byte_bit_length = 0
        self._capacity = size
        self._buffer = np.zeros(self._capacity, dtype=np.uint8)
        self._length = 0  # length in bytes
        self._bit_position = FIRST_BIT_OF_BYTE
        self._cur_byte = 0
        self._offset = 0
        self.io = io
        self._mask: List[int] = list()
        for i in range(64):
            self._mask.append(1 << i)
        self._mask.reverse()
        self.stat = Statistics()

    def _flush(self):
        self._buffer.tofile(self.io)

    def _push_byte(self):
        self._buffer.itemset(self._length, self._cur_byte)
        self._cur_byte = 0
        self._length += 1
        if self._length >= self._capacity:
            self._flush()
            self._buffer = np.zeros(self._capacity, dtype=np.uint8)
            self._length = 0

    def set_metric(self, metric: str):
        self.stat.set_metric(metric)

    def add_value(self, value: int, bits_in_value: int = 1):
        if bits_in_value == 0:
            # Nothing to do.
            return
        for bit_pos in range(bits_in_value):
            print('test: %s == %s' % (value, self._mask[64 - bits_in_value + bit_pos]))
            if value & self._mask[64 - bits_in_value + bit_pos]:
                self._cur_byte |= self._bit_position
            if self._bit_position == 1:
                self._bit_position = FIRST_BIT_OF_BYTE
                self._push_byte()
            else:
                self._bit_position = self._bit_position >> 1
        print('added data: %s' % self._cur_byte)
        self.stat.measure(bits_in_value)

    def close(self):
        print('closing buffer')
        print(self._cur_byte)

        if self._length > 0 or self._bit_position != FIRST_BIT_OF_BYTE:
            self._push_byte()
            print(self._buffer)
            self._flush()


class DummyBufferWriter(BufferWriter):
    def __init__(self, size: int = CHUNK_SIZE):
        self.saved_bits = 0
        self.stat = Statistics()

    def set_metric(self, metric: str):
        self.stat.set_metric(metric)

    def add_value(self, value: int, bits_in_value=1):
        print('added %s' % value)
        self.saved_bits += bits_in_value
        self.stat.measure(bits_in_value)

    def close(self):
        print('closing buffer, total bytes saved: %s' % (self.saved_bits / 8))
