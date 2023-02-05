import numpy as np


class BinaryBuffer(object):
    def __init__(self, byte_buffer=None, size=512, start_bit_num=3):
        if byte_buffer is None:
            self.default_buffer_size = size
            self.unused_bits_in_last_byte_bit_length = start_bit_num
            self._capacity = size
            self._buffer = np.zeros(size, dtype=np.uint8)
            self._length = 0  # byte length
            self._num_bits = 0  # bit length
            self._bit_position = self.unused_bits_in_last_byte_bit_length

            # Reserve space for the unused bit count
            self.add_bits(0, self.unused_bits_in_last_byte_bit_length)
        else:
            self.unused_bits_in_last_byte_bit_length = start_bit_num
            self._length = len(byte_buffer)
            self._bit_position = 0
            self._buffer = np.frombuffer(byte_buffer, dtype=np.uint8)
            unusedBitsInLastByte = self.ReadValue(self.unused_bits_in_last_byte_bit_length)
            self._num_bits = self._length * 8 - unusedBitsInLastByte
            self._capacity = self._length

    def MoveToStartOfBuffer(self):
        self._bit_position = self.unused_bits_in_last_byte_bit_length

    def ToBytes(self):
        # Update the available bits in the last byte counter stored
        # at the start of the buffer.
        bits_available = self.__BitsAvailableInLastByte()
        bits_unused_shifted = (bits_available << (8 - self.unused_bits_in_last_byte_bit_length))
        self.__SetByteAt(0, (self._buffer[0] | bits_unused_shifted))

        copy = self._buffer[:self._length].tobytes()
        return copy

    def add_bits(self, value: int, bits_in_value=1):
        if (bits_in_value == 0):
            # Nothing to do.
            return

        bits_available = self.__BitsAvailableInLastByte()
        self._num_bits += bits_in_value

        if bits_in_value <= bits_available:
            # The value fits in the last byte
            newLastByte = self.__GetLastByte() + (value << (bits_available - bits_in_value))
            self.__SetLastByte(newLastByte)
            return

        bits_left = bits_in_value
        if (bits_available > 0):
            # Fill up the last byte
            newLastByte = self.__GetLastByte() + (value >> (bits_in_value - bits_available))
            self.__SetLastByte(newLastByte)
            bits_left -= bits_available

        while bits_left >= 8:
            # We have enough bits to fill up an entire byte
            next_byte = ((value >> (bits_left - 8)) & 0xFF)
            self.__WriteByte(next_byte)
            bits_left -= 8

        if bits_left != 0:
            # Start a new byte with the rest of the bits
            mask = ((1 << bits_left) - 1)
            next_byte = ((value & mask) << (8 - bits_left))
            self.__WriteByte(next_byte)

    def FindTheFirstZeroBit(self, limit) -> int:
        bits = 0
        while bits < limit:
            bit = self[self._bit_position]
            self._bit_position += 1
            if bit == 0:
                return bits
            bits += 1
        return bits

    def ReadValue(self, bitsToRead):
        if self._bit_position + bitsToRead > self._length * 8:
            raise Exception("exceed length")

        value = 0
        for i in range(bitsToRead):
            value <<= 1
            value += self[self._bit_position]
            self._bit_position += 1
        return value

    def IsAtEndOfBuffer(self):
        return self._bit_position >= self._num_bits

    def __BitsAvailableInLastByte(self):
        """ return 0~7 """
        bitsAvailable = (8 - (self._num_bits & 0x7)) if ((self._num_bits & 0x7) != 0) else 0
        return bitsAvailable

    def __GetLastByte(self):
        if self._length == 0:
            return 0
        return self._buffer[self._length - 1]

    def __SetLastByte(self, newValue):
        self.__SetByteAt(self._length - 1, newValue)

    def __SetByteAt(self, offset, newValue):
        if (self._length == 0):
            self.__WriteByte(newValue)
        else:
            self._buffer[offset] = newValue

    def __WriteByte(self, value):
        if self._length >= self._capacity:
            self.__GrowBuffer()

        self._buffer[self._length] = value
        self._length += 1

    def __GrowBuffer(self):
        newLength = self._capacity * 2
        newBuffer = np.hstack((self._buffer, np.ones(self._capacity, dtype=np.uint8)))
        self._buffer = newBuffer
        self._capacity = newLength

    def __str__(self):
        ret = ""
        for i in range(len(self)):
            ret += str(self[i])
        return ret

    def __len__(self):
        return self._num_bits

    def __getitem__(self, i):
        if i < 0:
            i = self._num_bits + i
        return (self._buffer[i >> 3] >> (7 - (i & 0x7))) & 1


def test(_numBits):
    bitsAvailable = (8 - (_numBits & 0x7)) if ((_numBits & 0x7) != 0) else 0
    return bitsAvailable


if __name__ == "__main__":
    a = BinaryBuffer()
    a.add_bits(7, 3)
    a.add_bits(0, 4)
    a.add_bits(31, 5)
    b = a.ToBytes()
    print(a)
    print(len(a))
    pass
