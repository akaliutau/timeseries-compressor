from typing import List

from bitbuffer import BufferWriter
from record import Record
from utils import float_lookup_table

KEY_RECORD_BLOCK: int = 0  # [x00, x00]
SCHEMA_BLOCK: int = 1  # [x00, x01]
STRING_CACHE_BLOCK: int = 2  # [x00, x02]

UINT8 = 1 << 7
UINT16 = 1 << 15
UINT32 = 1 << 31
UINT64 = 1 << 63


def t_uint8(v: int) -> int:
    return UINT8 + v


def t_uint16(v: int) -> int:
    return UINT16 + v


def t_uint32(v: int) -> int:
    return UINT32 + v


def t_uint64(v: int) -> int:
    return UINT64 + v


def t_int16(buf: BufferWriter, value: int) -> None:
    if value == 0:
        buf.add_value(0, 1)  # bits 0
    elif t_uint8(value) < 256:
        buf.add_value(2, 2)
        buf.add_value(t_uint8(value), 8)  # bits 10|8bit
    else:
        buf.add_value(3, 2)
        buf.add_value(t_uint16(value), 16)  # bits 11|16bit


def t_int32(buf: BufferWriter, value: int) -> None:
    if value == 0:
        buf.add_value(0, 1)  # bits 0
    elif t_uint8(value) < 256:
        buf.add_value(2, 2)
        buf.add_value(t_uint8(value), 8)  # bits 10|8bit
    else:
        buf.add_value(3, 2)
        buf.add_value(t_uint32(value), 32)  # bits 11|32bit


def t_int64(buf: BufferWriter, value: int) -> None:
    if value == 0:
        buf.add_value(0, 1)  # bits 0
    elif t_uint8(value) < 256:
        buf.add_value(2, 2)
        buf.add_value(t_uint8(value), 8)  # bits 10|8bit
    else:
        buf.add_value(3, 2)
        buf.add_value(t_uint64(value), 64)  # bits 11|64bit


def t_float64(buf: BufferWriter, value: int) -> None:
    if value == 0:
        buf.add_value(0, 1)  # bits 0
        return
    # here: value is NOT zero => must have at least 1 non-zero byte
    i = 0
    while i < 8:
        if value & float_lookup_table[i]:
            break
        i += 1
    first_non_zero_byte = i  # edge cases -  (i=0) no prefix (i=7) - only 1 last byte is != 0
    i = 7
    while i > -1:
        if value & float_lookup_table[i]:
            break
        i -= 1
    last_non_zero_byte = i  # edge cases -  (7) no suffix) (0) - first byte
    important_bytes = last_non_zero_byte - first_non_zero_byte
    buf.add_value(1, 1)  # bits 1
    buf.add_value(first_non_zero_byte, 3)  # bits 3
    buf.add_value(important_bytes, 3)  # bits 3
    shift_in_bits = (7 - last_non_zero_byte) * 8
    value = value >> shift_in_bits
    buf.add_value(value, (important_bytes + 1) * 8)  # bits 1  # bits 1|3bit|3bit|data


def t_str(buf: BufferWriter, value: int) -> None:
    if value == 0:
        buf.add_value(0, 1)  # bits 0
        return
    buf.add_value(1, 1)  # bits 1
    buf.add_value(value, 16)  # bits 16 for index TODO can be optimized


def t_array(buf: BufferWriter, arr: List[int]) -> None:
    if not arr:
        buf.add_value(0, 1)  # bits 0
        return
    # here: arr has at least 1 non-zero byte
    i = 0
    while i < len(arr):
        if arr[i]:
            break
        i += 1
    first_non_zero_byte = i  # edge cases -  (i=0) no prefix (i=7) - only 1 last byte
    i = len(arr) - 1
    while i > -1:
        if arr[i]:
            break
        i -= 1
    last_non_zero_byte = i  # edge cases -  (i=7) no suffix) (i=0) - first byte
    important_bytes = last_non_zero_byte - first_non_zero_byte
    buf.add_value(1, 1)  # bits 1
    buf.add_value(first_non_zero_byte, 5)  # bits 5
    buf.add_value(important_bytes, 5)  # bits 5
    i = first_non_zero_byte
    while i <= last_non_zero_byte:
        buf.add_value(arr[i], 8)  # bits 1  # bits 1|5bit|5bit|data
        i += 1


t_operators = {
    'float64': t_float64,
    'int32': t_int32,
    'date': t_int16,
    'timestamp': t_int64,
    'string': t_str,
    'array': t_array,
    'nullable': t_str
}


class BlockWriter:
    FIRST_BIT = 1 << 63

    def __init__(self, bit_buffer: BufferWriter):
        self.buf = bit_buffer

    def save_schema(self, data: bytes) -> None:
        if not data or len(data) == 0:
            return
        self.buf.set_metric('schema block')
        self.buf.add_value(SCHEMA_BLOCK, 16)
        self.buf.add_value(len(data), 32)
        for b in data:
            self.buf.add_value(b, 8)

    def save_string_cache(self, data: bytes) -> None:
        if not data or len(data) == 0:
            return
        self.buf.set_metric('string cache')
        self.buf.add_value(STRING_CACHE_BLOCK, 16)
        self.buf.add_value(len(data), 32)
        for b in data:
            self.buf.add_value(b, 8)

    def save_record(self, r: Record) -> None:
        print('sink: %s' % r)
        if r.signature == KEY_RECORD_BLOCK:
            self.buf.set_metric('key record')
        else:
            self.buf.set_metric('delta record')
        self.buf.add_value(r.first_ref + 128, 8)
        self.buf.add_value(r.second_ref + 128, 8)
        if r.signature == KEY_RECORD_BLOCK:
            self.buf.add_value(r.schema_hash, 32)
        for field in r.columns.values():
            t_operators[field.value_type](self.buf, field.stored)
