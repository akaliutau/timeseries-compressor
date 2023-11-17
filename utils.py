import time
from datetime import datetime, date
from typing import List

import numpy as np


def int16_est(value: int) -> int:
    if value == 0:
        return 1  # bits 0
    elif abs(value + 256) < 256:
        return 10  # bits 10|8bit
    else:
        return 18  # bits 11|16bit


def int32_est(value: int) -> int:
    if value == 0:
        return 1  # bits 0
    elif abs(value + 256) < 256:
        return 10  # bits 10|8bit
    else:
        return 34  # bits 11|32bit


def int64_est(value: int) -> int:
    if value == 0:
        return 1  # bits 0
    elif abs(value + 256) < 256:
        return 10  # bits 10|8bit
    else:
        return 66  # bits 11|64bit


float_lookup_table: List[int] = [
    0xff00000000000000,
    0x00ff000000000000,
    0x0000ff0000000000,
    0x000000ff00000000,
    0x00000000ff000000,
    0x0000000000ff0000,
    0x000000000000ff00,
    0x00000000000000ff
]


def float64_est(value: int) -> int:
    if value == 0:
        return 1
    i = 0
    while i < 8:
        if value & float_lookup_table[i]:
            break
        i += 1
    offset = i
    i = 7
    while i > -1:
        if value & float_lookup_table[i]:
            break
        i -= 1
    return 7 + (i - offset + 1) * 8  # bits 1|3bit|3bit|data


def string_est(value: str) -> int:
    if value:
        return 17
    return 1


def array_est(value: List[int]) -> int:
    i = 0
    while i < len(value):
        if value[i] != 0:
            break
        i += 1
    if i == len(value):
        return 1
    offset = i
    i = len(value) - 1
    while i > -1:
        if value[i] != 0:
            break
        i -= 1
    return 11 + (i - offset + 1) * 8  # bits 1|5bit|5bit|data


def delta_int(our: int, their: int) -> int:
    return our - their


def delta_float(our: int, their: int) -> int:
    return our ^ their


def delta_str(our: int, their: int) -> int:
    return 0 if our == their else our


def delta_array(our: List[int], their: List[int]) -> List[int]:
    size = min(len(our), len(their))
    ret = list()
    for i in range(size):
        ret.append(our[i] ^ their[i])
    if size < len(our):
        ret.extend(our[size:])
    return ret


def flatten(rec: dict) -> dict:
    ret = dict()
    for k, v in rec.items():
        if k == 'data':
            continue
        ret[k] = v
    if 'data' in rec:
        for k, v in rec['data'].items():
            ret['data.' + k] = v

    return ret


def _to_bytes(self, value: float) -> str:
    return hex(np.frombuffer(self.numfunc(value).tobytes(), dtype=self.parsefunc)[0]) + ' ' + str(value)


def float_to_int(value: float) -> int:
    return int(np.frombuffer(np.float64(value).tobytes(), dtype=np.uint64)[0])


def date_to_int(value: float) -> int:
    return int(np.frombuffer(np.float64(value).tobytes(), dtype=np.unit64)[0])


def date_to_days_epoch(dt: date) -> int:
    seconds = time.mktime(dt.timetuple())
    return int(round(seconds / 86400))


def datetime_to_milliseconds_epoch(dt: datetime) -> int:
    microseconds = time.mktime(dt.timetuple()) * 1000000 + dt.microsecond
    return int(round(microseconds) / 1000)


def datetime_to_microseconds_epoch(dt: datetime) -> int:
    microseconds = time.mktime(dt.timetuple()) * 1000000 + dt.microsecond
    return int(round(microseconds))


def short_hash(obj: any) -> int:  # Don't need this
    return hash(any) & 0xffff
