import datetime
import json
import sys
from random import random

if __name__ == '__main__':
    li = list()
    with open('data/stock.json') as f:
        l = f.readline()
        while l:
            rec = json.loads(l)
            out = dict()
            out['date'] = rec['date']
            dt = datetime.datetime.strptime(rec['date'], '%Y-%m-%d')
            dt = dt + datetime.timedelta(milliseconds=100 * random())
            out['timestamp'] = dt.strftime('%Y-%m-%dT%H-%M-%S.%fZ')
            out['data_source'] = 'free_tier'
            data = dict()
            data['open'] = rec['open']
            data['high'] = rec['high']
            data['low'] = rec['low']
            data['close'] = rec['close']
            data['adjusted_close'] = rec['adjusted_close']
            data['volume'] = rec['volume']
            data['symbol'] = rec['symbol']
            data['name'] = rec['name']
            bytes_array = rec['volume'].to_bytes(8, sys.byteorder)
            data['volume_array'] = list(bytes_array)
            out['data'] = data
            li.append(out)
            l = f.readline()

    with open('data/stock_data.json', 'w+') as f:
        for l in li:
            f.writelines(json.dumps(l) + '\n')
