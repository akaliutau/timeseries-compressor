import argparse
import json

from bitbuffer import BitBufferWriter
from cache import StringCache, SchemaCache
from datablock import Sink
from record import Record
from recordbuffer import RecordBuffer
from utils import flatten
from writer import BlockWriter

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Generates sample files (simulates log lines or json files')
    parser.add_argument('-i', '--in', help='input file with data', required=True)
    parser.add_argument('-o', '--out', default='stock_data.bin', help='input file with data', required=False)
    parser.add_argument('-l', '--lines', help='number of lines to read', default=1000, required=False)

    args = vars(parser.parse_args())

    lines = int(args.get('lines'))
    index = 0

    with open(args.get('in'), 'r+') as fp, \
            open(args.get('out'), 'wb') as out:
        string_cache = StringCache()
        schema_cache = SchemaCache()
        bitbuffer = BitBufferWriter(out)
        bw = BlockWriter(bit_buffer=bitbuffer)
        sink = Sink(block_writer=bw, string_cache=string_cache, schema_cache=schema_cache)
        buf_2 = RecordBuffer(sink=sink, string_cache=string_cache, schema_cache=schema_cache, iteration=1, max_size=100)
        buf_1 = RecordBuffer(sink=buf_2, string_cache=string_cache, schema_cache=schema_cache, iteration=0, max_size=100)

        line = fp.readline()
        while line.strip() and lines > index:
            json_line = json.loads(line)

            record = Record(index, linking_column='data.symbol')
            flat_record = flatten(json_line)
            record.from_dict(flat_record)
            buf_1.index_string_values(record)
            buf_1.add(record)

            index += 1
            line = fp.readline()
        bitbuffer.stat.show()