# Time-series Data Compressor

This is a repository with prototype of time-series compressor loosely based on ideas described in the paper 
[Gorilla: A Fast, Scalable, In-Memory Time Series Database](https://www.vldb.org/pvldb/vol8/p1816-teller.pdf) 
(a copy included into this repository)

The main motivation was to investigate the effect of:

* double delta compression
* XOR compression for distant timestamps

Current implementation is a research _prototype_ (compresses data using double deltas and XOR binary compression for 
float numbers)

# Installation

```shell
python3 -m venv venv
source ./venv/bin/activate
pip install -r requirements.txt
```

# Testing

```shell
pytest
python3 compress.py -i data/stock_data.json -o data/stock_data.bin
```

The output will show some compression statistics similar to the next one (stat on number of key and delta records, 
plus memory overhead for string cache and schema):

```shell
string cache : 120.0 bytes
schema block : 3144.0 bytes
key record : 1682.125 bytes
delta record : 26319.375 bytes
total 31265.5 bytes
string cache : 7 block(s), avg size = 17.142857142857142 bytes/block
schema block : 2 block(s), avg size = 1572.0 bytes/block
key record : 30 block(s), avg size = 56.07083333333333 bytes/block
delta record : 677 block(s), avg size = 38.876477104874446 bytes/block
```

One block is roughly equivalent to one line of original data in the input file. 
The average size of json line is 305 bytes.


