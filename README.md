# Time-series Data Compressor

This is a variation of implementation of ideas in the paper 
[Gorilla: A Fast, Scalable, In-Memory Time Series Database](https://www.vldb.org/pvldb/vol8/p1816-teller.pdf) 
(a copy included into this repository)

Main ideas: delta compression, XOR compression for float numbers, etc

Current implementation is a prototype, which compresses data using double deltas and XOR binary compression for 
float numbers

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

The output will show some compression statistics similar to:

```shell
string cache : 120.0 bytes
schema block : 3144.0 bytes
key record : 1682.125 bytes
delta record : 26319.375 bytes
total 31265.5 bytes
string cache : 7 block(s), avg size = 17.142857142857142 bytes/record
schema block : 2 block(s), avg size = 1572.0 bytes/record
key record : 30 block(s), avg size = 56.07083333333333 bytes/record
delta record : 677 block(s), avg size = 38.876477104874446 bytes/record
```


