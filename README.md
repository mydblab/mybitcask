# mybitcask (🚧WIP)


C++ implementation of the [bitcask](https://github.com/basho/bitcask/blob/develop-3.0/doc/bitcask-intro.pdf)-like db (with [improvements](https://github.com/mydblab/mybitcask#improvements-to-bitcask)).  A Log-Structured Hash Table for Fast Key/Value Data.


Mybitcask is a library that provides an embeddable, persistent key-value store for fast storage.

mybitcask-mysql is a MySQL pluggable storage engine based on Mybitcask.

## Improvements to bitcask
TODO

## mykv
A simple cli tool

![](https://github.com/mydblab/mybitcask/blob/develop/screenshots/mykv.png?raw=true)

### build
```sh
mkdir build
cmake --build ./build --target mykv -j 8
mkdir db_data
./build/mykv ./db_data
```
