#!/usr/bin/env python3
"""Writes the compressed text files test/sql/hive/compression/*.test read, into test/data/compression.

Hadoop's Lz4Codec and SnappyCodec (what Hive and Spark write text files with) do not write the codec's own file format
but BlockCompressorStream's: blocks of the 4-byte big-endian uncompressed length followed by chunks, each the 4-byte
big-endian compressed length and a raw lz4 / snappy block. The snappy blocks here are literal-only, which is a valid
snappy encoding (and what small files written by Athena contain); decoding real compressed snappy is the library's job.

    pip install lz4 && python3 scripts/generate_compression_fixtures.py
"""

import bz2
import json
import os
import struct

import lz4.block
import lz4.frame

OUT = os.path.join(os.path.dirname(__file__), "..", "test", "data", "compression")

CSV = "".join(f'"{i}","v{i}"\n' for i in range(1, 7)).encode()
JSON = "".join(json.dumps({"id": i, "name": f"v{i}"}) + "\n" for i in range(1, 7)).encode()


def snappy_literal(data: bytes) -> bytes:
    # uncompressed length as a varint, then the data as literal elements
    out = bytearray()
    n = len(data)
    while True:
        byte = n & 0x7F
        n >>= 7
        out.append(byte | (0x80 if n else 0))
        if not n:
            break
    for start in range(0, len(data), 65536):
        piece = data[start : start + 65536]
        length = len(piece) - 1
        if length < 60:
            out.append(length << 2)
        elif length < 256:
            out += bytes([60 << 2, length])
        else:
            out += bytes([61 << 2]) + struct.pack("<H", length)
        out += piece
    return bytes(out)


def lz4_raw(data: bytes) -> bytes:
    return lz4.block.compress(data, store_size=False)


def hadoop_blocks(data: bytes, compress, block_size: int, chunk_size: int) -> bytes:
    out = bytearray()
    for block_start in range(0, len(data), block_size):
        block = data[block_start : block_start + block_size]
        out += struct.pack(">I", len(block))
        for chunk_start in range(0, len(block), chunk_size):
            chunk = compress(block[chunk_start : chunk_start + chunk_size])
            out += struct.pack(">I", len(chunk)) + chunk
    return bytes(out)


def write(name: str, data: bytes) -> None:
    with open(os.path.join(OUT, name), "wb") as f:
        f.write(data)


def main() -> None:
    os.makedirs(OUT, exist_ok=True)
    # one block of one chunk, as small files are; then several blocks of several chunks
    write("hadoop.csv.lz4", hadoop_blocks(CSV, lz4_raw, 1 << 18, 1 << 18))
    write("hadoop_multi.csv.lz4", hadoop_blocks(CSV, lz4_raw, 30, 11))
    write("hadoop.csv.snappy", hadoop_blocks(CSV, snappy_literal, 1 << 18, 1 << 18))
    write("hadoop_multi.csv.snappy", hadoop_blocks(CSV, snappy_literal, 30, 11))
    write("hadoop.json.lz4", hadoop_blocks(JSON, lz4_raw, 1 << 18, 1 << 18))
    write("hadoop.json.snappy", hadoop_blocks(JSON, snappy_literal, 1 << 18, 1 << 18))
    # what the lz4 tool writes
    write("frame.csv.lz4", lz4.frame.compress(CSV))
    write("data.csv.bz2", bz2.compress(CSV))
    write("data.json.bz2", bz2.compress(JSON))
    # two streams, as concatenating .bz2 files gives
    write("concatenated.csv.bz2", bz2.compress(CSV[:30]) + bz2.compress(CSV[30:]))
    # a Hive table directory whose files use different codecs, as appends with changed settings leave it
    os.makedirs(os.path.join(OUT, "table"), exist_ok=True)
    lines = CSV.splitlines(keepends=True)
    first, rest = b"".join(lines[:3]), b"".join(lines[3:])
    write("table/part-0.csv.lz4", hadoop_blocks(first, lz4_raw, 1 << 18, 1 << 18))
    write("table/part-1.csv.snappy", hadoop_blocks(rest, snappy_literal, 1 << 18, 1 << 18))
    write("table/part-2.csv.bz2", bz2.compress(CSV))
    # corrupt or unsupported
    whole = hadoop_blocks(CSV, lz4_raw, 1 << 18, 1 << 18)
    write("truncated.csv.lz4", whole[: len(whole) - 5])
    write("truncated.csv.bz2", bz2.compress(CSV)[:-5])
    write("framed.csv.snappy", b"\xff\x06\x00\x00sNaPpY")
    write("empty.csv.snappy", b"")


if __name__ == "__main__":
    main()
