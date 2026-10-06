#pragma once

namespace duckdb {
class DatabaseInstance;

//! Registers decompression for the codecs Hive and Spark write text files with besides gzip and zstd: bzip2 (.bz2),
//! lz4 (.lz4) and snappy (.snappy). Hadoop's lz4 and snappy codecs do not write the codec's own file format but raw
//! blocks with big-endian lengths (BlockCompressorStream), which is what these read; .lz4 files in the LZ4 frame
//! format (what the lz4 tool writes) are read as well. They are found by file extension like gzip and zstd, so
//! read_csv and read_json pick them up with compression 'auto', and can be named explicitly ('bzip2', 'lz4',
//! 'snappy'). Writing is not supported.
void RegisterHiveCompressionFileSystems(DatabaseInstance &db);

} // namespace duckdb
