#include "core/hive_compression.hpp"

#include "duckdb/common/compressed_file_system.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/database.hpp"

#include <bzlib.h>
#include <lz4.h>
#include <lz4frame.h>
#include <snappy.h>

namespace duckdb {

namespace {

constexpr idx_t IN_BUFFER_SIZE = 1 << 20;
constexpr idx_t OUT_BUFFER_SIZE = 1 << 20;
//! A Hadoop block or chunk length above this is a corrupt file, not something to allocate
constexpr idx_t MAX_HADOOP_BLOCK_SIZE = 1 << 28;
//! The first four bytes (little-endian) of a file in the LZ4 frame format
constexpr uint32_t LZ4_FRAME_MAGIC = 0x184D2204;

bool HasExtension(const string &path, const char *extension) {
	auto end = path.find('?');
	auto name = StringUtil::Lower(end == string::npos ? path : path.substr(0, end));
	return StringUtil::EndsWith(name, extension);
}

//! Room for 'size' more bytes after out_buff_end. CompressedFile drains the output buffer before every Read and keeps
//! no other copy of it, so it can be replaced by a larger one.
void ReserveOutput(StreamData &sd, idx_t size) {
	auto used = NumericCast<idx_t>(sd.out_buff_end - sd.out_buff.get());
	if (used + size <= sd.out_buf_size) {
		return;
	}
	auto start = NumericCast<idx_t>(sd.out_buff_start - sd.out_buff.get());
	auto new_size = MaxValue<idx_t>(sd.out_buf_size * 2, used + size);
	auto new_buff = make_unsafe_uniq_array<data_t>(new_size);
	memcpy(new_buff.get(), sd.out_buff.get(), used);
	sd.out_buff = std::move(new_buff);
	sd.out_buf_size = new_size;
	sd.out_buff_start = sd.out_buff.get() + start;
	sd.out_buff_end = sd.out_buff.get() + used;
}

idx_t OutputRoom(const StreamData &sd) {
	return NumericCast<idx_t>(sd.out_buff.get() + sd.out_buf_size - sd.out_buff_end);
}

uint32_t LoadBigEndian(const_data_ptr_t bytes) {
	return (uint32_t(bytes[0]) << 24) | (uint32_t(bytes[1]) << 16) | (uint32_t(bytes[2]) << 8) | uint32_t(bytes[3]);
}

//===--------------------------------------------------------------------===//
// bzip2
//===--------------------------------------------------------------------===//
class BZip2StreamWrapper : public StreamWrapper {
public:
	~BZip2StreamWrapper() override {
		Close();
	}

	void Initialize(QueryContext context, CompressedFile &file, bool write) override {
		if (write) {
			throw NotImplementedException("Writing bzip2 compressed files is not supported");
		}
		path = file.path;
		Start();
	}

	bool Read(StreamData &sd) override {
		if (ended) {
			// more input after the end of a stream: the next of concatenated streams
			if (sd.in_buff_start == sd.in_buff_end) {
				return false;
			}
			BZ2_bzDecompressEnd(&stream);
			Start();
		}
		while (true) {
			auto input = NumericCast<idx_t>(sd.in_buff_end - sd.in_buff_start);
			stream.next_in = char_ptr_cast(sd.in_buff_start);
			stream.avail_in = NumericCast<unsigned int>(MinValue<idx_t>(input, NumericLimits<unsigned int>::Maximum()));
			stream.next_out = char_ptr_cast(sd.out_buff_end);
			stream.avail_out =
			    NumericCast<unsigned int>(MinValue<idx_t>(OutputRoom(sd), NumericLimits<unsigned int>::Maximum()));
			auto ret = BZ2_bzDecompress(&stream);
			if (ret != BZ_OK && ret != BZ_STREAM_END) {
				throw IOException("Failed to decode bzip2 stream of '%s' (bzip2 error %d)", path, ret);
			}
			seen_input = seen_input || stream.next_in != char_ptr_cast(sd.in_buff_start);
			sd.in_buff_start = data_ptr_cast(stream.next_in);
			sd.out_buff_end = data_ptr_cast(stream.next_out);
			if (ret == BZ_STREAM_END) {
				ended = true;
				return false;
			}
			if (sd.in_buff_start != sd.in_buff_end || stream.avail_out > 0) {
				return false;
			}
			// All input is consumed with the output buffer full, so bzip2 may still hold output. CompressedFile stops
			// at the end of the input without another Read, so that output has to come out now.
			ReserveOutput(sd, OUT_BUFFER_SIZE);
		}
	}

	void FinalizeRead(StreamData &sd) override {
		if (!ended && seen_input) {
			throw IOException("Unexpected end of bzip2 stream of '%s'", path);
		}
	}

	void Write(CompressedFile &file, StreamData &sd, data_ptr_t buffer, int64_t nr_bytes) override {
		throw InternalException("bzip2 stream opened for reading was written to");
	}

	void Close() override {
		if (started) {
			BZ2_bzDecompressEnd(&stream);
			started = false;
		}
	}

private:
	void Start() {
		memset(&stream, 0, sizeof(stream));
		auto ret = BZ2_bzDecompressInit(&stream, 0, 0);
		if (ret != BZ_OK) {
			throw IOException("Failed to initialize bzip2 decompression of '%s' (bzip2 error %d)", path, ret);
		}
		started = true;
		ended = false;
	}

private:
	string path;
	bz_stream stream;
	bool started = false;
	bool ended = false;
	bool seen_input = false;
};

//===--------------------------------------------------------------------===//
// Hadoop lz4 / snappy
//===--------------------------------------------------------------------===//
enum class HadoopBlockCodec : uint8_t { LZ4, SNAPPY };

//! Hadoop's BlockCompressorStream: a sequence of blocks, each the 4-byte big-endian length of the block's uncompressed
//! data followed by compressed chunks (a 4-byte big-endian length and the codec's raw block) until that many bytes are
//! produced. A '.lz4' file starting with the LZ4 frame magic number is in the LZ4 frame format instead.
class HadoopBlockStreamWrapper : public StreamWrapper {
public:
	explicit HadoopBlockStreamWrapper(HadoopBlockCodec codec_p) : codec(codec_p) {
	}
	~HadoopBlockStreamWrapper() override {
		Close();
	}

	void Initialize(QueryContext context, CompressedFile &file, bool write) override {
		if (write) {
			throw NotImplementedException("Writing %s compressed files is not supported", CodecName());
		}
		path = file.path;
	}

	bool Read(StreamData &sd) override {
		// at most one chunk per call, decoded straight into the (empty) output buffer, so no decoded data is ever held
		// back: CompressedFile stops at the end of the input without another Read
		const_data_ptr_t bytes;
		while (true) {
			switch (state) {
			case State::BLOCK_LENGTH:
				if (!Take(sd, sizeof(uint32_t), bytes)) {
					return false;
				}
				if (at_start) {
					at_start = false;
					if (codec == HadoopBlockCodec::LZ4 && LoadLittleEndian(bytes) == LZ4_FRAME_MAGIC) {
						StartFrame(bytes);
						break;
					}
					if (codec == HadoopBlockCodec::SNAPPY && bytes[0] == 0xff && bytes[1] == 0x06) {
						throw NotImplementedException("'%s' is in the snappy framing format, only the snappy files "
						                              "Hadoop's SnappyCodec writes are supported",
						                              path);
					}
				}
				block_remaining = CheckedLength(LoadBigEndian(bytes));
				if (block_remaining > 0) {
					state = State::CHUNK_LENGTH;
				}
				break;
			case State::CHUNK_LENGTH:
				if (!Take(sd, sizeof(uint32_t), bytes)) {
					return false;
				}
				chunk_length = CheckedLength(LoadBigEndian(bytes));
				state = State::CHUNK;
				break;
			case State::CHUNK:
				if (!Take(sd, chunk_length, bytes)) {
					return false;
				}
				DecodeChunk(sd, bytes);
				state = block_remaining > 0 ? State::CHUNK_LENGTH : State::BLOCK_LENGTH;
				return false;
			case State::FRAME:
				return ReadFrame(sd);
			}
		}
	}

	void FinalizeRead(StreamData &sd) override {
		if (state == State::FRAME) {
			if (!frame_ended) {
				throw IOException("Unexpected end of lz4 frame of '%s'", path);
			}
			return;
		}
		if (state != State::BLOCK_LENGTH || !pending.empty()) {
			throw IOException("Unexpected end of %s stream of '%s'", CodecName(), path);
		}
	}

	void Write(CompressedFile &file, StreamData &sd, data_ptr_t buffer, int64_t nr_bytes) override {
		throw InternalException("%s stream opened for reading was written to", CodecName());
	}

	void Close() override {
		if (frame_context) {
			LZ4F_freeDecompressionContext(frame_context);
			frame_context = nullptr;
		}
	}

private:
	enum class State : uint8_t { BLOCK_LENGTH, CHUNK_LENGTH, CHUNK, FRAME };

	const char *CodecName() const {
		return codec == HadoopBlockCodec::LZ4 ? "lz4" : "snappy";
	}

	static uint32_t LoadLittleEndian(const_data_ptr_t bytes) {
		return uint32_t(bytes[0]) | (uint32_t(bytes[1]) << 8) | (uint32_t(bytes[2]) << 16) | (uint32_t(bytes[3]) << 24);
	}

	idx_t CheckedLength(uint32_t length) const {
		if (length > MAX_HADOOP_BLOCK_SIZE) {
			throw IOException("Corrupt %s stream of '%s': block length %d", CodecName(), path, length);
		}
		return length;
	}

	//! Point 'result' at the next 'count' bytes of the stream and consume them. Bytes that span input buffers are
	//! gathered in 'pending' (false is returned until all have arrived); 'result' stays valid until the next Take.
	bool Take(StreamData &sd, idx_t count, const_data_ptr_t &result) {
		auto available = NumericCast<idx_t>(sd.in_buff_end - sd.in_buff_start);
		if (pending.empty() && available >= count) {
			result = sd.in_buff_start;
			sd.in_buff_start += count;
			return true;
		}
		auto copy = MinValue<idx_t>(count - pending.size(), available);
		pending.insert(pending.end(), sd.in_buff_start, sd.in_buff_start + copy);
		sd.in_buff_start += copy;
		if (pending.size() < count) {
			return false;
		}
		taken.swap(pending);
		pending.clear();
		result = taken.data();
		return true;
	}

	void DecodeChunk(StreamData &sd, const_data_ptr_t chunk) {
		idx_t decoded;
		if (codec == HadoopBlockCodec::LZ4) {
			ReserveOutput(sd, block_remaining);
			auto result = LZ4_decompress_safe(const_char_ptr_cast(chunk), char_ptr_cast(sd.out_buff_end),
			                                  NumericCast<int>(chunk_length), NumericCast<int>(block_remaining));
			if (result < 0) {
				throw IOException("Corrupt lz4 stream of '%s': a block does not decompress", path);
			}
			decoded = NumericCast<idx_t>(result);
		} else {
			size_t length;
			if (!snappy::GetUncompressedLength(const_char_ptr_cast(chunk), chunk_length, &length) ||
			    length > block_remaining) {
				throw IOException("Corrupt snappy stream of '%s': a block has an invalid length", path);
			}
			ReserveOutput(sd, length);
			if (!snappy::RawUncompress(const_char_ptr_cast(chunk), chunk_length, char_ptr_cast(sd.out_buff_end))) {
				throw IOException("Corrupt snappy stream of '%s': a block does not decompress", path);
			}
			decoded = length;
		}
		sd.out_buff_end += decoded;
		block_remaining -= decoded;
	}

	void StartFrame(const_data_ptr_t magic) {
		auto ret = LZ4F_createDecompressionContext(&frame_context, LZ4F_VERSION);
		if (LZ4F_isError(ret)) {
			throw IOException("Failed to initialize lz4 decompression of '%s': %s", path, LZ4F_getErrorName(ret));
		}
		// the magic number was already consumed: hand it to the decoder, which only buffers it
		data_t unused;
		size_t output = 0;
		size_t input = sizeof(uint32_t);
		ret = LZ4F_decompress(frame_context, &unused, &output, magic, &input, nullptr);
		if (LZ4F_isError(ret) || input != sizeof(uint32_t)) {
			throw IOException("Failed to decode lz4 frame of '%s'", path);
		}
		state = State::FRAME;
	}

	bool ReadFrame(StreamData &sd) {
		while (true) {
			auto room = OutputRoom(sd);
			size_t output = room;
			size_t input = NumericCast<size_t>(sd.in_buff_end - sd.in_buff_start);
			auto ret = LZ4F_decompress(frame_context, sd.out_buff_end, &output, sd.in_buff_start, &input, nullptr);
			if (LZ4F_isError(ret)) {
				throw IOException("Failed to decode lz4 frame of '%s': %s", path, LZ4F_getErrorName(ret));
			}
			sd.in_buff_start += input;
			sd.out_buff_end += output;
			// 0: a frame ended; another may follow (concatenated frames decode with the same context)
			frame_ended = ret == 0;
			if (sd.in_buff_start != sd.in_buff_end || output < room) {
				return false;
			}
			// all input consumed with the output buffer full: the decoder may hold more output, take it now
			ReserveOutput(sd, OUT_BUFFER_SIZE);
		}
	}

private:
	HadoopBlockCodec codec;
	string path;
	State state = State::BLOCK_LENGTH;
	bool at_start = true;
	idx_t block_remaining = 0;
	idx_t chunk_length = 0;
	vector<data_t> pending;
	vector<data_t> taken;
	LZ4F_dctx *frame_context = nullptr;
	bool frame_ended = false;
};

//===--------------------------------------------------------------------===//
// File systems
//===--------------------------------------------------------------------===//
enum class HiveCodec : uint8_t { BZIP2, LZ4, SNAPPY };

class HiveCompressionFileSystem : public CompressedFileSystem {
public:
	explicit HiveCompressionFileSystem(HiveCodec codec_p) : codec(codec_p) {
	}

	unique_ptr<FileHandle> OpenCompressedFile(QueryContext context, unique_ptr<FileHandle> handle, bool write) override;

	std::string GetName() const override {
		switch (codec) {
		case HiveCodec::BZIP2:
			return "BZip2FileSystem";
		case HiveCodec::LZ4:
			return "HadoopLZ4FileSystem";
		default:
			return "HadoopSnappyFileSystem";
		}
	}

	FileCompressionType GetCompressionType() override {
		switch (codec) {
		case HiveCodec::BZIP2:
			return FileCompressionType("bzip2");
		case HiveCodec::LZ4:
			return FileCompressionType("lz4");
		default:
			return FileCompressionType("snappy");
		}
	}

	//! Hadoop's CompressionCodecFactory picks the codec by these extensions too
	bool CanHandleFile(const string &fpath) override {
		switch (codec) {
		case HiveCodec::BZIP2:
			return HasExtension(fpath, ".bz2");
		case HiveCodec::LZ4:
			return HasExtension(fpath, ".lz4");
		default:
			return HasExtension(fpath, ".snappy");
		}
	}

	unique_ptr<StreamWrapper> CreateStream() override {
		switch (codec) {
		case HiveCodec::BZIP2:
			return make_uniq<BZip2StreamWrapper>();
		case HiveCodec::LZ4:
			return make_uniq<HadoopBlockStreamWrapper>(HadoopBlockCodec::LZ4);
		default:
			return make_uniq<HadoopBlockStreamWrapper>(HadoopBlockCodec::SNAPPY);
		}
	}

	idx_t InBufferSize() override {
		return IN_BUFFER_SIZE;
	}
	idx_t OutBufferSize() override {
		return OUT_BUFFER_SIZE;
	}

private:
	HiveCodec codec;
};

//! A file handle owns the file system it decompresses with, like ZStdFile and GZipFile
struct HiveCompressionFileSystemHolder {
	explicit HiveCompressionFileSystemHolder(HiveCodec codec) : codec_file_system(codec) {
	}
	HiveCompressionFileSystem codec_file_system;
};

class HiveCompressedFile : private HiveCompressionFileSystemHolder, public CompressedFile {
public:
	HiveCompressedFile(HiveCodec codec, QueryContext context, unique_ptr<FileHandle> child_handle_p, const string &path)
	    : HiveCompressionFileSystemHolder(codec), CompressedFile(codec_file_system, std::move(child_handle_p), path) {
		Initialize(context, false);
	}

	FileCompressionType GetFileCompressionType() override {
		return codec_file_system.GetCompressionType();
	}
};

unique_ptr<FileHandle> HiveCompressionFileSystem::OpenCompressedFile(QueryContext context,
                                                                     unique_ptr<FileHandle> handle, bool write) {
	if (write) {
		throw NotImplementedException("Writing %s compressed files is not supported (%s)",
		                              GetCompressionType().ToString(), handle->path);
	}
	auto path = handle->path;
	return make_uniq<HiveCompressedFile>(codec, context, std::move(handle), path);
}

} // namespace

void RegisterHiveCompressionFileSystems(DatabaseInstance &db) {
	auto &fs = db.GetFileSystem();
	fs.RegisterCompressionFilesystem(make_uniq<HiveCompressionFileSystem>(HiveCodec::BZIP2));
	fs.RegisterCompressionFilesystem(make_uniq<HiveCompressionFileSystem>(HiveCodec::LZ4));
	fs.RegisterCompressionFilesystem(make_uniq<HiveCompressionFileSystem>(HiveCodec::SNAPPY));
}

} // namespace duckdb
