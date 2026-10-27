#include "catalog/glue_write_transaction.hpp"

#include "catalog/glue_catalog.hpp"
#include "catalog/glue_schema_entry.hpp"
#include "catalog/glue_transaction.hpp"

#include "duckdb/common/atomic.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/client_context.hpp"

#include <thread>

namespace duckdb {

string GlueTableWrite::FinalPath(const string &file) const {
	if (staging_root.empty() || !StringUtil::StartsWith(file, staging_root + "/")) {
		return file;
	}
	return location + file.substr(staging_root.size());
}

GlueTableWrite &GlueWriteTransaction::AddWrite(unique_ptr<GlueTableWrite> write) {
	lock_guard<mutex> guard(lock);
	writes.push_back(std::move(write));
	return *writes.back();
}

bool GlueWriteTransaction::HasWrites() {
	lock_guard<mutex> guard(lock);
	return !writes.empty();
}

//! S3 has no rename: a staged file is published by copying its bytes to the final key
static void CopyFileContents(FileSystem &fs, const string &from, const string &to) {
	auto source = fs.OpenFile(from, FileFlags::FILE_FLAGS_READ);
	auto target = fs.OpenFile(to, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE_NEW);
	constexpr idx_t BUFFER_SIZE = 8ULL << 20ULL;
	auto buffer = make_unsafe_uniq_array<data_t>(BUFFER_SIZE);
	auto size = NumericCast<idx_t>(source->GetFileSize());
	for (idx_t offset = 0; offset < size;) {
		auto count = MinValue<idx_t>(BUFFER_SIZE, size - offset);
		source->Read(buffer.get(), count, offset);
		target->Write(buffer.get(), count);
		offset += count;
	}
	target->Sync();
	target->Close();
}

static void PublishStagedFiles(FileSystem &fs, GlueTableWrite &write) {
	vector<string> staged;
	for (auto &file : write.files) {
		if (write.FinalPath(file) != file) {
			staged.push_back(file);
		}
	}
	if (staged.empty()) {
		return;
	}
	mutex publish_lock;
	ErrorData first_error;
	atomic<idx_t> next {0};
	auto worker = [&]() {
		while (true) {
			auto index = next++;
			if (index >= staged.size()) {
				return;
			}
			auto final_path = write.FinalPath(staged[index]);
			try {
				CopyFileContents(fs, staged[index], final_path);
				lock_guard<mutex> guard(publish_lock);
				write.published_files.push_back(final_path);
			} catch (std::exception &ex) {
				lock_guard<mutex> guard(publish_lock);
				// the target may exist partially
				write.published_files.push_back(final_path);
				if (!first_error.HasError()) {
					first_error = ErrorData(ex);
				}
				return;
			}
		}
	};
	auto thread_count = MinValue<idx_t>(16, staged.size());
	vector<std::thread> threads;
	for (idx_t i = 1; i < thread_count; i++) {
		threads.emplace_back(worker);
	}
	worker();
	for (auto &thread : threads) {
		thread.join();
	}
	if (first_error.HasError()) {
		throw IOException("Publishing a staged file failed: %s", first_error.RawMessage());
	}
}

static void RemoveFilesBestEffort(FileSystem &fs, const vector<string> &files) {
	if (files.empty()) {
		return;
	}
	try {
		fs.RemoveFiles(files);
	} catch (std::exception &) {
		for (auto &file : files) {
			fs.TryRemoveFile(file);
		}
	}
}

void GlueWriteTransaction::Commit(ClientContext &context, GlueCatalog &catalog, GlueTransactionCache &cache) {
	lock_guard<mutex> guard(lock);
	if (writes.empty()) {
		return;
	}
	auto &fs = FileSystem::GetFileSystem(context);
	try {
		for (auto &write_p : writes) {
			auto &write = *write_p;
			if (!write.complete) {
				throw TransactionException("Hive write into '%s.%s' did not complete", write.schema_name,
				                           write.table_name);
			}
			// CTAS overwrite: the old files go only once every new file is written
			if (!write.replaced_files.empty()) {
				fs.RemoveFiles(write.replaced_files);
				write.replaced_files.clear();
			}
			if (!write.staging_root.empty()) {
				PublishStagedFiles(fs, write);
			}
			if (write.create_table) {
				write.create_schema->CommitCreateTable(context, *write.create_table);
				write.table_created = true;
			}
			if (!write.partitions.empty()) {
				vector<idx_t> created;
				try {
					GlueAPI::BatchCreatePartitions(context, catalog, write.schema_name, write.table_name,
					                               write.partitions, &created);
				} catch (std::exception &) {
					for (auto index : created) {
						write.created_partitions.push_back(write.partitions[index].values);
					}
					throw;
				}
				for (auto index : created) {
					write.created_partitions.push_back(write.partitions[index].values);
				}
			}
		}
	} catch (std::exception &ex) {
		ErrorData error(ex);
		auto undo_error = RollbackInternal(context, catalog, cache);
		if (undo_error.HasError()) {
			throw IOException("Hive write failed at commit: %s. Undoing it failed too: %s", error.RawMessage(),
			                  undo_error.RawMessage());
		}
		throw IOException("Hive write was rolled back at commit: %s. Its files were deleted and its Glue changes "
		                  "undone, so it can be run again",
		                  error.RawMessage());
	}
	for (auto &write : writes) {
		if (write->staging_root.empty()) {
			continue;
		}
		vector<string> staged;
		for (auto &file : write->files) {
			if (write->FinalPath(file) != file) {
				staged.push_back(file);
			}
		}
		// what stays behind is hidden from readers
		RemoveFilesBestEffort(fs, staged);
	}
	writes.clear();
	cache.Clear();
	GlueTransactionCache::Invalidate(context, catalog);
}

ErrorData GlueWriteTransaction::Rollback(ClientContext &context, GlueCatalog &catalog, GlueTransactionCache &cache) {
	lock_guard<mutex> guard(lock);
	return RollbackInternal(context, catalog, cache);
}

ErrorData GlueWriteTransaction::RollbackInternal(ClientContext &context, GlueCatalog &catalog,
                                                 GlueTransactionCache &cache) {
	auto to_undo = std::move(writes);
	writes.clear();
	ErrorData result;
	auto &fs = FileSystem::GetFileSystem(context);
	for (idx_t i = to_undo.size(); i > 0; i--) {
		auto &write = *to_undo[i - 1];
		try {
			if (write.table_created) {
				// the partitions go with the table
				write.create_schema->RollbackCreateTable(context, write.table_name);
			} else if (!write.created_partitions.empty()) {
				GlueAPI::BatchDeletePartitions(context, catalog, write.schema_name, write.table_name,
				                               write.created_partitions);
			}
		} catch (std::exception &ex) {
			result.Merge(ErrorData(ex));
		}
		vector<string> files = write.files;
		files.insert(files.end(), write.published_files.begin(), write.published_files.end());
		try {
			RemoveFilesBestEffort(fs, files);
		} catch (std::exception &ex) {
			result.Merge(ErrorData(ex));
		}
	}
	if (!to_undo.empty()) {
		try {
			cache.Clear();
			GlueTransactionCache::Invalidate(context, catalog);
		} catch (std::exception &ex) {
			result.Merge(ErrorData(ex));
		}
	}
	return result;
}

} // namespace duckdb
