#include "planning/hive_query_cache.hpp"

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/client_context.hpp"
#include "api/glue_api.hpp"
#include "catalog/glue_catalog.hpp"
#include "catalog/glue_transaction.hpp"
#include "planning/hive_multi_file_reader.hpp"

namespace duckdb {

static constexpr const char *HIVE_QUERY_CACHE = "glue_hive_query_cache";
static constexpr const char *HIVE_LISTING_CACHE = "glue_listing_cache";

namespace {

//! The directory listings of Glue tables, kept across statements under glue_cache_listings
class HiveListingCache : public ClientContextState {
public:
	shared_ptr<MultiFileList> GetListing(ClientContext &context, const string &directory) {
		annotated_lock_guard<annotated_mutex> guard(lock);
		auto &listing = listings[directory];
		if (!listing) {
			auto &fs = FileSystem::GetFileSystem(context);
			listing = shared_ptr<MultiFileList>(fs.GlobFileList(directory + "/**", FileGlobOptions::ALLOW_EMPTY));
		}
		return listing;
	}

	void Clear() {
		annotated_lock_guard<annotated_mutex> guard(lock);
		listings.clear();
	}

	void QueryEnd(ClientContext &context) override {
		Value enabled;
		if (!context.TryGetCurrentSetting("glue_cache_listings", enabled) || enabled.IsNull() ||
		    !BooleanValue::Get(enabled)) {
			Clear();
		}
	}

	void TransactionCommit(MetaTransaction &transaction, ClientContext &context) override {
		if (!GlueTransactionCache::ConnectionScoped(context)) {
			Clear();
		}
	}

	void TransactionRollback(MetaTransaction &transaction, ClientContext &context) override {
		Clear();
	}

private:
	annotated_mutex lock;
	unordered_map<string, shared_ptr<MultiFileList>> listings DUCKDB_GUARDED_BY(lock);
};

} // namespace

void ClearHiveListingCache(ClientContext &context) {
	auto cache = context.registered_state->Get<HiveListingCache>(HIVE_LISTING_CACHE);
	if (cache) {
		cache->Clear();
	}
}

shared_ptr<HiveQueryCache> HiveQueryCache::Get(ClientContext &context) {
	return context.registered_state->GetOrCreate<HiveQueryCache>(HIVE_QUERY_CACHE);
}

string HiveQueryCache::TableKey(const string &catalog, const string &database, const string &table) {
	return PartitionValuesKey({catalog, database, table});
}

void HiveQueryCache::QueryEnd(ClientContext &context) {
	annotated_lock_guard<annotated_mutex> guard(lock);
	glue_tables.clear();
	samples.clear();
	directory_listings.clear();
}

shared_ptr<GlueTableEntry> HiveQueryCache::GetGlueTable(const string &key) {
	annotated_lock_guard<annotated_mutex> guard(lock);
	auto &entry = glue_tables[key];
	if (!entry) {
		entry = make_shared_ptr<GlueTableEntry>();
	}
	return entry;
}

shared_ptr<HiveTableSampleEntry> HiveQueryCache::GetSample(const string &table) {
	annotated_lock_guard<annotated_mutex> guard(lock);
	auto &entry = samples[table];
	if (!entry) {
		entry = make_shared_ptr<HiveTableSampleEntry>();
	}
	return entry;
}

shared_ptr<MultiFileList> HiveQueryCache::GetDirectoryListing(ClientContext &context, const string &directory,
                                                              bool cache_in_connection) {
	auto key = directory;
	StringUtil::RTrim(key, "/");
	if (cache_in_connection) {
		// the connection's listings (glue_cache_listings): kept for later statements, cleared with the metadata cache
		return context.registered_state->GetOrCreate<HiveListingCache>(HIVE_LISTING_CACHE)->GetListing(context, key);
	}
	annotated_lock_guard<annotated_mutex> guard(lock);
	auto &listing = directory_listings[key];
	if (!listing) {
		auto &fs = FileSystem::GetFileSystem(context);
		listing = shared_ptr<MultiFileList>(fs.GlobFileList(key + "/**", FileGlobOptions::ALLOW_EMPTY));
	}
	return listing;
}

//! Every partition of the table into 'entry', unless it has them already. Called with the entry's lock held.
static void LoadAllPartitions(ClientContext &context, const HiveScanInfo &info, GlueCatalog &catalog,
                              GlueTableEntry &entry) DUCKDB_REQUIRES(entry.lock) {
	if (entry.partitions) {
		return;
	}
	auto load = [&]() {
		return make_shared_ptr<const vector<GluePartitionInfo>>(
		    GlueAPI::GetPartitions(context, catalog, info.database_name, info.table_name));
	};
	auto cache = GlueTransactionCache::Of(context, catalog);
	entry.partitions = cache ? cache->GetPartitions(info.database_name, info.table_name, load) : load();
}

shared_ptr<const vector<GluePartitionInfo>> GetTablePartitions(ClientContext &context, const HiveScanInfo &info) {
	auto entry = HiveQueryCache::Get(context)->GetGlueTable(
	    HiveQueryCache::TableKey(info.catalog_name, info.database_name, info.table_name));
	// per table, so the Glue calls for different tables do not wait on each other
	annotated_lock_guard<annotated_mutex> guard(entry->lock);
	auto &catalog = Catalog::GetCatalog(context, Identifier(info.catalog_name)).Cast<GlueCatalog>();
	LoadAllPartitions(context, info, catalog, *entry);
	return entry->partitions;
}

bool TablePartitionsCached(ClientContext &context, const HiveScanInfo &info) {
	auto entry = HiveQueryCache::Get(context)->GetGlueTable(
	    HiveQueryCache::TableKey(info.catalog_name, info.database_name, info.table_name));
	annotated_lock_guard<annotated_mutex> guard(entry->lock);
	if (entry->partitions) {
		return true;
	}
	auto &catalog = Catalog::GetCatalog(context, Identifier(info.catalog_name)).Cast<GlueCatalog>();
	auto cache = GlueTransactionCache::Of(context, catalog);
	return cache && cache->CachedPartitions(info.database_name, info.table_name);
}

namespace {

//! What this connection learned about looking up only some partitions of a table, so that it does not ask again: the
//! lookups Glue refused or did not answer in full, the kinds of lookup Glue refused for the whole table (a principal
//! not allowed BatchGetPartition), and the tables whose partitions it can not tell apart by their locations, which are
//! fetched in full right away
class PartitionSubsetRejections : public ClientContextState {
public:
	bool Rejected(const string &key) {
		lock_guard<mutex> guard(lock);
		return keys.count(key) > 0;
	}
	void Add(const string &key) {
		lock_guard<mutex> guard(lock);
		keys.insert(key);
	}

private:
	mutex lock;
	unordered_set<string> keys;
};

} // namespace

//! False when 'input' does not decode to UTF-8 ('%FF')
static bool TryURLDecode(const string &input, string &result) {
	try {
		result = StringUtil::URLDecode(input);
		return true;
	} catch (std::exception &) {
		return false;
	}
}

//! The value each directory of the location of 'partition' below the table location spells, when that is the
//! directory its values name: '<key>=<value>' for each key in order, with the value as it is or escaped ('a%3Ab' for
//! 'a:b', as Hive writes it). Empty when the partition is stored elsewhere.
static vector<string> OwnDirectoryValues(const HiveScanInfo &info, const GluePartitionInfo &partition) {
	auto root = info.root_location;
	StringUtil::RTrim(root, "/");
	auto location = partition.location;
	StringUtil::RTrim(location, "/");
	if (root.empty() || !StringUtil::StartsWith(location, root + "/") ||
	    partition.values.size() != info.partition_keys.size()) {
		return {};
	}
	auto directories = StringUtil::Split(location.substr(root.size() + 1), '/');
	if (directories.size() != info.partition_keys.size()) {
		return {};
	}
	vector<string> result;
	for (idx_t i = 0; i < directories.size(); i++) {
		auto separator = directories[i].find('=');
		if (separator == string::npos) {
			return {};
		}
		auto name = directories[i].substr(0, separator);
		auto value = directories[i].substr(separator + 1);
		string decoded_name;
		string decoded;
		if (!TryURLDecode(name, decoded_name) || !StringUtil::CIEquals(decoded_name, info.partition_keys[i]) ||
		    (value != partition.values[i] && !(TryURLDecode(value, decoded) && decoded == partition.values[i]))) {
			return {};
		}
		result.push_back(std::move(value));
	}
	return result;
}

//! Why the files of 'partitions' could belong to partitions other than these when all are fetched (empty when they
//! can not): a partition not stored in the directory its values name below the table location (registered with a
//! location of its own, as a partition sharing another's is), or two of them on the same location. Which partition owns
//! a shared location depends on the order Glue lists all of them in.
static string PartitionSubsetLocationProblem(const HiveScanInfo &info, const vector<GluePartitionInfo> &partitions) {
	unordered_set<string> locations;
	for (auto &partition : partitions) {
		auto location = partition.location;
		StringUtil::RTrim(location, "/");
		if (location.empty()) {
			// no files, however the others are registered
			continue;
		}
		if (!locations.insert(location).second) {
			return StringUtil::Format("partitions share the location '%s'", location);
		}
		if (OwnDirectoryValues(info, partition).empty()) {
			return StringUtil::Format("partition [%s] is stored at '%s', not in the directory its values name",
			                          StringUtil::Join(partition.values, ", "), location);
		}
	}
	return string();
}

//! A value Glue can hold, and so a partition can have: at most 1024 bytes, without control characters but tab
static bool IsGluePartitionValue(const string &value) {
	if (value.size() > 1024) {
		return false;
	}
	for (auto c : value) {
		if (static_cast<uint8_t>(c) < 0x20 && c != '\t') {
			return false;
		}
	}
	return true;
}

//! The most keys of a partition with escaped directory names whose other spellings are looked up
static constexpr idx_t MAX_ESCAPED_PARTITION_KEYS = 4;

//! The values of the other partitions whose directory is that of 'partition', which is stored in the one its values
//! name ('directories', OwnDirectoryValues): a name with an escape ('a%3Ab') is the directory of the value it decodes
//! to ('a:b') and of the value spelled as it is. False when too many of its names are escaped.
static bool DirectoryAliases(const GluePartitionInfo &partition, const vector<string> &directories,
                             vector<vector<string>> &aliases) {
	vector<vector<string>> spellings;
	idx_t escaped = 0;
	for (idx_t i = 0; i < directories.size(); i++) {
		vector<string> key_spellings {partition.values[i]};
		string decoded;
		if (TryURLDecode(directories[i], decoded) && decoded != directories[i]) {
			auto other = partition.values[i] == directories[i] ? decoded : directories[i];
			if (IsGluePartitionValue(other)) {
				key_spellings.push_back(std::move(other));
				escaped++;
			}
		}
		spellings.push_back(std::move(key_spellings));
	}
	if (escaped > MAX_ESCAPED_PARTITION_KEYS) {
		return false;
	}
	// every combination of the spellings but the partition's own
	vector<idx_t> positions(spellings.size(), 0);
	while (true) {
		idx_t key_index = positions.size();
		while (key_index > 0) {
			key_index--;
			if (++positions[key_index] < spellings[key_index].size()) {
				break;
			}
			positions[key_index] = 0;
			if (key_index == 0) {
				return true;
			}
		}
		if (positions.empty()) {
			return true;
		}
		vector<string> alias;
		for (idx_t i = 0; i < positions.size(); i++) {
			alias.push_back(spellings[i][positions[i]]);
		}
		aliases.push_back(std::move(alias));
	}
}

//! How CheckEscapedDirectories ended
enum class EscapedDirectoryCheck : uint8_t {
	//! 'result' is the subset
	CHECKED,
	//! the other spellings of the directories can not be looked up: one can not be asked for, Glue refuses or denies
	//! the lookup, or is busy
	UNCHECKED,
	//! looking them up failed with another Glue error, which may not last (an expired token, a table that is gone)
	FAILED,
	//! looking them up would take several requests one after another (PartitionSubsetProbes::sequential_requests):
	//! 'result' is what the lookup found
	LIST_ALL
};

//! Whether the partitions a lookup found ('result') are told apart from those it did not find by their escaped
//! directory names: for each found stored under a name with an escape, the partitions with the other spellings of its
//! directory (DirectoryAliases) are found too, known not to exist (the lookup asked for their values), or looked up
//! with 'probes.probe'. One of those registered at the location of a partition found is kept in 'result', where
//! PartitionSubsetLocationProblem finds that the two share it; the others the lookup found only for the probes are
//! dropped.
static EscapedDirectoryCheck CheckEscapedDirectories(ClientContext &context, const HiveScanInfo &info,
                                                     GlueCatalog &catalog, const PartitionSubsetProbes &probes,
                                                     vector<GluePartitionInfo> &result) {
	unordered_set<string> needed;
	if (probes.values) {
		for (auto &values : *probes.values) {
			needed.insert(PartitionValuesKey(values));
		}
	}
	// the values whose partitions are in 'result' if they exist
	auto known = needed;
	for (auto &values : probes.extra) {
		known.insert(PartitionValuesKey(values));
	}
	vector<GluePartitionInfo> subset;
	vector<GluePartitionInfo> others;
	unordered_set<string> subset_keys;
	for (auto &partition : result) {
		auto key = PartitionValuesKey(partition.values);
		known.insert(key);
		if (!probes.values || needed.count(key)) {
			subset_keys.insert(key);
			subset.push_back(std::move(partition));
		} else {
			others.push_back(std::move(partition));
		}
	}
	vector<vector<string>> unknown;
	unordered_set<string> unknown_keys;
	for (auto &partition : subset) {
		auto directories = OwnDirectoryValues(info, partition);
		if (directories.empty()) {
			// stored elsewhere, which PartitionSubsetLocationProblem finds
			continue;
		}
		vector<vector<string>> aliases;
		if (!DirectoryAliases(partition, directories, aliases)) {
			return EscapedDirectoryCheck::UNCHECKED;
		}
		for (auto &alias : aliases) {
			auto key = PartitionValuesKey(alias);
			if (!known.count(key) && unknown_keys.insert(key).second) {
				unknown.push_back(std::move(alias));
			}
		}
	}
	if (!unknown.empty()) {
		if (!probes.probe) {
			return EscapedDirectoryCheck::UNCHECKED;
		}
		if (probes.sequential_requests && probes.sequential_requests(unknown) > 1) {
			result = std::move(subset);
			result.insert(result.end(), std::make_move_iterator(others.begin()), std::make_move_iterator(others.end()));
			return EscapedDirectoryCheck::LIST_ALL;
		}
		vector<GluePartitionInfo> found;
		try {
			if (!probes.probe(catalog, unknown, found)) {
				return EscapedDirectoryCheck::UNCHECKED;
			}
		} catch (std::exception &ex) {
			ErrorData error(ex);
			if (error.Type() == ExceptionType::INTERRUPT || error.Type() == ExceptionType::FATAL ||
			    error.Type() == ExceptionType::INTERNAL) {
				throw;
			}
			DUCKDB_LOG_INFO(context,
			                "Glue could not be asked for the other spellings of escaped partition directories of "
			                "'%s.%s': %s",
			                info.database_name, info.table_name, error.RawMessage());
			return error.Type() == ExceptionType::INVALID_INPUT || error.Type() == ExceptionType::PERMISSION
			           ? EscapedDirectoryCheck::UNCHECKED
			           : EscapedDirectoryCheck::FAILED;
		}
		for (auto &partition : found) {
			if (!subset_keys.count(PartitionValuesKey(partition.values))) {
				others.push_back(std::move(partition));
			}
		}
	}
	unordered_set<string> locations;
	for (auto &partition : subset) {
		auto location = partition.location;
		StringUtil::RTrim(location, "/");
		locations.insert(std::move(location));
	}
	for (auto &partition : others) {
		auto location = partition.location;
		StringUtil::RTrim(location, "/");
		if (!location.empty() && locations.count(location)) {
			subset.push_back(std::move(partition));
		}
	}
	result = std::move(subset);
	return EscapedDirectoryCheck::CHECKED;
}

shared_ptr<const vector<GluePartitionInfo>> GetTablePartitionSubset(ClientContext &context, const HiveScanInfo &info,
                                                                    const string &key,
                                                                    const PartitionSubsetLoader &load, bool &subset,
                                                                    const string &lookup,
                                                                    const PartitionSubsetProbes &probes) {
	auto table_key = HiveQueryCache::TableKey(info.catalog_name, info.database_name, info.table_name);
	auto entry = HiveQueryCache::Get(context)->GetGlueTable(table_key);
	annotated_lock_guard<annotated_mutex> guard(entry->lock);
	subset = false;
	auto &catalog = Catalog::GetCatalog(context, Identifier(info.catalog_name)).Cast<GlueCatalog>();
	auto cache = GlueTransactionCache::Of(context, catalog);
	if (!entry->partitions && cache) {
		entry->partitions = cache->CachedPartitions(info.database_name, info.table_name);
	}
	if (entry->partitions) {
		// all partitions known already: nothing to ask Glue
		return entry->partitions;
	}
	auto rejections =
	    context.registered_state->GetOrCreate<PartitionSubsetRejections>("glue_partition_subset_rejections");
	// by the Glue catalog, not its alias: one attached again under the same name may be another
	auto table = PartitionValuesKey(
	    {catalog.AttachIdentity(), StringUtil::Lower(info.database_name), StringUtil::Lower(info.table_name)});
	auto locations_key = PartitionValuesKey({table, "locations"});
	auto table_refusal_key = PartitionValuesKey({table, "refused", lookup});
	auto lookup_key = PartitionValuesKey({table, "lookup", key});
	// a lookup of this kind found a partition whose escaped directory could not be checked
	auto unchecked_key = PartitionValuesKey({table, "unchecked", lookup});
	if (rejections->Rejected(locations_key) || rejections->Rejected(unchecked_key)) {
		LoadAllPartitions(context, info, catalog, *entry);
		return entry->partitions;
	}
	if (rejections->Rejected(table_refusal_key) || rejections->Rejected(lookup_key) ||
	    entry->failed_lookups.count(lookup)) {
		return nullptr;
	}
	auto &partitions = entry->partition_subsets[key];
	if (!partitions) {
		auto check = EscapedDirectoryCheck::CHECKED;
		vector<GluePartitionInfo> found;
		auto load_subset = [&]() -> shared_ptr<const vector<GluePartitionInfo>> {
			vector<GluePartitionInfo> result;
			if (!load(catalog, result)) {
				return nullptr;
			}
			check = CheckEscapedDirectories(context, info, catalog, probes, result);
			if (check != EscapedDirectoryCheck::CHECKED) {
				found = std::move(result);
				return nullptr;
			}
			return make_shared_ptr<const vector<GluePartitionInfo>>(std::move(result));
		};
		try {
			partitions =
			    cache ? cache->GetPartitions(info.database_name, info.table_name, load_subset, key) : load_subset();
		} catch (std::exception &ex) {
			ErrorData error(ex);
			switch (error.Type()) {
			case ExceptionType::INVALID_INPUT:
				// a value or an expression Glue does not accept
				rejections->Add(lookup_key);
				break;
			case ExceptionType::PERMISSION:
				// the principal may not look partitions up this way
			case ExceptionType::INVALID_TYPE:
				// Glue can not compare a value the table stores with the lookup (text that is not a number for an
				// integer key), which another lookup of this kind would ask about again
				rejections->Add(table_refusal_key);
				break;
			case ExceptionType::IO:
			case ExceptionType::HTTP:
				// any other Glue error (an expired token, a table that is gone, a timeout) may not last: fetching all
				// partitions fails the same way if it does
				entry->failed_lookups.insert(lookup);
				break;
			default:
				throw;
			}
			DUCKDB_LOG_INFO(context, "Glue refused to look up only some partitions of '%s.%s': %s", info.database_name,
			                info.table_name, error.RawMessage());
			return nullptr;
		}
		if (check == EscapedDirectoryCheck::UNCHECKED || check == EscapedDirectoryCheck::FAILED) {
			if (check == EscapedDirectoryCheck::UNCHECKED) {
				// every later lookup of this kind would look up, check and fetch all partitions again
				rejections->Add(unchecked_key);
			}
			DUCKDB_LOG_INFO(context,
			                "Fetching all partitions of '%s.%s': a partition looked up is stored under an escaped "
			                "directory name, which also names other values, whose partitions could not be looked up",
			                info.database_name, info.table_name);
			LoadAllPartitions(context, info, catalog, *entry);
			return entry->partitions;
		}
		if (check == EscapedDirectoryCheck::LIST_ALL) {
			// one listing answers the check, where the probes would wait for one request after the other
			LoadAllPartitions(context, info, catalog, *entry);
			auto all = entry->partitions;
			auto listed = probes;
			listed.sequential_requests = nullptr;
			listed.probe = [&all](GlueCatalog &, const vector<vector<string>> &values,
			                      vector<GluePartitionInfo> &result) {
				unordered_set<string> keys;
				for (auto &partition_values : values) {
					keys.insert(PartitionValuesKey(partition_values));
				}
				for (auto &partition : *all) {
					if (keys.count(PartitionValuesKey(partition.values))) {
						result.push_back(partition);
					}
				}
				return true;
			};
			string problem = "the other spellings of its escaped directory names could not be checked";
			if (CheckEscapedDirectories(context, info, catalog, listed, found) == EscapedDirectoryCheck::CHECKED) {
				problem = PartitionSubsetLocationProblem(info, found);
			}
			if (!problem.empty()) {
				// as when the probes find it
				rejections->Add(locations_key);
			}
			DUCKDB_LOG_INFO(context,
			                "Fetching all partitions of '%s.%s': a partition looked up is stored under an escaped "
			                "directory name, which also names other values, too many to look up in one request%s%s",
			                info.database_name, info.table_name, problem.empty() ? "" : ", and ", problem);
			return entry->partitions;
		}
		if (!partitions) {
			// throttled, failed on Glue's side or left unprocessed after the retries: Glue is busy, and asking it the
			// same way for other values of the table would wait out the same back-offs
			rejections->Add(table_refusal_key);
			DUCKDB_LOG_INFO(context, "Glue did not answer the lookup of only some partitions of '%s.%s' in full",
			                info.database_name, info.table_name);
			return nullptr;
		}
	}
	auto problem = PartitionSubsetLocationProblem(info, *partitions);
	if (problem.empty()) {
		subset = true;
		return partitions;
	}
	rejections->Add(locations_key);
	DUCKDB_LOG_INFO(
	    context,
	    "Fetching all partitions of '%s.%s': %s, and a partition the scan does not need could be registered "
	    "there too",
	    info.database_name, info.table_name, problem);
	LoadAllPartitions(context, info, catalog, *entry);
	return entry->partitions;
}
//! The cache key of a lookup by values, never empty (the key of all partitions in the transaction cache)
static string PinnedPartitionsKey(const vector<vector<string>> &values) {
	vector<string> partitions;
	for (auto &partition : values) {
		partitions.push_back(PartitionValuesKey(partition));
	}
	return PartitionValuesKey({"BatchGetPartition", PartitionValuesKey(partitions)});
}

//! 'value' with the escapes Hive gives a partition directory name (FileUtils.escapePathName: 'a%3Ab' for 'a:b'), as
//! Hive and Spark write it
static string HiveEscapedValue(const string &value) {
	static const char *HEX_DIGIT = "0123456789ABCDEF";
	static const string ESCAPED = "\"#%'*/:=?\\{[]^";
	string result;
	for (auto c : value) {
		auto byte = static_cast<uint8_t>(c);
		if (byte < 0x20 || byte == 0x7F || ESCAPED.find(c) != string::npos) {
			result += '%';
			result += HEX_DIGIT[byte >> 4];
			result += HEX_DIGIT[byte & 15];
		} else {
			result += c;
		}
	}
	return result;
}

//! The values of the partitions a lookup of 'values' asks for alongside them: the other spellings a writer may give
//! their directories, which is also the directory of a partition with those values (DirectoryAliases). Each value
//! escaped as Hive and as a URL escape it, and decoded when it holds escapes, at most two of them per value asked for:
//! asked for in the same requests, they spare GetTablePartitionSubset another one when a partition's directory name
//! has an escape (a Hive timestamp, '2024-01-01 00%3A00%3A00')
static vector<vector<string>> OtherDirectorySpellings(const vector<vector<string>> &values) {
	unordered_set<string> seen;
	for (auto &partition : values) {
		seen.insert(PartitionValuesKey(partition));
	}
	vector<vector<string>> result;
	for (auto &partition : values) {
		for (idx_t i = 0; i < partition.size(); i++) {
			vector<string> spellings {HiveEscapedValue(partition[i]), StringUtil::URLEncode(partition[i])};
			string decoded;
			if (TryURLDecode(partition[i], decoded)) {
				spellings.push_back(std::move(decoded));
			}
			for (auto &spelling : spellings) {
				if (spelling == partition[i] || !IsGluePartitionValue(spelling)) {
					continue;
				}
				auto other = partition;
				other[i] = spelling;
				idx_t bytes = 0;
				for (auto &value : other) {
					bytes += value.size();
				}
				if (bytes > GlueAPI::MAX_LOOKUP_PARTITION_BYTES) {
					// looked up with the probes, when its directory turns out to be escaped
					continue;
				}
				if (seen.insert(PartitionValuesKey(other)).second) {
					result.push_back(std::move(other));
				}
			}
		}
	}
	if (result.size() > 2 * values.size()) {
		// too many to ask for each time: looked up when a directory name turns out to be escaped
		return {};
	}
	return result;
}

shared_ptr<const vector<GluePartitionInfo>> GetPinnedTablePartitions(ClientContext &context, const HiveScanInfo &info,
                                                                     const vector<vector<string>> &values,
                                                                     bool &pinned) {
	PartitionSubsetProbes probes;
	probes.values = &values;
	probes.extra = OtherDirectorySpellings(values);
	probes.probe = [&](GlueCatalog &catalog, const vector<vector<string>> &others, vector<GluePartitionInfo> &result) {
		return GlueAPI::BatchGetPartitions(context, catalog, info.database_name, info.table_name, others, result);
	};
	return GetTablePartitionSubset(
	    context, info, PinnedPartitionsKey(values),
	    [&](GlueCatalog &catalog, vector<GluePartitionInfo> &result) {
		    if (probes.extra.empty()) {
			    return GlueAPI::BatchGetPartitions(context, catalog, info.database_name, info.table_name, values,
			                                       result);
		    }
		    auto asked = values;
		    asked.insert(asked.end(), probes.extra.begin(), probes.extra.end());
		    return GlueAPI::BatchGetPartitions(context, catalog, info.database_name, info.table_name, asked, result);
	    },
	    pinned, "BatchGetPartition", probes);
}

} // namespace duckdb
