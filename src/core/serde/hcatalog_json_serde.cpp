#include "core/serde/hive_serde.hpp"

namespace duckdb {

HCatalogJSONSerDe::HCatalogJSONSerDe()
    : HiveJSONSerDe(HiveSerDeType::HIVE_JSON, HiveFileFormat::JSON, "JsonSerDe",
                    "org.apache.hive.hcatalog.data.JsonSerDe", "json") {
}

HiveJSONDialect HCatalogJSONSerDe::ReadDialect(const GlueTableInfo &table) const {
	HiveJSONDialect dialect;
	NoteUnsupported(table, "timestamp.formats", false, dialect);
	return dialect;
}

} // namespace duckdb
