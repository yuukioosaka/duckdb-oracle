#pragma once

#include "duckdb.hpp"
#include "duckdb/storage/storage_extension.hpp"

namespace duckdb {

// ───────────────────────────────────────────────────────────────────────────────
// OracleStorageExtension: DuckDB に StorageExtension として登録するエントリ
// ───────────────────────────────────────────────────────────────────────────────
class OracleStorageExtension : public StorageExtension {
public:
    OracleStorageExtension();
};

} // namespace duckdb
