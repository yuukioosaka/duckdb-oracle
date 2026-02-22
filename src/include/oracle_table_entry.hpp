#pragma once

#include "duckdb.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "oracle_connection.hpp"
#include "oracle_type_mapping.hpp"

namespace duckdb {

// ───────────────────────────────────────────────────────────────────────────────
// OracleTableEntry
// ───────────────────────────────────────────────────────────────────────────────
class OracleTableEntry : public TableCatalogEntry {
public:
    OracleTableEntry(Catalog &catalog, SchemaCatalogEntry &schema,
                      CreateTableInfo &info, OracleConnectionPool &pool);

    // ─── スキャン ──────────────────────────────────────────────────────────────
    unique_ptr<NodeStatistics> GetStatistics(ClientContext &context,
                                              column_t column_id) override;

    TableFunction GetScanFunction(ClientContext &context,
                                  unique_ptr<FunctionData> &bind_data) override;

    // ─── INSERT / UPDATE / DELETE ──────────────────────────────────────────────
    TableStorageInfo GetStorageInfo(ClientContext &context) override;
    unique_ptr<BaseStatistics>
        GetColumnStatistics(ClientContext &context, column_t column_id) override;

    // ─── カラムメタ ────────────────────────────────────────────────────────────
    const std::vector<OracleColumnInfo> &GetOracleColumns() const {
        return oracle_columns_;
    }

private:
    OracleConnectionPool &pool_;
    std::vector<OracleColumnInfo> oracle_columns_;
};

// テーブル情報を Oracle から読み取って CreateTableInfo を構築する
CreateTableInfo OracleTableInfoToCreateTableInfo(
    Catalog &catalog,
    const std::string &schema,
    const std::string &table,
    const std::vector<OracleColumnInfo> &columns);

} // namespace duckdb
