#include "oracle_table_entry.hpp"
#include "oracle_scan.hpp"
#include "oracle_utils.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include <memory>

namespace duckdb {

// ─── ファクトリ関数 ────────────────────────────────────────────────────────────

CreateTableInfo OracleTableInfoToCreateTableInfo(
    Catalog &catalog,
    const std::string &schema,
    const std::string &table,
    const std::vector<OracleColumnInfo> &columns) {

    CreateTableInfo info;
    info.schema    = schema;
    info.table     = table;
    info.temporary = false;

    for (const auto &col : columns) {
        LogicalType dtype = OracleTypeMapping::ToDuckDBType(col);
        ColumnDefinition cdef(col.name, dtype);
        // NULL 制約
        if (!col.nullable) {
            cdef.SetDefault(nullptr);
        }
        info.columns.AddColumn(std::move(cdef));
    }
    return info;
}

// ─── OracleTableEntry ─────────────────────────────────────────────────────────

OracleTableEntry::OracleTableEntry(Catalog &catalog, SchemaCatalogEntry &schema_entry,
                                    CreateTableInfo &info, OracleConnectionPool &pool)
    : TableCatalogEntry(catalog, schema_entry, info), pool_(pool) {

    // oracle_columns_ は呼び出し元が直接設定する
}

unique_ptr<NodeStatistics>
OracleTableEntry::GetStatistics(ClientContext &context, column_t column_id) {
    // Oracle の DBA_TABLES.NUM_ROWS を利用する将来実装用
    return make_uniq<NodeStatistics>();
}

TableFunction OracleTableEntry::GetScanFunction(ClientContext &context,
                                                  unique_ptr<FunctionData> &bind_data) {
    // Bind データを構築
    auto data = make_uniq<OracleScanBindData>();
    data->pool      = std::shared_ptr<OracleConnectionPool>(&pool_, [](auto *) {}); // non-owning
    data->schema    = schema.name;
    data->table     = name;
    data->all_columns = oracle_columns_;

    // 型リストを構築
    for (const auto &col : oracle_columns_) {
        data->all_types.push_back(OracleTypeMapping::ToDuckDBType(col));
    }

    // Oracle バージョンを取得
    auto conn = pool_.Acquire();
    data->oracle_major_version = conn->GetServerMajorVersion();
    pool_.Release(conn);

    bind_data = std::move(data);
    return OracleScan::GetFunction();
}

TableStorageInfo OracleTableEntry::GetStorageInfo(ClientContext &context) {
    TableStorageInfo info;
    info.cardinality = DConstants::INVALID_INDEX;
    return info;
}

unique_ptr<BaseStatistics>
OracleTableEntry::GetColumnStatistics(ClientContext &context, column_t column_id) {
    return nullptr;
}

} // namespace duckdb
