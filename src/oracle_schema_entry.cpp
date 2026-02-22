#include "oracle_schema_entry.hpp"
#include "oracle_table_entry.hpp"
#include "oracle_catalog.hpp"
#include "oracle_utils.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"

namespace duckdb {

OracleSchemaEntry::OracleSchemaEntry(Catalog &catalog, CreateSchemaInfo &info,
                                       OracleConnectionPool &pool)
    : SchemaCatalogEntry(catalog, info), pool_(pool) {}

// ─── GetOrLoadTable ───────────────────────────────────────────────────────────

optional_ptr<CatalogEntry>
OracleSchemaEntry::GetOrLoadTable(const std::string &table_name) {
    std::string upper_name = OracleUtils::ToUpper(table_name);
    {
        std::lock_guard<std::mutex> lk(cache_mutex_);
        auto it = table_cache_.find(upper_name);
        if (it != table_cache_.end()) {
            return it->second.get();
        }
    }

    // Oracle から列情報をロード
    auto conn = pool_.Acquire();
    auto columns = conn->GetColumns(name, upper_name);
    pool_.Release(conn);

    if (columns.empty()) {
        return nullptr; // テーブルが存在しない
    }

    auto create_info = OracleTableInfoToCreateTableInfo(catalog, name, upper_name, columns);
    auto entry = make_uniq<OracleTableEntry>(catalog, *this, create_info, pool_);
    entry->oracle_columns_ = std::move(columns);

    std::lock_guard<std::mutex> lk(cache_mutex_);
    auto *raw = entry.get();
    table_cache_[upper_name] = std::move(entry);
    return raw;
}

// ─── GetEntry ─────────────────────────────────────────────────────────────────

optional_ptr<CatalogEntry>
OracleSchemaEntry::GetEntry(CatalogTransaction transaction,
                             CatalogType type, const string &entry_name) {
    if (type != CatalogType::TABLE_ENTRY && type != CatalogType::VIEW_ENTRY) {
        return nullptr;
    }
    return GetOrLoadTable(entry_name);
}

// ─── Scan ─────────────────────────────────────────────────────────────────────

void OracleSchemaEntry::Scan(ClientContext &context, CatalogType type,
                              std::function<void(CatalogEntry &)> callback) {
    if (type != CatalogType::TABLE_ENTRY && type != CatalogType::VIEW_ENTRY) {
        return;
    }

    auto conn = pool_.Acquire();
    auto tables = conn->GetTables(name);
    pool_.Release(conn);

    for (const auto &tbl : tables) {
        auto entry_ptr = GetOrLoadTable(tbl.name);
        if (entry_ptr) {
            callback(*entry_ptr);
        }
    }
}

// ─── CreateTable ──────────────────────────────────────────────────────────────

optional_ptr<CatalogEntry>
OracleSchemaEntry::CreateTable(CatalogTransaction transaction,
                                BoundCreateTableInfo &info) {
    // Oracle にテーブルを作成し、キャッシュに追加
    std::ostringstream ddl;
    ddl << "CREATE TABLE " << OracleUtils::QuoteIdentifier(name)
        << "." << OracleUtils::QuoteIdentifier(info.Base().table) << " (";

    const auto &columns = info.Base().columns;
    for (idx_t i = 0; i < columns.LogicalColumnCount(); ++i) {
        const auto &col = columns.GetColumn(LogicalIndex(i));
        if (i > 0) ddl << ", ";
        ddl << OracleUtils::QuoteIdentifier(col.GetName())
            << " " << OracleTypeMapping::ToOracleType(col.GetType());
        if (col.HasDefaultValue() == false) {
            // NULL NOT NULL
        }
    }
    ddl << ")";

    auto conn = pool_.Acquire();
    conn->ExecuteDML(ddl.str());
    pool_.Release(conn);

    return GetOrLoadTable(info.Base().table);
}

// ─── DropEntry ────────────────────────────────────────────────────────────────

void OracleSchemaEntry::DropEntry(ClientContext &context, DropInfo &info) {
    std::string upper_name = OracleUtils::ToUpper(info.name);
    std::string sql = "DROP TABLE " + OracleUtils::QuoteIdentifier(name)
                    + "." + OracleUtils::QuoteIdentifier(upper_name);
    if (info.if_exists) sql += " PURGE";

    auto conn = pool_.Acquire();
    conn->ExecuteDML(sql);
    pool_.Release(conn);

    // キャッシュから削除
    std::lock_guard<std::mutex> lk(cache_mutex_);
    table_cache_.erase(upper_name);
}

// ─── CreateIndex (stub) ───────────────────────────────────────────────────────

optional_ptr<CatalogEntry>
OracleSchemaEntry::CreateIndex(CatalogTransaction transaction,
                                CreateIndexInfo &info,
                                TableCatalogEntry &table) {
    throw NotImplementedException("Oracle extension: CREATE INDEX is not yet supported");
}

} // namespace duckdb
