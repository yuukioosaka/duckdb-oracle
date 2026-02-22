#pragma once

#include "duckdb.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "oracle_connection.hpp"

namespace duckdb {

class OracleCatalog;

// ───────────────────────────────────────────────────────────────────────────────
// OracleSchemaEntry: Oracle の「ユーザー/スキーマ」に対応
// ───────────────────────────────────────────────────────────────────────────────
class OracleSchemaEntry : public SchemaCatalogEntry {
public:
    OracleSchemaEntry(Catalog &catalog, CreateSchemaInfo &info,
                       OracleConnectionPool &pool);

    // ─── テーブルエントリ取得 ──────────────────────────────────────────────────
    optional_ptr<CatalogEntry> GetEntry(CatalogTransaction transaction,
                                         CatalogType type,
                                         const string &name) override;

    void Scan(ClientContext &context, CatalogType type,
              std::function<void(CatalogEntry &)> callback) override;

    // ─── DDL（書き込みモード時に有効） ────────────────────────────────────────
    optional_ptr<CatalogEntry>
        CreateTable(CatalogTransaction transaction,
                    BoundCreateTableInfo &info) override;
    void DropEntry(ClientContext &context, DropInfo &info) override;

    // 書き込み不要な操作はデフォルト拒否
    optional_ptr<CatalogEntry>
        CreateIndex(CatalogTransaction transaction,
                    CreateIndexInfo &info,
                    TableCatalogEntry &table) override;

private:
    OracleConnectionPool &pool_;

    // テーブル名 → CatalogEntry のローカルキャッシュ
    unordered_map<string, unique_ptr<CatalogEntry>> table_cache_;
    mutex cache_mutex_;

    optional_ptr<CatalogEntry> GetOrLoadTable(const string &table_name);
};

} // namespace duckdb
