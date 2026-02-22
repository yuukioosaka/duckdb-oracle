#pragma once

#include "duckdb.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/transaction/transaction_manager.hpp"
#include "oracle_connection.hpp"

namespace duckdb {

// ───────────────────────────────────────────────────────────────────────────────
// OracleCatalog: DuckDB Catalog の Oracle 実装
// ───────────────────────────────────────────────────────────────────────────────
class OracleCatalog : public Catalog {
public:
    explicit OracleCatalog(AttachedDatabase &db,
                            const OracleConnectionParameters &params);
    ~OracleCatalog() override;

    // StorageExtension の attach コールバックから呼ばれる
    static unique_ptr<Catalog> Attach(StorageExtensionInfo *info,
                                       ClientContext &context,
                                       AttachedDatabase &db,
                                       const string &name,
                                       AttachInfo &attach_info,
                                       AccessMode access_mode);

    // ─── Catalog 仮想関数 ──────────────────────────────────────────────────────
    string GetCatalogType() override { return "oracle"; }
    void Initialize(bool load_builtin) override;
    optional_ptr<CatalogEntry> GetEntryInternal(CatalogTransaction transaction,
                                                 CatalogType type,
                                                 const string &schema,
                                                 const string &name) override;
    optional_ptr<SchemaCatalogEntry>
        GetSchema(CatalogTransaction transaction,
                  const string &schema_name,
                  OnEntryNotFound on_not_found,
                  QueryErrorContext error_context = QueryErrorContext()) override;

    void ScanSchemas(ClientContext &context,
                     std::function<void(SchemaCatalogEntry &)> callback) override;
    bool InMemory() override { return false; }
    string GetDBPath() override { return ""; }

    // ─── 接続 & キャッシュ ─────────────────────────────────────────────────────
    OracleConnectionPool &GetConnectionPool() { return *pool_; }
    void ClearCache();

    // ─── スキーマキャッシュ ────────────────────────────────────────────────────
    void PreloadSchema(const string &schema);

private:
    OracleConnectionParameters params_;
    unique_ptr<OracleConnectionPool> pool_;

    // スキーマエントリキャッシュ
    unordered_map<string, unique_ptr<SchemaCatalogEntry>> schema_cache_;
    mutex cache_mutex_;

    optional_ptr<SchemaCatalogEntry> GetSchemaFromCache(const string &schema_name);
    unique_ptr<SchemaCatalogEntry>   CreateSchemaEntry(const string &schema_name);
};

// ───────────────────────────────────────────────────────────────────────────────
// OracleTransactionManager（DDL 非対応・読み取りのみ時はダミーでよい）
// ───────────────────────────────────────────────────────────────────────────────
class OracleTransaction : public Transaction {
public:
    OracleTransaction(TransactionManager &manager, ClientContext &context);
    ~OracleTransaction() override = default;
};

class OracleTransactionManager : public TransactionManager {
public:
    explicit OracleTransactionManager(AttachedDatabase &db_p, OracleCatalog &catalog);
    static unique_ptr<TransactionManager> Create(StorageExtensionInfo *info,
                                                  AttachedDatabase &db,
                                                  Catalog &catalog);

    Transaction &StartTransaction(ClientContext &context) override;
    ErrorData    CommitTransaction(ClientContext &context, Transaction &transaction) override;
    void         RollbackTransaction(Transaction &transaction) override;
    void         Checkpoint(ClientContext &context, bool force = false) override {}

private:
    OracleCatalog &catalog_;
};

} // namespace duckdb
