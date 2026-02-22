#include "oracle_catalog.hpp"
#include "oracle_schema_entry.hpp"
#include "oracle_utils.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"

namespace duckdb {

// ─── コンストラクタ ───────────────────────────────────────────────────────────

OracleCatalog::OracleCatalog(AttachedDatabase &db,
                               const OracleConnectionParameters &params)
    : Catalog(db), params_(params) {
    pool_ = make_uniq<OracleConnectionPool>(params_, /*max=*/8);
}

OracleCatalog::~OracleCatalog() = default;

// ─── Attach コールバック ──────────────────────────────────────────────────────

unique_ptr<Catalog>
OracleCatalog::Attach(StorageExtensionInfo *info, ClientContext &context,
                       AttachedDatabase &db, const string &name,
                       AttachInfo &attach_info, AccessMode access_mode) {
    OracleConnectionParameters params =
        OracleConnectionParameters::ParseConnectionString(attach_info.path);
    params.read_only = (access_mode == AccessMode::READ_ONLY);

    // AttachInfo のオプションを確認
    for (auto &opt : attach_info.options) {
        if (opt.first == "schema") {
            params.schema = opt.second.GetValue<string>();
        } else if (opt.first == "fetch_size") {
            params.fetch_size = (int)opt.second.GetValue<int64_t>();
        }
    }

    // 接続テスト
    auto test_conn = OracleConnection::Open(params);
    // 成功したら解放（プールで再度接続する）

    return make_uniq<OracleCatalog>(db, params);
}

// ─── Initialize ───────────────────────────────────────────────────────────────

void OracleCatalog::Initialize(bool load_builtin) {
    // デフォルトスキーマをプリロード
    PreloadSchema(params_.GetEffectiveSchema());
}

// ─── GetSchemaFromCache ────────────────────────────────────────────────────────

optional_ptr<SchemaCatalogEntry>
OracleCatalog::GetSchemaFromCache(const string &schema_name) {
    std::lock_guard<std::mutex> lk(cache_mutex_);
    auto it = schema_cache_.find(OracleUtils::ToUpper(schema_name));
    if (it != schema_cache_.end()) {
        return it->second.get();
    }
    return nullptr;
}

// ─── CreateSchemaEntry ────────────────────────────────────────────────────────

unique_ptr<SchemaCatalogEntry>
OracleCatalog::CreateSchemaEntry(const string &schema_name) {
    CreateSchemaInfo info;
    info.schema = OracleUtils::ToUpper(schema_name);
    auto entry = make_uniq<OracleSchemaEntry>(*this, info, *pool_);
    return std::move(entry);
}

// ─── PreloadSchema ────────────────────────────────────────────────────────────

void OracleCatalog::PreloadSchema(const string &schema) {
    std::string upper = OracleUtils::ToUpper(schema);
    auto entry = CreateSchemaEntry(upper);
    std::lock_guard<std::mutex> lk(cache_mutex_);
    schema_cache_[upper] = std::move(entry);
}

// ─── GetSchema ────────────────────────────────────────────────────────────────

optional_ptr<SchemaCatalogEntry>
OracleCatalog::GetSchema(CatalogTransaction transaction,
                          const string &schema_name,
                          OnEntryNotFound on_not_found,
                          QueryErrorContext error_context) {
    std::string upper = OracleUtils::ToUpper(schema_name);

    auto cached = GetSchemaFromCache(upper);
    if (cached) return cached;

    // キャッシュにない場合、動的に作成してキャッシュ
    {
        std::lock_guard<std::mutex> lk(cache_mutex_);
        // Double-check
        auto it = schema_cache_.find(upper);
        if (it != schema_cache_.end()) {
            return it->second.get();
        }
        auto entry = CreateSchemaEntry(upper);
        auto *raw = entry.get();
        schema_cache_[upper] = std::move(entry);
        return raw;
    }
}

// ─── GetEntryInternal ─────────────────────────────────────────────────────────

optional_ptr<CatalogEntry>
OracleCatalog::GetEntryInternal(CatalogTransaction transaction,
                                 CatalogType type,
                                 const string &schema,
                                 const string &name) {
    auto schema_entry = GetSchema(transaction, schema,
                                  OnEntryNotFound::RETURN_NULL);
    if (!schema_entry) return nullptr;
    return schema_entry->GetEntry(transaction, type, name);
}

// ─── ScanSchemas ─────────────────────────────────────────────────────────────

void OracleCatalog::ScanSchemas(ClientContext &context,
                                  std::function<void(SchemaCatalogEntry &)> callback) {
    std::lock_guard<std::mutex> lk(cache_mutex_);
    for (auto &kv : schema_cache_) {
        callback(*kv.second);
    }
}

// ─── ClearCache ───────────────────────────────────────────────────────────────

void OracleCatalog::ClearCache() {
    {
        std::lock_guard<std::mutex> lk(cache_mutex_);
        schema_cache_.clear();
    }
    pool_->ClearCache();
    // デフォルトスキーマを再ロード
    PreloadSchema(params_.GetEffectiveSchema());
}

// ─── OracleTransaction ────────────────────────────────────────────────────────

OracleTransaction::OracleTransaction(TransactionManager &manager,
                                       ClientContext &context)
    : Transaction(manager, context) {}

// ─── OracleTransactionManager ────────────────────────────────────────────────

OracleTransactionManager::OracleTransactionManager(AttachedDatabase &db_p,
                                                     OracleCatalog &catalog)
    : TransactionManager(db_p), catalog_(catalog) {}

unique_ptr<TransactionManager>
OracleTransactionManager::Create(StorageExtensionInfo *info,
                                  AttachedDatabase &db, Catalog &catalog) {
    auto &oracle_catalog = catalog.Cast<OracleCatalog>();
    return make_uniq<OracleTransactionManager>(db, oracle_catalog);
}

Transaction &OracleTransactionManager::StartTransaction(ClientContext &context) {
    auto transaction = make_uniq<OracleTransaction>(*this, context);
    auto &result = *transaction;
    lock_guard<mutex> l(transaction_lock);
    transactions[result] = std::move(transaction);
    return result;
}

ErrorData OracleTransactionManager::CommitTransaction(ClientContext &context,
                                                        Transaction &transaction) {
    lock_guard<mutex> l(transaction_lock);
    transactions.erase(transaction);
    return ErrorData();
}

void OracleTransactionManager::RollbackTransaction(Transaction &transaction) {
    lock_guard<mutex> l(transaction_lock);
    transactions.erase(transaction);
}

} // namespace duckdb
