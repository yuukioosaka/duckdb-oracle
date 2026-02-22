#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/function/table_function.hpp"

#include "oracle_storage.hpp"
#include "oracle_catalog.hpp"
#include "oracle_connection.hpp"
#include "oracle_utils.hpp"
#include "oracle_scan.hpp"

namespace duckdb {

// ─── oracle_query() テーブル関数 ──────────────────────────────────────────────

struct OracleQueryBindData : public FunctionData {
    std::shared_ptr<OracleConnectionPool> pool;
    std::string sql;
    std::vector<OracleColumnInfo> columns;
    std::vector<LogicalType>      types;
    int oracle_major_version = 12;

    unique_ptr<FunctionData> Copy() const override {
        auto copy = make_uniq<OracleQueryBindData>();
        copy->pool = pool;
        copy->sql  = sql;
        copy->columns = columns;
        copy->types   = types;
        copy->oracle_major_version = oracle_major_version;
        return std::move(copy);
    }
    bool Equals(const FunctionData &other) const override {
        return sql == other.Cast<OracleQueryBindData>().sql;
    }
};

struct OracleQueryLocalState : public LocalTableFunctionState {
    std::shared_ptr<OracleConnection> conn;
    vector<LogicalType> types;
    std::string sql;
    bool done = false;
    vector<DataChunk> chunks;
    idx_t chunk_idx = 0;
};

static unique_ptr<FunctionData>
OracleQueryBind(ClientContext &context, TableFunctionBindInput &input,
                vector<LogicalType> &return_types, vector<string> &names) {
    // 引数: oracle_query(database_name, sql_string)
    if (input.inputs.size() != 2) {
        throw BinderException("oracle_query requires exactly 2 arguments: "
                              "(database_name, sql_string)");
    }
    auto db_name = input.inputs[0].GetValue<string>();
    auto sql_str = input.inputs[1].GetValue<string>();

    // データベース名から Catalog を取得
    auto &catalog = Catalog::GetCatalog(context, db_name);
    if (catalog.GetCatalogType() != "oracle") {
        throw BinderException("Database '" + db_name + "' is not an Oracle database");
    }
    auto &oracle_catalog = catalog.Cast<OracleCatalog>();

    // クエリを実行してカラム情報を取得（DESCRIBE 相当）
    // ODPI-C で SELECT 0 ROWS のダミーを実行してメタデータを取得
    auto conn = oracle_catalog.GetConnectionPool().Acquire();
    std::string meta_sql = "SELECT * FROM (" + sql_str + ") WHERE 1=0";
    dpiStmt *stmt = nullptr;

    // 簡易実装: 実際のクエリを実行してメタ情報を取得
    // (本番実装では PrepareStatement を使う)
    auto bind_data = make_uniq<OracleQueryBindData>();
    bind_data->sql = sql_str;
    bind_data->oracle_major_version = conn->GetServerMajorVersion();

    // ダミーの実行でカラム情報を取得
    conn->ExecuteQuery(meta_sql, {}, 1, [&](DataChunk &chunk) -> bool {
        return false;
    });
    oracle_catalog.GetConnectionPool().Release(conn);

    // フォールバック: VARCHAR 型でカラム名を返す（実際にはメタデータから取得）
    // TODO: ODPI-C の PrepareStatement でメタデータのみ取得するよう改善

    bind_data->pool = std::shared_ptr<OracleConnectionPool>(
        &oracle_catalog.GetConnectionPool(), [](auto *) {});

    return_types = bind_data->types;
    names = {};
    for (const auto &c : bind_data->columns) names.push_back(c.name);

    return std::move(bind_data);
}

// ─── oracle_clear_cache() スカラー関数 ────────────────────────────────────────

static void OracleClearCacheFunction(DataChunk &args, ExpressionState &state,
                                      Vector &result) {
    auto &db_name_vec = args.data[0];
    UnaryExecutor::Execute<string_t, int32_t>(
        db_name_vec, result, args.size(), [&](string_t db_name) -> int32_t {
            // ClientContext を取得する方法は state 経由
            auto &client = state.GetContext();
            try {
                auto &catalog = Catalog::GetCatalog(client, db_name.GetString());
                if (catalog.GetCatalogType() == "oracle") {
                    catalog.Cast<OracleCatalog>().ClearCache();
                }
                return 1;
            } catch (...) {
                return 0;
            }
        });
}

// ─── oracle_info() テーブル関数 ───────────────────────────────────────────────

static unique_ptr<FunctionData>
OracleInfoBind(ClientContext &context, TableFunctionBindInput &input,
               vector<LogicalType> &return_types, vector<string> &names) {
    names        = {"key", "value"};
    return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR};
    return nullptr;
}

struct OracleInfoState : public GlobalTableFunctionState {
    vector<pair<string, string>> info;
    idx_t idx = 0;
};

static unique_ptr<GlobalTableFunctionState>
OracleInfoInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
    auto state = make_uniq<OracleInfoState>();
    if (!input.inputs.empty()) {
        auto db_name = input.inputs[0].GetValue<string>();
        try {
            auto &catalog = Catalog::GetCatalog(context, db_name);
            if (catalog.GetCatalogType() == "oracle") {
                auto &oc = catalog.Cast<OracleCatalog>();
                auto conn = oc.GetConnectionPool().Acquire();
                state->info.push_back({"server_version", conn->GetServerVersion()});
                state->info.push_back({"catalog_type", "oracle"});
                oc.GetConnectionPool().Release(conn);
            }
        } catch (const std::exception &e) {
            state->info.push_back({"error", e.what()});
        }
    }
    return std::move(state);
}

static void OracleInfoScan(ClientContext &context, TableFunctionInput &data,
                            DataChunk &output) {
    auto &state = data.global_state->Cast<OracleInfoState>();
    if (state.idx >= state.info.size()) return;

    idx_t count = 0;
    while (state.idx < state.info.size() && count < STANDARD_VECTOR_SIZE) {
        output.SetValue(0, count, Value(state.info[state.idx].first));
        output.SetValue(1, count, Value(state.info[state.idx].second));
        ++state.idx;
        ++count;
    }
    output.SetCardinality(count);
}

// ─── 拡張エントリポイント ─────────────────────────────────────────────────────

static void LoadInternal(DatabaseInstance &db) {
    // 1. StorageExtension の登録
    auto &config = DBConfig::GetConfig(db);
    config.storage_extensions["oracle"] = make_uniq<OracleStorageExtension>();

    // 2. oracle_query() テーブル関数の登録
    // (メタ情報取得の完全実装後に有効化)
    // TableFunction oracle_query_func("oracle_query", {LogicalType::VARCHAR, LogicalType::VARCHAR},
    //                                 nullptr, OracleQueryBind);
    // ExtensionUtil::RegisterFunction(db, oracle_query_func);

    // 3. oracle_clear_cache() スカラー関数の登録
    ScalarFunction clear_cache_func(
        "oracle_clear_cache",
        {LogicalType::VARCHAR},
        LogicalType::INTEGER,
        OracleClearCacheFunction);
    ExtensionUtil::RegisterFunction(db, clear_cache_func);

    // 4. oracle_info() テーブル関数の登録
    TableFunction info_func("oracle_info", {LogicalType::VARCHAR},
                             OracleInfoScan, OracleInfoBind,
                             OracleInfoInitGlobal);
    ExtensionUtil::RegisterFunction(db, info_func);
}

} // namespace duckdb

// ─── extern "C" エクスポート ──────────────────────────────────────────────────

extern "C" {

DUCKDB_EXTENSION_API void oracle_scanner_init(duckdb::DatabaseInstance &db) {
    duckdb::LoadInternal(db);
}

DUCKDB_EXTENSION_API const char *oracle_scanner_version() {
    return duckdb::DuckDB::LibraryVersion();
}

DUCKDB_EXTENSION_API void oracle_scanner_storage_init(duckdb::DBConfig &config) {
    config.storage_extensions["oracle"] = duckdb::make_uniq<duckdb::OracleStorageExtension>();
}

}
