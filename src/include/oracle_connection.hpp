#pragma once

#include "duckdb.hpp"
#include "oracle_utils.hpp"
#include "oracle_type_mapping.hpp"
#include <dpi.h>
#include <mutex>
#include <vector>
#include <functional>
#include <memory>

namespace duckdb {

// ───────────────────────────────────────────────────────────────────────────────
// テーブル一覧取得用の軽量構造体
// ───────────────────────────────────────────────────────────────────────────────
struct OracleTableInfo {
    std::string schema;
    std::string name;
    bool        is_view = false;
};

// ───────────────────────────────────────────────────────────────────────────────
// OracleConnection: ODPI-C 接続ラッパー（スレッドセーフ）
// ───────────────────────────────────────────────────────────────────────────────
class OracleConnection {
public:
    ~OracleConnection();

    // 接続を開く。失敗時は例外をスロー
    static std::shared_ptr<OracleConnection>
        Open(const OracleConnectionParameters &params);

    // ─── スキーマ情報 ──────────────────────────────────────────────────────────
    std::vector<OracleTableInfo>  GetTables(const std::string &schema);
    std::vector<OracleColumnInfo> GetColumns(const std::string &schema,
                                              const std::string &table);

    // ─── クエリ実行 ────────────────────────────────────────────────────────────
    // callback は DataChunk ごとに呼ばれる。戻り値が false なら中断。
    void ExecuteQuery(const std::string &sql,
                      const std::vector<LogicalType> &types,
                      idx_t fetch_size,
                      std::function<bool(DataChunk &)> callback);

    // 結果を返さない DML / DDL 実行
    void ExecuteDML(const std::string &sql);

    // バッチ INSERT（ARRAY DML を使用）
    void BulkInsert(const std::string &table_name,
                    const std::vector<std::string> &column_names,
                    DataChunk &chunk);

    // ─── バージョン情報 ────────────────────────────────────────────────────────
    std::string GetServerVersion();
    int         GetServerMajorVersion();

    // ─── パラメータ ────────────────────────────────────────────────────────────
    const OracleConnectionParameters &GetParams() const { return params_; }

private:
    OracleConnection() = default;

    void ThrowIfError(int rc, const std::string &context);
    void SetupContext();

    OracleConnectionParameters params_;
    dpiContext *ctx_   = nullptr;
    dpiConn    *conn_  = nullptr;
    std::mutex  mutex_;

    // ODPI-C のグローバルコンテキストはプロセスで一つ
    static dpiContext *global_ctx_;
    static std::mutex  ctx_mutex_;
    static dpiContext *GetOrCreateContext();
};

// ───────────────────────────────────────────────────────────────────────────────
// 接続プール（Catalog がキャッシュとして保持）
// ───────────────────────────────────────────────────────────────────────────────
class OracleConnectionPool {
public:
    explicit OracleConnectionPool(const OracleConnectionParameters &params,
                                   size_t max_connections = 8);

    std::shared_ptr<OracleConnection> Acquire();
    void Release(std::shared_ptr<OracleConnection> conn);

    void ClearCache();

private:
    OracleConnectionParameters params_;
    size_t max_connections_;
    std::vector<std::shared_ptr<OracleConnection>> pool_;
    std::mutex mutex_;
};

} // namespace duckdb
