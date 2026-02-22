#pragma once

#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "oracle_connection.hpp"
#include "oracle_type_mapping.hpp"

namespace duckdb {

// ───────────────────────────────────────────────────────────────────────────────
// スキャン Bind データ
// ───────────────────────────────────────────────────────────────────────────────
struct OracleScanBindData : public FunctionData {
    std::shared_ptr<OracleConnectionPool> pool;

    std::string schema;
    std::string table;
    std::vector<OracleColumnInfo> all_columns;  // テーブル全カラム
    std::vector<LogicalType>      all_types;

    // Pushdown されたフィルタ
    std::vector<std::string> filters;           // WHERE 句に追加する SQL 断片

    // Projection Pushdown: スキャンするカラムインデックス
    std::vector<column_t> column_ids;

    // LIMIT Pushdown
    idx_t limit  = DConstants::INVALID_INDEX;
    idx_t offset = 0;

    int oracle_major_version = 12; // FETCH FIRST vs ROWNUM

    unique_ptr<FunctionData> Copy() const override;
    bool Equals(const FunctionData &other) const override;

    // 実行する SELECT 文を組み立てる
    std::string BuildSelectQuery() const;
};

// ───────────────────────────────────────────────────────────────────────────────
// グローバルステート（並列スキャン用）
// ───────────────────────────────────────────────────────────────────────────────
struct OracleScanGlobalState : public GlobalTableFunctionState {
    explicit OracleScanGlobalState(const OracleScanBindData &bind_data);

    // ROWID 範囲ごとのタスクリスト
    struct ScanTask {
        std::string rowid_lo;  // 空文字 = 先頭
        std::string rowid_hi;  // 空文字 = 末尾
        bool        done = false;
    };

    std::vector<ScanTask> tasks;
    idx_t        next_task = 0;
    std::mutex   mutex;
    idx_t        max_threads;

    idx_t MaxThreads() const override { return max_threads; }
};

// ───────────────────────────────────────────────────────────────────────────────
// ローカルステート（スレッドごと）
// ───────────────────────────────────────────────────────────────────────────────
struct OracleScanLocalState : public LocalTableFunctionState {
    std::shared_ptr<OracleConnection> connection;
    DataChunk  current_chunk;
    bool       done = false;
};

// ───────────────────────────────────────────────────────────────────────────────
// スキャン関数群（OracleTableEntry::GetScanFunction から登録）
// ───────────────────────────────────────────────────────────────────────────────
class OracleScan {
public:
    // Bind
    static unique_ptr<FunctionData>
        Bind(ClientContext &context, TableFunctionBindInput &input,
             vector<LogicalType> &return_types, vector<string> &names);

    // GlobalState
    static unique_ptr<GlobalTableFunctionState>
        InitGlobal(ClientContext &context, TableFunctionInitInput &input);

    // LocalState
    static unique_ptr<LocalTableFunctionState>
        InitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                  GlobalTableFunctionState *global_state);

    // スキャン本体
    static void Scan(ClientContext &context, TableFunctionInput &data,
                     DataChunk &output);

    // Cardinality ヒント
    static unique_ptr<NodeStatistics>
        Cardinality(ClientContext &context, const FunctionData *bind_data);

    // Pushdown コールバック
    static void ComplexFilter(ClientContext &context,
                               LogicalGet &get,
                               FunctionData *bind_data,
                               vector<unique_ptr<Expression>> &filters);

    // テーブル関数オブジェクトを返す
    static TableFunction GetFunction();
};

} // namespace duckdb
