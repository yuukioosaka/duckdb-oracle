#include "oracle_scan.hpp"
#include "oracle_optimizer.hpp"
#include "oracle_utils.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include <sstream>

namespace duckdb {

// ─── OracleScanBindData ───────────────────────────────────────────────────────

unique_ptr<FunctionData> OracleScanBindData::Copy() const {
    auto copy = make_uniq<OracleScanBindData>();
    copy->pool   = pool;
    copy->schema = schema;
    copy->table  = table;
    copy->all_columns = all_columns;
    copy->all_types   = all_types;
    copy->filters     = filters;
    copy->column_ids  = column_ids;
    copy->limit       = limit;
    copy->offset      = offset;
    copy->oracle_major_version = oracle_major_version;
    return std::move(copy);
}

bool OracleScanBindData::Equals(const FunctionData &other) const {
    const auto &o = other.Cast<OracleScanBindData>();
    return schema == o.schema && table == o.table;
}

std::string OracleScanBindData::BuildSelectQuery() const {
    std::ostringstream oss;
    oss << "SELECT ";

    // Projection: column_ids が空なら全カラム
    if (column_ids.empty()) {
        oss << "*";
    } else {
        bool first = true;
        for (column_t cid : column_ids) {
            if (cid == COLUMN_IDENTIFIER_ROW_ID) {
                if (!first) oss << ", ";
                oss << "ROWID";
                first = false;
            } else if (cid < all_columns.size()) {
                if (!first) oss << ", ";
                oss << OracleUtils::QuoteIdentifier(all_columns[cid].name);
                first = false;
            }
        }
        if (first) oss << "*";
    }

    oss << " FROM " << OracleUtils::QuoteIdentifier(schema)
        << "." << OracleUtils::QuoteIdentifier(table);

    // WHERE
    if (!filters.empty()) {
        oss << " WHERE ";
        for (size_t i = 0; i < filters.size(); ++i) {
            if (i > 0) oss << " AND ";
            oss << filters[i];
        }
    }

    // LIMIT / OFFSET
    if (limit != DConstants::INVALID_INDEX) {
        if (oracle_major_version >= 12) {
            // Oracle 12c+ ANSI 構文
            if (offset > 0) {
                oss << " OFFSET " << offset << " ROWS";
            }
            oss << " FETCH FIRST " << limit << " ROWS ONLY";
        } else {
            // Oracle 11g 以前: ROWNUM を使ったサブクエリ
            std::string inner = oss.str();
            std::ostringstream outer;
            outer << "SELECT * FROM (SELECT ROWNUM rn__, t__.* FROM (" << inner
                  << ") t__ WHERE ROWNUM <= " << (offset + limit) << ")"
                  << " WHERE rn__ > " << offset;
            return outer.str();
        }
    }

    return oss.str();
}

// ─── GlobalState ──────────────────────────────────────────────────────────────

OracleScanGlobalState::OracleScanGlobalState(const OracleScanBindData &bind_data) {
    // 単純実装: タスクは 1 つ（並列化拡張は Phase 3）
    ScanTask task;
    tasks.push_back(task);
    max_threads = 1;
}

// ─── Bind ─────────────────────────────────────────────────────────────────────

unique_ptr<FunctionData>
OracleScan::Bind(ClientContext &context, TableFunctionBindInput &input,
                  vector<LogicalType> &return_types, vector<string> &names) {
    auto &bind_data = input.bind_data->Cast<OracleScanBindData>();

    for (const auto &col : bind_data.all_columns) {
        names.push_back(col.name);
        return_types.push_back(OracleTypeMapping::ToDuckDBType(col));
    }
    bind_data.all_types = return_types;

    return input.bind_data->Copy();
}

// ─── InitGlobal ───────────────────────────────────────────────────────────────

unique_ptr<GlobalTableFunctionState>
OracleScan::InitGlobal(ClientContext &context, TableFunctionInitInput &input) {
    const auto &bind_data = input.bind_data->Cast<OracleScanBindData>();
    return make_uniq<OracleScanGlobalState>(bind_data);
}

// ─── InitLocal ────────────────────────────────────────────────────────────────

unique_ptr<LocalTableFunctionState>
OracleScan::InitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                       GlobalTableFunctionState *gstate) {
    const auto &bind_data = input.bind_data->Cast<OracleScanBindData>();
    auto local = make_uniq<OracleScanLocalState>();

    // プールから接続を取得
    local->connection = bind_data.pool->Acquire();
    return std::move(local);
}

// ─── Scan ─────────────────────────────────────────────────────────────────────

void OracleScan::Scan(ClientContext &context, TableFunctionInput &data,
                       DataChunk &output) {
    auto &bind_data  = data.bind_data->Cast<OracleScanBindData>();
    auto &local      = data.local_state->Cast<OracleScanLocalState>();
    auto &global_st  = data.global_state->Cast<OracleScanGlobalState>();

    if (local.done) return;

    // 列の型リストを組み立てる（column_ids によるプロジェクション）
    std::vector<LogicalType> projected_types;
    if (bind_data.column_ids.empty()) {
        projected_types = bind_data.all_types;
    } else {
        for (column_t cid : bind_data.column_ids) {
            if (cid == COLUMN_IDENTIFIER_ROW_ID) {
                projected_types.push_back(LogicalType::VARCHAR);
            } else if (cid < bind_data.all_types.size()) {
                projected_types.push_back(bind_data.all_types[cid]);
            }
        }
    }

    std::string sql = bind_data.BuildSelectQuery();
    bool got_chunk = false;

    local.connection->ExecuteQuery(sql, projected_types,
                                   bind_data.pool->Acquire()->GetParams().fetch_size,
                                   [&](DataChunk &chunk) -> bool {
        output.Move(chunk);
        got_chunk = true;
        return false; // 1チャンクずつ返す
    });

    if (!got_chunk) {
        local.done = true;
    }
}

// ─── Cardinality ──────────────────────────────────────────────────────────────

unique_ptr<NodeStatistics>
OracleScan::Cardinality(ClientContext &context, const FunctionData *bind_data_p) {
    // Oracle の NUM_ROWS 統計を使う（AnalyzeTable が必要）
    // ここでは推定値を返す
    return make_uniq<NodeStatistics>(100000, 100000);
}

// ─── ComplexFilter (Pushdown) ─────────────────────────────────────────────────

void OracleScan::ComplexFilter(ClientContext &context,
                                LogicalGet &get,
                                FunctionData *bind_data_p,
                                vector<unique_ptr<Expression>> &filters) {
    auto &bind_data = bind_data_p->Cast<OracleScanBindData>();

    // カラム名リストを構築
    std::vector<std::string> col_names;
    for (const auto &col : bind_data.all_columns) {
        col_names.push_back(col.name);
    }

    OracleFilterPushdown::PushdownFilters(bind_data, col_names, filters);
}

// ─── GetFunction ──────────────────────────────────────────────────────────────

TableFunction OracleScan::GetFunction() {
    TableFunction func("oracle_scan", {}, OracleScan::Scan);
    func.bind          = OracleScan::Bind;
    func.init_global   = OracleScan::InitGlobal;
    func.init_local    = OracleScan::InitLocal;
    func.cardinality   = OracleScan::Cardinality;
    func.pushdown_complex_filter = OracleScan::ComplexFilter;
    func.projection_pushdown = true;
    return func;
}

} // namespace duckdb
