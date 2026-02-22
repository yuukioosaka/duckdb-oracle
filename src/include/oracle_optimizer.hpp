#pragma once

#include "duckdb.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include <string>
#include <vector>

namespace duckdb {

class OracleScanBindData;

// ───────────────────────────────────────────────────────────────────────────────
// OracleFilterPushdown: DuckDB の Expression を Oracle SQL に変換
// ───────────────────────────────────────────────────────────────────────────────
class OracleFilterPushdown {
public:
    // Expression を WHERE 句の SQL 文字列に変換
    // 変換できない場合は空文字列を返す
    static std::string ExpressionToSQL(const Expression &expr,
                                        const std::vector<std::string> &column_names);

    // フィルタリストを処理し、pushdown 可能なものを bind_data に追加、
    // 残りは filters に残す
    static void PushdownFilters(OracleScanBindData &bind_data,
                                 std::vector<std::string> &column_names,
                                 std::vector<unique_ptr<Expression>> &filters);

private:
    static std::string ComparisonToSQL(const BoundComparisonExpression &expr,
                                        const std::vector<std::string> &col_names);
    static std::string ConjunctionToSQL(const BoundConjunctionExpression &expr,
                                         const std::vector<std::string> &col_names);
    static std::string FunctionToSQL(const BoundFunctionExpression &expr,
                                      const std::vector<std::string> &col_names);
    static std::string ConstantToSQL(const BoundConstantExpression &expr);
    static std::string ColumnToSQL(const BoundColumnRefExpression &expr,
                                    const std::vector<std::string> &col_names);
};

} // namespace duckdb
