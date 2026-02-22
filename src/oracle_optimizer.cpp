#include "oracle_optimizer.hpp"
#include "oracle_scan.hpp"
#include "oracle_utils.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_column_ref_expression.hpp"
#include <sstream>

namespace duckdb {

// ─── ConstantToSQL ────────────────────────────────────────────────────────────

std::string OracleFilterPushdown::ConstantToSQL(const BoundConstantExpression &expr) {
    const Value &val = expr.value;
    if (val.IsNull()) return "NULL";

    switch (val.type().id()) {
    case LogicalTypeId::BOOLEAN:
        return val.GetValue<bool>() ? "1" : "0";
    case LogicalTypeId::TINYINT:
    case LogicalTypeId::SMALLINT:
    case LogicalTypeId::INTEGER:
        return std::to_string(val.GetValue<int32_t>());
    case LogicalTypeId::BIGINT:
        return std::to_string(val.GetValue<int64_t>());
    case LogicalTypeId::FLOAT:
        return std::to_string(val.GetValue<float>());
    case LogicalTypeId::DOUBLE:
        return std::to_string(val.GetValue<double>());
    case LogicalTypeId::VARCHAR: {
        // シングルクォートをエスケープ
        std::string s = val.GetValue<std::string>();
        std::string result = "'";
        for (char c : s) {
            if (c == '\'') result += "'";
            result += c;
        }
        result += "'";
        return result;
    }
    case LogicalTypeId::DATE: {
        // DATE '2024-01-01' 形式
        auto d = val.GetValue<date_t>();
        int32_t year, month, day;
        Date::Convert(d, year, month, day);
        char buf[32];
        snprintf(buf, sizeof(buf), "DATE '%04d-%02d-%02d'", year, month, day);
        return buf;
    }
    case LogicalTypeId::TIMESTAMP: {
        auto ts = val.GetValue<timestamp_t>();
        auto date = Timestamp::GetDate(ts);
        auto time = Timestamp::GetTime(ts);
        int32_t year, month, day;
        Date::Convert(date, year, month, day);
        int32_t hour, min, sec, ms;
        Time::Convert(time, hour, min, sec, ms);
        char buf[64];
        snprintf(buf, sizeof(buf),
                 "TIMESTAMP '%04d-%02d-%02d %02d:%02d:%02d'",
                 year, month, day, hour, min, sec);
        return buf;
    }
    default:
        return ""; // 変換不能
    }
}

// ─── ColumnToSQL ──────────────────────────────────────────────────────────────

std::string OracleFilterPushdown::ColumnToSQL(const BoundColumnRefExpression &expr,
                                               const std::vector<std::string> &col_names) {
    idx_t col_idx = expr.binding.column_index;
    if (col_idx >= col_names.size()) return "";
    return OracleUtils::QuoteIdentifier(col_names[col_idx]);
}

// ─── ComparisonToSQL ──────────────────────────────────────────────────────────

std::string OracleFilterPushdown::ComparisonToSQL(
    const BoundComparisonExpression &expr,
    const std::vector<std::string> &col_names) {

    std::string lhs = ExpressionToSQL(*expr.left, col_names);
    std::string rhs = ExpressionToSQL(*expr.right, col_names);
    if (lhs.empty() || rhs.empty()) return "";

    std::string op;
    switch (expr.type) {
    case ExpressionType::COMPARE_EQUAL:            op = " = ";  break;
    case ExpressionType::COMPARE_NOTEQUAL:         op = " <> "; break;
    case ExpressionType::COMPARE_LESSTHAN:         op = " < ";  break;
    case ExpressionType::COMPARE_GREATERTHAN:      op = " > ";  break;
    case ExpressionType::COMPARE_LESSTHANOREQUALTO:   op = " <= "; break;
    case ExpressionType::COMPARE_GREATERTHANOREQUALTO: op = " >= "; break;
    default:
        return "";
    }
    return "(" + lhs + op + rhs + ")";
}

// ─── ConjunctionToSQL ─────────────────────────────────────────────────────────

std::string OracleFilterPushdown::ConjunctionToSQL(
    const BoundConjunctionExpression &expr,
    const std::vector<std::string> &col_names) {

    std::string op;
    if (expr.type == ExpressionType::CONJUNCTION_AND) {
        op = " AND ";
    } else if (expr.type == ExpressionType::CONJUNCTION_OR) {
        op = " OR ";
    } else {
        return "";
    }

    std::vector<std::string> parts;
    for (const auto &child : expr.children) {
        std::string part = ExpressionToSQL(*child, col_names);
        if (part.empty()) return ""; // 全部プッシュダウンできないと不正確
        parts.push_back(part);
    }
    std::string result = "(";
    for (size_t i = 0; i < parts.size(); ++i) {
        if (i > 0) result += op;
        result += parts[i];
    }
    result += ")";
    return result;
}

// ─── FunctionToSQL ────────────────────────────────────────────────────────────

std::string OracleFilterPushdown::FunctionToSQL(
    const BoundFunctionExpression &expr,
    const std::vector<std::string> &col_names) {

    const std::string &fn = expr.function.name;

    // IS NULL / IS NOT NULL
    if (fn == "isnull" && expr.children.size() == 1) {
        std::string child = ExpressionToSQL(*expr.children[0], col_names);
        if (child.empty()) return "";
        return "(" + child + " IS NULL)";
    }
    if (fn == "isnotnull" && expr.children.size() == 1) {
        std::string child = ExpressionToSQL(*expr.children[0], col_names);
        if (child.empty()) return "";
        return "(" + child + " IS NOT NULL)";
    }
    // LIKE
    if (fn == "~~" && expr.children.size() == 2) {
        std::string col = ExpressionToSQL(*expr.children[0], col_names);
        std::string pat = ExpressionToSQL(*expr.children[1], col_names);
        if (col.empty() || pat.empty()) return "";
        return "(" + col + " LIKE " + pat + ")";
    }

    return ""; // 未サポート関数
}

// ─── ExpressionToSQL ──────────────────────────────────────────────────────────

std::string OracleFilterPushdown::ExpressionToSQL(
    const Expression &expr,
    const std::vector<std::string> &col_names) {

    switch (expr.expression_class) {
    case ExpressionClass::BOUND_COMPARISON:
        return ComparisonToSQL(expr.Cast<BoundComparisonExpression>(), col_names);
    case ExpressionClass::BOUND_CONJUNCTION:
        return ConjunctionToSQL(expr.Cast<BoundConjunctionExpression>(), col_names);
    case ExpressionClass::BOUND_FUNCTION:
        return FunctionToSQL(expr.Cast<BoundFunctionExpression>(), col_names);
    case ExpressionClass::BOUND_CONSTANT:
        return ConstantToSQL(expr.Cast<BoundConstantExpression>());
    case ExpressionClass::BOUND_COLUMN_REF:
        return ColumnToSQL(expr.Cast<BoundColumnRefExpression>(), col_names);
    default:
        return "";
    }
}

// ─── PushdownFilters ──────────────────────────────────────────────────────────

void OracleFilterPushdown::PushdownFilters(OracleScanBindData &bind_data,
                                            std::vector<std::string> &column_names,
                                            std::vector<unique_ptr<Expression>> &filters) {
    std::vector<unique_ptr<Expression>> remaining;
    for (auto &filter : filters) {
        std::string sql = ExpressionToSQL(*filter, column_names);
        if (!sql.empty()) {
            bind_data.filters.push_back(sql);
        } else {
            remaining.push_back(std::move(filter));
        }
    }
    filters = std::move(remaining);
}

} // namespace duckdb
