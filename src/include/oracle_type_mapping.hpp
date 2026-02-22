#pragma once

#include "duckdb.hpp"
#include "duckdb/common/types.hpp"
#include <dpi.h>

namespace duckdb {

// ───────────────────────────────────────────────────────────────────────────────
// Oracle カラム情報
// ───────────────────────────────────────────────────────────────────────────────
struct OracleColumnInfo {
    std::string name;
    std::string oracle_type_name;   // "NUMBER", "VARCHAR2", ...
    int32_t     precision = 0;      // NUMBER(p,s) の p
    int32_t     scale     = -127;   // NUMBER(p,s) の s (-127 = 未指定)
    int32_t     char_length = 0;    // VARCHAR2(n) の n
    bool        nullable = true;

    // ODPI-C クエリ情報から生成
    static OracleColumnInfo FromQueryInfo(const dpiQueryInfo &info, const std::string &name);
};

// ───────────────────────────────────────────────────────────────────────────────
// 型マッピング
// ───────────────────────────────────────────────────────────────────────────────
class OracleTypeMapping {
public:
    // OracleColumnInfo → DuckDB LogicalType
    static LogicalType ToDuckDBType(const OracleColumnInfo &col);

    // DuckDB LogicalType → Oracle DDL 型文字列
    static std::string ToOracleType(const LogicalType &type);

    // ODPI-C の dpiNativeTypeNum を DuckDB の Value に変換
    static Value ToDuckDBValue(dpiData *data, dpiNativeTypeNum native_type,
                               const LogicalType &target_type);
};

} // namespace duckdb
