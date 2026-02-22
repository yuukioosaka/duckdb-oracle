#include "oracle_type_mapping.hpp"
#include "duckdb/common/exception.hpp"
#include <cstring>
#include <cmath>

namespace duckdb {

// ─── OracleColumnInfo::FromQueryInfo ──────────────────────────────────────────

OracleColumnInfo OracleColumnInfo::FromQueryInfo(const dpiQueryInfo &info,
                                                   const std::string &name) {
    OracleColumnInfo col;
    col.name = name;
    col.nullable = info.nullOk;

    switch (info.typeInfo.oracleTypeNum) {
    case DPI_ORACLE_TYPE_NUMBER:
        col.oracle_type_name = "NUMBER";
        col.precision = info.typeInfo.precision;
        col.scale     = info.typeInfo.scale;
        break;
    case DPI_ORACLE_TYPE_VARCHAR:
        col.oracle_type_name = "VARCHAR2";
        col.char_length = info.typeInfo.dbSizeInBytes;
        break;
    case DPI_ORACLE_TYPE_NVARCHAR:
        col.oracle_type_name = "NVARCHAR2";
        col.char_length = info.typeInfo.sizeInChars;
        break;
    case DPI_ORACLE_TYPE_CHAR:
        col.oracle_type_name = "CHAR";
        col.char_length = info.typeInfo.dbSizeInBytes;
        break;
    case DPI_ORACLE_TYPE_NCHAR:
        col.oracle_type_name = "NCHAR";
        col.char_length = info.typeInfo.sizeInChars;
        break;
    case DPI_ORACLE_TYPE_DATE:
        col.oracle_type_name = "DATE";
        break;
    case DPI_ORACLE_TYPE_TIMESTAMP:
        col.oracle_type_name = "TIMESTAMP";
        col.scale = info.typeInfo.fsPrecision;
        break;
    case DPI_ORACLE_TYPE_TIMESTAMP_TZ:
        col.oracle_type_name = "TIMESTAMP WITH TIME ZONE";
        break;
    case DPI_ORACLE_TYPE_TIMESTAMP_LTZ:
        col.oracle_type_name = "TIMESTAMP WITH LOCAL TIME ZONE";
        break;
    case DPI_ORACLE_TYPE_CLOB:
        col.oracle_type_name = "CLOB";
        break;
    case DPI_ORACLE_TYPE_NCLOB:
        col.oracle_type_name = "NCLOB";
        break;
    case DPI_ORACLE_TYPE_BLOB:
        col.oracle_type_name = "BLOB";
        break;
    case DPI_ORACLE_TYPE_RAW:
        col.oracle_type_name = "RAW";
        col.char_length = info.typeInfo.dbSizeInBytes;
        break;
    case DPI_ORACLE_TYPE_NATIVE_FLOAT:
        col.oracle_type_name = "BINARY_FLOAT";
        break;
    case DPI_ORACLE_TYPE_NATIVE_DOUBLE:
        col.oracle_type_name = "BINARY_DOUBLE";
        break;
    case DPI_ORACLE_TYPE_NATIVE_INT:
    case DPI_ORACLE_TYPE_NATIVE_UINT:
        col.oracle_type_name = "NUMBER";
        col.precision = 38;
        col.scale = 0;
        break;
    case DPI_ORACLE_TYPE_ROWID:
        col.oracle_type_name = "ROWID";
        break;
    case DPI_ORACLE_TYPE_INTERVAL_YM:
        col.oracle_type_name = "INTERVAL YEAR TO MONTH";
        break;
    case DPI_ORACLE_TYPE_INTERVAL_DS:
        col.oracle_type_name = "INTERVAL DAY TO SECOND";
        break;
    default:
        col.oracle_type_name = "VARCHAR2";
        col.char_length = 4000;
        break;
    }
    return col;
}

// ─── OracleTypeMapping::ToDuckDBType ──────────────────────────────────────────

LogicalType OracleTypeMapping::ToDuckDBType(const OracleColumnInfo &col) {
    const auto &type = col.oracle_type_name;

    if (type == "NUMBER") {
        if (col.precision == 0 && col.scale == -127) {
            // NUMBER with no precision/scale → DOUBLE
            return LogicalType::DOUBLE;
        }
        if (col.scale == 0 || col.scale == -127) {
            // 整数
            if (col.precision <= 4)  return LogicalType::SMALLINT;
            if (col.precision <= 9)  return LogicalType::INTEGER;
            if (col.precision <= 18) return LogicalType::BIGINT;
            if (col.precision <= 38) return LogicalType::HUGEINT;
        }
        // 小数
        if (col.precision > 0 && col.scale >= 0) {
            return LogicalType::DECIMAL(col.precision, col.scale);
        }
        return LogicalType::DOUBLE;
    }

    if (type == "VARCHAR2" || type == "NVARCHAR2" ||
        type == "CHAR"    || type == "NCHAR" ||
        type == "ROWID"   || type == "CLOB"  || type == "NCLOB") {
        return LogicalType::VARCHAR;
    }

    if (type == "DATE" || type == "TIMESTAMP" ||
        type == "TIMESTAMP WITH LOCAL TIME ZONE") {
        return LogicalType::TIMESTAMP;
    }

    if (type == "TIMESTAMP WITH TIME ZONE") {
        return LogicalType::TIMESTAMP_TZ;
    }

    if (type == "BLOB" || type == "RAW") {
        return LogicalType::BLOB;
    }

    if (type == "BINARY_FLOAT") {
        return LogicalType::FLOAT;
    }

    if (type == "BINARY_DOUBLE") {
        return LogicalType::DOUBLE;
    }

    if (type == "INTERVAL YEAR TO MONTH" || type == "INTERVAL DAY TO SECOND") {
        return LogicalType::INTERVAL;
    }

    // フォールバック
    return LogicalType::VARCHAR;
}

// ─── OracleTypeMapping::ToOracleType ──────────────────────────────────────────

std::string OracleTypeMapping::ToOracleType(const LogicalType &type) {
    switch (type.id()) {
    case LogicalTypeId::BOOLEAN:   return "NUMBER(1)";
    case LogicalTypeId::TINYINT:   return "NUMBER(3)";
    case LogicalTypeId::SMALLINT:  return "NUMBER(5)";
    case LogicalTypeId::INTEGER:   return "NUMBER(10)";
    case LogicalTypeId::BIGINT:    return "NUMBER(19)";
    case LogicalTypeId::HUGEINT:   return "NUMBER(38)";
    case LogicalTypeId::FLOAT:     return "BINARY_FLOAT";
    case LogicalTypeId::DOUBLE:    return "BINARY_DOUBLE";
    case LogicalTypeId::DECIMAL: {
        auto width = DecimalType::GetWidth(type);
        auto scale = DecimalType::GetScale(type);
        return "NUMBER(" + std::to_string(width) + "," + std::to_string(scale) + ")";
    }
    case LogicalTypeId::VARCHAR:   return "VARCHAR2(4000)";
    case LogicalTypeId::BLOB:      return "BLOB";
    case LogicalTypeId::DATE:      return "DATE";
    case LogicalTypeId::TIMESTAMP: return "TIMESTAMP";
    case LogicalTypeId::TIMESTAMP_TZ: return "TIMESTAMP WITH TIME ZONE";
    case LogicalTypeId::INTERVAL:  return "INTERVAL DAY(9) TO SECOND(9)";
    default:
        return "VARCHAR2(4000)";
    }
}

// ─── OracleTypeMapping::ToDuckDBValue ─────────────────────────────────────────

Value OracleTypeMapping::ToDuckDBValue(dpiData *data,
                                        dpiNativeTypeNum native_type,
                                        const LogicalType &target_type) {
    if (data->isNull) {
        return Value(target_type);
    }

    switch (native_type) {
    case DPI_NATIVE_TYPE_DOUBLE:
        switch (target_type.id()) {
        case LogicalTypeId::FLOAT:   return Value::FLOAT((float)data->value.asDouble);
        case LogicalTypeId::DOUBLE:  return Value::DOUBLE(data->value.asDouble);
        case LogicalTypeId::DECIMAL: {
            double d = data->value.asDouble;
            uint8_t width = DecimalType::GetWidth(target_type);
            uint8_t scale = DecimalType::GetScale(target_type);
            int64_t v = (int64_t)std::round(d * std::pow(10.0, scale));
            return Value::DECIMAL(v, width, scale);
        }
        case LogicalTypeId::BIGINT:  return Value::BIGINT((int64_t)data->value.asDouble);
        case LogicalTypeId::INTEGER: return Value::INTEGER((int32_t)data->value.asDouble);
        default:
            return Value::DOUBLE(data->value.asDouble);
        }

    case DPI_NATIVE_TYPE_FLOAT:
        return Value::FLOAT(data->value.asFloat);

    case DPI_NATIVE_TYPE_INT64:
        switch (target_type.id()) {
        case LogicalTypeId::BIGINT:  return Value::BIGINT(data->value.asInt64);
        case LogicalTypeId::INTEGER: return Value::INTEGER((int32_t)data->value.asInt64);
        case LogicalTypeId::SMALLINT:return Value::SMALLINT((int16_t)data->value.asInt64);
        default:
            return Value::BIGINT(data->value.asInt64);
        }

    case DPI_NATIVE_TYPE_UINT64:
        return Value::UBIGINT(data->value.asUint64);

    case DPI_NATIVE_TYPE_BYTES: {
        std::string s(data->value.asBytes.ptr, data->value.asBytes.length);
        if (target_type == LogicalType::BLOB) {
            return Value::BLOB(s);
        }
        return Value(s);
    }

    case DPI_NATIVE_TYPE_TIMESTAMP: {
        const dpiTimestamp &ts = data->value.asTimestamp;
        // Oracle DATE / TIMESTAMP → DuckDB TIMESTAMP (microseconds since epoch)
        // 簡易実装: mktime を使用
        std::tm t = {};
        t.tm_year = ts.year - 1900;
        t.tm_mon  = ts.month - 1;
        t.tm_mday = ts.day;
        t.tm_hour = ts.hour;
        t.tm_min  = ts.minute;
        t.tm_sec  = ts.second;
        t.tm_isdst = -1;
        time_t epoch = mktime(&t);
        int64_t us = (int64_t)epoch * 1000000LL + ts.fsecond / 1000;

        if (target_type == LogicalType::TIMESTAMP_TZ) {
            int32_t tz_offset_sec = (ts.tzHourOffset * 60 + ts.tzMinuteOffset) * 60;
            us -= (int64_t)tz_offset_sec * 1000000LL;
            return Value::TIMESTAMPTZ(timestamp_t(us));
        }
        return Value::TIMESTAMP(timestamp_t(us));
    }

    case DPI_NATIVE_TYPE_INTERVAL_YM: {
        interval_t iv;
        iv.months = data->value.asIntervalYM.years * 12 +
                    data->value.asIntervalYM.months;
        iv.days  = 0;
        iv.micros = 0;
        return Value::INTERVAL(iv);
    }

    case DPI_NATIVE_TYPE_INTERVAL_DS: {
        const dpiIntervalDS &ids = data->value.asIntervalDS;
        interval_t iv;
        iv.months = 0;
        iv.days   = ids.days;
        iv.micros = (int64_t)ids.hours * 3600000000LL +
                    (int64_t)ids.minutes * 60000000LL +
                    (int64_t)ids.seconds * 1000000LL +
                    ids.fseconds / 1000;
        return Value::INTERVAL(iv);
    }

    case DPI_NATIVE_TYPE_BOOLEAN:
        return Value::BOOLEAN(data->value.asBoolean != 0);

    case DPI_NATIVE_TYPE_LOB: {
        // CLOB / BLOB: ストリームで読み取る
        dpiLob *lob = data->value.asLOB;
        uint64_t lob_size = 0;
        dpiLob_getSize(lob, &lob_size);
        if (lob_size == 0) {
            if (target_type == LogicalType::BLOB) return Value::BLOB("");
            return Value(std::string(""));
        }
        std::string buf(lob_size, '\0');
        uint64_t actual = lob_size;
        dpiLob_readBytes(lob, 1, lob_size, &buf[0], &actual);
        buf.resize(actual);
        if (target_type == LogicalType::BLOB) return Value::BLOB(buf);
        return Value(buf);
    }

    default:
        // 未対応型は NULL
        return Value(target_type);
    }
}

} // namespace duckdb
