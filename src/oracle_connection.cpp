#include "oracle_connection.hpp"
#include "duckdb/common/exception.hpp"
#include <sstream>
#include <stdexcept>

namespace duckdb {

// ─── グローバルコンテキスト ────────────────────────────────────────────────────
dpiContext *OracleConnection::global_ctx_ = nullptr;
std::mutex  OracleConnection::ctx_mutex_;

dpiContext *OracleConnection::GetOrCreateContext() {
    std::lock_guard<std::mutex> lk(ctx_mutex_);
    if (global_ctx_) return global_ctx_;

    dpiErrorInfo err;
    if (dpiContext_createWithParams(DPI_MAJOR_VERSION, DPI_MINOR_VERSION,
                                    nullptr, &global_ctx_, &err) != DPI_SUCCESS) {
        throw std::runtime_error(std::string("Failed to create ODPI-C context: ") +
                                 err.message);
    }
    return global_ctx_;
}

// ─── Open ─────────────────────────────────────────────────────────────────────

std::shared_ptr<OracleConnection>
OracleConnection::Open(const OracleConnectionParameters &params) {
    auto conn = std::shared_ptr<OracleConnection>(new OracleConnection());
    conn->params_ = params;
    conn->ctx_ = GetOrCreateContext();

    std::string conn_str = params.BuildConnectString();
    dpiErrorInfo err;
    if (dpiConn_create(conn->ctx_,
                       params.user.c_str(),     (uint32_t)params.user.size(),
                       params.password.c_str(), (uint32_t)params.password.size(),
                       conn_str.c_str(),        (uint32_t)conn_str.size(),
                       nullptr, nullptr,
                       &conn->conn_) != DPI_SUCCESS) {
        dpiContext_getError(conn->ctx_, &err);
        throw std::runtime_error(
            OracleUtils::FormatOracleError("OracleConnection::Open", err.message));
    }
    return conn;
}

// ─── Destructor ───────────────────────────────────────────────────────────────

OracleConnection::~OracleConnection() {
    if (conn_) {
        dpiConn_release(conn_);
        conn_ = nullptr;
    }
}

// ─── ThrowIfError ─────────────────────────────────────────────────────────────

void OracleConnection::ThrowIfError(int rc, const std::string &context) {
    if (rc == DPI_SUCCESS) return;
    dpiErrorInfo err;
    dpiContext_getError(ctx_, &err);
    throw std::runtime_error(OracleUtils::FormatOracleError(context, err.message));
}

// ─── GetServerVersion ─────────────────────────────────────────────────────────

std::string OracleConnection::GetServerVersion() {
    dpiVersionInfo vi;
    dpiErrorInfo err;
    if (dpiConn_getServerVersion(conn_, nullptr, nullptr, &vi) != DPI_SUCCESS) {
        dpiContext_getError(ctx_, &err);
        return std::string("unknown: ") + err.message;
    }
    return std::to_string(vi.versionNum) + "." +
           std::to_string(vi.releaseNum) + "." +
           std::to_string(vi.updateNum);
}

int OracleConnection::GetServerMajorVersion() {
    dpiVersionInfo vi;
    if (dpiConn_getServerVersion(conn_, nullptr, nullptr, &vi) != DPI_SUCCESS) {
        return 12; // デフォルト
    }
    return vi.versionNum;
}

// ─── GetTables ────────────────────────────────────────────────────────────────

std::vector<OracleTableInfo>
OracleConnection::GetTables(const std::string &schema) {
    std::lock_guard<std::mutex> lk(mutex_);
    std::vector<OracleTableInfo> tables;

    // ALL_OBJECTS から TABLE と VIEW を取得
    std::string sql =
        "SELECT OBJECT_NAME, OBJECT_TYPE "
        "FROM ALL_OBJECTS "
        "WHERE OWNER = '" + OracleUtils::ToUpper(schema) + "' "
        "  AND OBJECT_TYPE IN ('TABLE', 'VIEW') "
        "ORDER BY OBJECT_NAME";

    dpiStmt *stmt = nullptr;
    ThrowIfError(dpiConn_prepareStmt(conn_, 0, sql.c_str(), (uint32_t)sql.size(),
                                     nullptr, 0, &stmt),
                 "GetTables::prepareStmt");

    ThrowIfError(dpiStmt_execute(stmt, DPI_MODE_EXEC_DEFAULT, nullptr),
                 "GetTables::execute");

    dpiNativeTypeNum name_type, type_type;
    dpiData *name_data, *type_data;
    int found = 0;

    while (dpiStmt_fetch(stmt, &found, nullptr) == DPI_SUCCESS && found) {
        dpiStmt_getQueryValue(stmt, 1, &name_type, &name_data);
        dpiStmt_getQueryValue(stmt, 2, &type_type, &type_data);

        OracleTableInfo info;
        info.schema  = OracleUtils::ToUpper(schema);
        info.name    = std::string(name_data->value.asBytes.ptr,
                                   name_data->value.asBytes.length);
        std::string obj_type(type_data->value.asBytes.ptr,
                             type_data->value.asBytes.length);
        info.is_view = (obj_type == "VIEW");
        tables.push_back(std::move(info));
    }

    dpiStmt_release(stmt);
    return tables;
}

// ─── GetColumns ───────────────────────────────────────────────────────────────

std::vector<OracleColumnInfo>
OracleConnection::GetColumns(const std::string &schema, const std::string &table) {
    std::lock_guard<std::mutex> lk(mutex_);
    std::vector<OracleColumnInfo> columns;

    // ALL_TAB_COLUMNS からカラム情報を取得
    std::string sql =
        "SELECT COLUMN_NAME, DATA_TYPE, DATA_PRECISION, DATA_SCALE, "
        "       CHAR_LENGTH, NULLABLE "
        "FROM ALL_TAB_COLUMNS "
        "WHERE OWNER = '" + OracleUtils::ToUpper(schema) + "' "
        "  AND TABLE_NAME = '" + OracleUtils::ToUpper(table) + "' "
        "ORDER BY COLUMN_ID";

    dpiStmt *stmt = nullptr;
    ThrowIfError(dpiConn_prepareStmt(conn_, 0, sql.c_str(), (uint32_t)sql.size(),
                                     nullptr, 0, &stmt),
                 "GetColumns::prepareStmt");
    ThrowIfError(dpiStmt_execute(stmt, DPI_MODE_EXEC_DEFAULT, nullptr),
                 "GetColumns::execute");

    int found = 0;
    while (dpiStmt_fetch(stmt, &found, nullptr) == DPI_SUCCESS && found) {
        dpiNativeTypeNum t;
        dpiData *d;
        OracleColumnInfo col;

        // COLUMN_NAME
        dpiStmt_getQueryValue(stmt, 1, &t, &d);
        col.name = std::string(d->value.asBytes.ptr, d->value.asBytes.length);

        // DATA_TYPE
        dpiStmt_getQueryValue(stmt, 2, &t, &d);
        col.oracle_type_name = std::string(d->value.asBytes.ptr, d->value.asBytes.length);

        // DATA_PRECISION (nullable)
        dpiStmt_getQueryValue(stmt, 3, &t, &d);
        col.precision = d->isNull ? 0 : (int32_t)d->value.asDouble;

        // DATA_SCALE (nullable)
        dpiStmt_getQueryValue(stmt, 4, &t, &d);
        col.scale = d->isNull ? -127 : (int32_t)d->value.asDouble;

        // CHAR_LENGTH
        dpiStmt_getQueryValue(stmt, 5, &t, &d);
        col.char_length = d->isNull ? 0 : (int32_t)d->value.asDouble;

        // NULLABLE
        dpiStmt_getQueryValue(stmt, 6, &t, &d);
        std::string nullable_str(d->value.asBytes.ptr, d->value.asBytes.length);
        col.nullable = (nullable_str == "Y");

        columns.push_back(std::move(col));
    }

    dpiStmt_release(stmt);
    return columns;
}

// ─── ExecuteQuery ─────────────────────────────────────────────────────────────

void OracleConnection::ExecuteQuery(const std::string &sql,
                                     const std::vector<LogicalType> &types,
                                     idx_t fetch_size,
                                     std::function<bool(DataChunk &)> callback) {
    std::lock_guard<std::mutex> lk(mutex_);

    dpiStmt *stmt = nullptr;
    ThrowIfError(dpiConn_prepareStmt(conn_, 0, sql.c_str(), (uint32_t)sql.size(),
                                     nullptr, 0, &stmt),
                 "ExecuteQuery::prepareStmt");

    // fetch_size のプリフェッチ設定
    dpiStmt_setFetchArraySize(stmt, (uint32_t)fetch_size);

    uint32_t num_cols = 0;
    ThrowIfError(dpiStmt_execute(stmt, DPI_MODE_EXEC_DEFAULT, &num_cols),
                 "ExecuteQuery::execute");

    // 各カラムの native type を取得
    std::vector<dpiQueryInfo> query_infos(num_cols);
    std::vector<dpiNativeTypeNum> native_types(num_cols);
    for (uint32_t i = 0; i < num_cols; ++i) {
        dpiStmt_getQueryInfo(stmt, i + 1, &query_infos[i]);
        // native type を選択
        switch (query_infos[i].typeInfo.oracleTypeNum) {
        case DPI_ORACLE_TYPE_NUMBER:
            // 整数かどうかで分岐
            if (query_infos[i].typeInfo.scale == 0 &&
                query_infos[i].typeInfo.precision > 0 &&
                query_infos[i].typeInfo.precision <= 18) {
                native_types[i] = DPI_NATIVE_TYPE_INT64;
            } else {
                native_types[i] = DPI_NATIVE_TYPE_DOUBLE;
            }
            break;
        case DPI_ORACLE_TYPE_NATIVE_FLOAT:
            native_types[i] = DPI_NATIVE_TYPE_FLOAT;
            break;
        case DPI_ORACLE_TYPE_NATIVE_DOUBLE:
            native_types[i] = DPI_NATIVE_TYPE_DOUBLE;
            break;
        case DPI_ORACLE_TYPE_DATE:
        case DPI_ORACLE_TYPE_TIMESTAMP:
        case DPI_ORACLE_TYPE_TIMESTAMP_TZ:
        case DPI_ORACLE_TYPE_TIMESTAMP_LTZ:
            native_types[i] = DPI_NATIVE_TYPE_TIMESTAMP;
            break;
        case DPI_ORACLE_TYPE_CLOB:
        case DPI_ORACLE_TYPE_NCLOB:
        case DPI_ORACLE_TYPE_BLOB:
            native_types[i] = DPI_NATIVE_TYPE_LOB;
            break;
        case DPI_ORACLE_TYPE_INTERVAL_YM:
            native_types[i] = DPI_NATIVE_TYPE_INTERVAL_YM;
            break;
        case DPI_ORACLE_TYPE_INTERVAL_DS:
            native_types[i] = DPI_NATIVE_TYPE_INTERVAL_DS;
            break;
        default:
            native_types[i] = DPI_NATIVE_TYPE_BYTES;
            break;
        }
    }

    // DataChunk を作成して行を詰める
    DataChunk chunk;
    chunk.Initialize(Allocator::DefaultAllocator(), types);

    int found = 0;
    idx_t row_count = 0;
    const idx_t CHUNK_SIZE = STANDARD_VECTOR_SIZE;

    while (true) {
        if (dpiStmt_fetch(stmt, &found, nullptr) != DPI_SUCCESS || !found) {
            break;
        }

        for (uint32_t col = 0; col < num_cols && col < types.size(); ++col) {
            dpiData *data;
            dpiNativeTypeNum actual_native;
            dpiStmt_getQueryValue(stmt, col + 1, &actual_native, &data);

            Value val = OracleTypeMapping::ToDuckDBValue(data, actual_native, types[col]);
            chunk.SetValue(col, row_count, val);
        }
        ++row_count;

        if (row_count == CHUNK_SIZE) {
            chunk.SetCardinality(row_count);
            if (!callback(chunk)) {
                break;
            }
            chunk.Reset();
            row_count = 0;
        }
    }

    // 残りを flush
    if (row_count > 0) {
        chunk.SetCardinality(row_count);
        callback(chunk);
    }

    dpiStmt_release(stmt);
}

// ─── ExecuteDML ───────────────────────────────────────────────────────────────

void OracleConnection::ExecuteDML(const std::string &sql) {
    std::lock_guard<std::mutex> lk(mutex_);

    dpiStmt *stmt = nullptr;
    ThrowIfError(dpiConn_prepareStmt(conn_, 0, sql.c_str(), (uint32_t)sql.size(),
                                     nullptr, 0, &stmt),
                 "ExecuteDML::prepareStmt");
    uint32_t num_cols = 0;
    int rc = dpiStmt_execute(stmt, DPI_MODE_EXEC_COMMIT_ON_SUCCESS, &num_cols);
    dpiStmt_release(stmt);
    ThrowIfError(rc, "ExecuteDML::execute");
}

// ─── BulkInsert ───────────────────────────────────────────────────────────────

void OracleConnection::BulkInsert(const std::string &table_name,
                                   const std::vector<std::string> &column_names,
                                   DataChunk &chunk) {
    if (chunk.size() == 0) return;

    // VALUES 句を生成 (バインド変数 :1, :2, ...)
    std::ostringstream oss;
    oss << "INSERT INTO " << OracleUtils::QuoteIdentifier(table_name) << " (";
    for (size_t i = 0; i < column_names.size(); ++i) {
        if (i > 0) oss << ", ";
        oss << OracleUtils::QuoteIdentifier(column_names[i]);
    }
    oss << ") VALUES (";
    for (size_t i = 0; i < column_names.size(); ++i) {
        if (i > 0) oss << ", ";
        oss << ":" << (i + 1);
    }
    oss << ")";

    std::string sql = oss.str();
    std::lock_guard<std::mutex> lk(mutex_);

    dpiStmt *stmt = nullptr;
    ThrowIfError(dpiConn_prepareStmt(conn_, 0, sql.c_str(), (uint32_t)sql.size(),
                                     nullptr, 0, &stmt),
                 "BulkInsert::prepareStmt");

    // 行ごとにバインドして実行（Array DML はより効率的だが実装複雑）
    for (idx_t row = 0; row < chunk.size(); ++row) {
        for (size_t col = 0; col < column_names.size(); ++col) {
            Value val = chunk.GetValue(col, row);
            dpiData data;
            data.isNull = val.IsNull();

            if (!val.IsNull()) {
                switch (val.type().id()) {
                case LogicalTypeId::INTEGER:
                    data.value.asInt64 = val.GetValue<int32_t>();
                    dpiStmt_bindValueByPos(stmt, (uint32_t)(col + 1),
                                          DPI_NATIVE_TYPE_INT64, &data);
                    break;
                case LogicalTypeId::BIGINT:
                    data.value.asInt64 = val.GetValue<int64_t>();
                    dpiStmt_bindValueByPos(stmt, (uint32_t)(col + 1),
                                          DPI_NATIVE_TYPE_INT64, &data);
                    break;
                case LogicalTypeId::DOUBLE:
                    data.value.asDouble = val.GetValue<double>();
                    dpiStmt_bindValueByPos(stmt, (uint32_t)(col + 1),
                                          DPI_NATIVE_TYPE_DOUBLE, &data);
                    break;
                case LogicalTypeId::VARCHAR: {
                    auto s = val.GetValue<std::string>();
                    data.value.asBytes.ptr = const_cast<char *>(s.c_str());
                    data.value.asBytes.length = (uint32_t)s.size();
                    dpiStmt_bindValueByPos(stmt, (uint32_t)(col + 1),
                                          DPI_NATIVE_TYPE_BYTES, &data);
                    break;
                }
                default:
                    // NULL として扱う
                    data.isNull = 1;
                    dpiStmt_bindValueByPos(stmt, (uint32_t)(col + 1),
                                          DPI_NATIVE_TYPE_BYTES, &data);
                    break;
                }
            } else {
                dpiStmt_bindValueByPos(stmt, (uint32_t)(col + 1),
                                       DPI_NATIVE_TYPE_BYTES, &data);
            }
        }

        uint32_t dummy_cols = 0;
        dpiStmt_execute(stmt, DPI_MODE_EXEC_DEFAULT, &dummy_cols);
    }

    dpiConn_commit(conn_);
    dpiStmt_release(stmt);
}

// ─── OracleConnectionPool ─────────────────────────────────────────────────────

OracleConnectionPool::OracleConnectionPool(const OracleConnectionParameters &params,
                                             size_t max_connections)
    : params_(params), max_connections_(max_connections) {}

std::shared_ptr<OracleConnection> OracleConnectionPool::Acquire() {
    std::lock_guard<std::mutex> lk(mutex_);
    if (!pool_.empty()) {
        auto conn = pool_.back();
        pool_.pop_back();
        return conn;
    }
    // プールが空なら新規接続
    return OracleConnection::Open(params_);
}

void OracleConnectionPool::Release(std::shared_ptr<OracleConnection> conn) {
    std::lock_guard<std::mutex> lk(mutex_);
    if (pool_.size() < max_connections_) {
        pool_.push_back(std::move(conn));
    }
    // プールが満杯なら conn は破棄（デストラクタで切断）
}

void OracleConnectionPool::ClearCache() {
    std::lock_guard<std::mutex> lk(mutex_);
    pool_.clear();
}

} // namespace duckdb
