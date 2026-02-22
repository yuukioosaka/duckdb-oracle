#pragma once

#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include <string>
#include <unordered_map>

namespace duckdb {

// ───────────────────────────────────────────────────────────────────────────────
// 接続パラメータ
// ───────────────────────────────────────────────────────────────────────────────
struct OracleConnectionParameters {
    std::string host        = "localhost";
    int         port        = 1521;
    std::string service_name;       // SERVICE_NAME (推奨)
    std::string sid;                // SID (旧来方式)
    std::string tns_name;           // TNS エイリアス
    std::string user;
    std::string password;
    std::string wallet_location;    // SSL/TLS ウォレットパス
    std::string schema;             // ATTACHするスキーマ (未指定=user)
    bool        read_only = false;
    int         fetch_size = 10000; // 一度に取得する行数

    // "host=... port=... service=... user=... password=..." 形式をパース
    static OracleConnectionParameters ParseConnectionString(const std::string &conn_str);

    // EasyConnect 形式 "//host:port/service" をパース
    static OracleConnectionParameters ParseEasyConnect(const std::string &conn_str);

    // ODPI-C 向け接続文字列を組み立てる
    std::string BuildConnectString() const;

    // スキーマが未設定の場合はユーザー名（大文字）を返す
    std::string GetEffectiveSchema() const;
};

// ───────────────────────────────────────────────────────────────────────────────
// 雑多なユーティリティ
// ───────────────────────────────────────────────────────────────────────────────
class OracleUtils {
public:
    // Oracle エラーを DuckDB 例外に変換
    static std::string FormatOracleError(const std::string &context,
                                         const std::string &oracle_msg);

    // Oracle の識別子（テーブル名等）をクォートする
    static std::string QuoteIdentifier(const std::string &name);

    // 文字列を大文字に変換（Oracle はデフォルト大文字）
    static std::string ToUpper(const std::string &s);

    // キーバリュー文字列をパース ("key=val key2='val 2'")
    static std::unordered_map<std::string, std::string>
        ParseKeyValueString(const std::string &s);
};

} // namespace duckdb
