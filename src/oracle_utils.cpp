#include "oracle_utils.hpp"
#include "duckdb/common/string_util.hpp"
#include <algorithm>
#include <cctype>
#include <sstream>
#include <stdexcept>

namespace duckdb {

// ─── OracleUtils ──────────────────────────────────────────────────────────────

std::string OracleUtils::FormatOracleError(const std::string &context,
                                             const std::string &oracle_msg) {
    return "Oracle error in " + context + ": " + oracle_msg;
}

std::string OracleUtils::QuoteIdentifier(const std::string &name) {
    return "\"" + name + "\"";
}

std::string OracleUtils::ToUpper(const std::string &s) {
    std::string result = s;
    std::transform(result.begin(), result.end(), result.begin(),
                   [](unsigned char c) { return std::toupper(c); });
    return result;
}

std::unordered_map<std::string, std::string>
OracleUtils::ParseKeyValueString(const std::string &s) {
    std::unordered_map<std::string, std::string> result;
    size_t pos = 0;
    const size_t len = s.size();

    while (pos < len) {
        // スキップ空白
        while (pos < len && std::isspace((unsigned char)s[pos])) ++pos;
        if (pos >= len) break;

        // キー読み取り
        size_t key_start = pos;
        while (pos < len && s[pos] != '=' && !std::isspace((unsigned char)s[pos])) ++pos;
        std::string key = s.substr(key_start, pos - key_start);
        if (key.empty()) { ++pos; continue; }

        // '=' スキップ
        while (pos < len && std::isspace((unsigned char)s[pos])) ++pos;
        if (pos >= len || s[pos] != '=') continue;
        ++pos;
        while (pos < len && std::isspace((unsigned char)s[pos])) ++pos;

        // 値読み取り
        std::string value;
        if (pos < len && s[pos] == '\'') {
            ++pos;
            size_t val_start = pos;
            while (pos < len && s[pos] != '\'') ++pos;
            value = s.substr(val_start, pos - val_start);
            if (pos < len) ++pos; // 閉じクォートをスキップ
        } else {
            size_t val_start = pos;
            while (pos < len && !std::isspace((unsigned char)s[pos])) ++pos;
            value = s.substr(val_start, pos - val_start);
        }

        result[key] = value;
    }
    return result;
}

// ─── OracleConnectionParameters ───────────────────────────────────────────────

OracleConnectionParameters
OracleConnectionParameters::ParseConnectionString(const std::string &conn_str) {
    OracleConnectionParameters params;

    // EasyConnect 形式: "//host[:port][/service]"
    if (conn_str.size() >= 2 && conn_str[0] == '/' && conn_str[1] == '/') {
        return ParseEasyConnect(conn_str);
    }

    auto kv = OracleUtils::ParseKeyValueString(conn_str);

    auto get = [&](const std::string &key, const std::string &default_val = "") {
        auto it = kv.find(key);
        return it != kv.end() ? it->second : default_val;
    };

    params.host         = get("host", "localhost");
    std::string port_s  = get("port", "1521");
    params.port         = std::stoi(port_s);
    params.service_name = get("service", get("service_name"));
    params.sid          = get("sid");
    params.tns_name     = get("tns");
    params.user         = get("user", get("username"));
    params.password     = get("password");
    params.schema       = get("schema");
    params.wallet_location = get("wallet", get("wallet_location"));

    std::string fetch_s = get("fetch_size", "10000");
    params.fetch_size   = std::stoi(fetch_s);

    return params;
}

OracleConnectionParameters
OracleConnectionParameters::ParseEasyConnect(const std::string &conn_str) {
    // 形式: "//host:port/service user=... password=..."
    OracleConnectionParameters params;
    std::string ec_part, kv_part;

    // スペースで分割
    size_t space_pos = conn_str.find(' ');
    if (space_pos != std::string::npos) {
        ec_part = conn_str.substr(0, space_pos);
        kv_part = conn_str.substr(space_pos + 1);
    } else {
        ec_part = conn_str;
    }

    // "//host:port/service" をパース
    std::string ec = ec_part.substr(2); // "//" を除去
    size_t colon = ec.find(':');
    size_t slash = ec.find('/');

    if (colon != std::string::npos && (slash == std::string::npos || colon < slash)) {
        params.host = ec.substr(0, colon);
        size_t slash2 = ec.find('/', colon);
        if (slash2 != std::string::npos) {
            params.port = std::stoi(ec.substr(colon + 1, slash2 - colon - 1));
            params.service_name = ec.substr(slash2 + 1);
        } else {
            params.port = std::stoi(ec.substr(colon + 1));
        }
    } else if (slash != std::string::npos) {
        params.host = ec.substr(0, slash);
        params.service_name = ec.substr(slash + 1);
    } else {
        params.host = ec;
    }

    // 追加 kv パース
    if (!kv_part.empty()) {
        auto kv = OracleUtils::ParseKeyValueString(kv_part);
        auto get = [&](const std::string &key, const std::string &dv = "") {
            auto it = kv.find(key);
            return it != kv.end() ? it->second : dv;
        };
        params.user     = get("user", get("username"));
        params.password = get("password");
        params.schema   = get("schema");
    }

    return params;
}

std::string OracleConnectionParameters::BuildConnectString() const {
    if (!tns_name.empty()) {
        return tns_name;
    }
    // EasyConnect Plus 形式
    std::ostringstream oss;
    oss << "//" << host << ":" << port << "/";
    if (!service_name.empty()) {
        oss << service_name;
    } else if (!sid.empty()) {
        // SID 形式は旧来。EasyConnect では:(SID=xxx) 表記が必要
        oss << "(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=" << host
            << ")(PORT=" << port << "))"
            << "(CONNECT_DATA=(SID=" << sid << ")))";
        return oss.str();
    }
    return oss.str();
}

std::string OracleConnectionParameters::GetEffectiveSchema() const {
    if (!schema.empty()) return OracleUtils::ToUpper(schema);
    return OracleUtils::ToUpper(user);
}

} // namespace duckdb
