# DuckDB Oracle Extension 設計書

## 概要

DuckDB から Oracle Database へ直接アクセスできる拡張機能（`oracle_scanner`）の設計書。
postgres_scanner・mysql_scanner と同じ `StorageExtension` パターンを採用し、`ATTACH` 構文でシームレスに Oracle に接続できることを目標とする。

---

## 先行事例の調査

| 拡張 | 接続ライブラリ | ATTACH TYPE | 読み書き | フィルタ Pushdown |
|------|--------------|-------------|--------|-----------------|
| postgres_scanner | libpq (binary protocol) | `postgres` | ✅ | ✅ (実験的) |
| mysql_scanner | MySQL C Connector | `mysql` | ✅ | ✅ |
| odbc_scanner | unixODBC | N/A (関数ベース) | 読み取りのみ | ❌ |
| ofquack | SOAP/WSDL | N/A | 読み取りのみ | ❌ |

Oracle 専用拡張はまだ存在しない。本拡張で初の本格実装となる。

---

## アーキテクチャ方針

### 接続ライブラリ選択

**ODPI-C（Oracle Database Programming Interface for C）を採用**

| 選択肢 | メリット | デメリット |
|--------|---------|-----------|
| **ODPI-C** | Oracle 公式・軽量・再配布可能・OCI ラッパー | OCI クライアント要 |
| OCI (直接) | 最高性能 | 複雑・Oracleライセンス問題 |
| ODBC (unixODBC) | 汎用的 | 型変換コスト大・パフォーマンス低 |
| JDBC (JNI) | 広く使われる | JVM 依存・複雑 |

ODPI-C は Oracle が公式サポートするオープンソース C ライブラリで、静的リンクが可能。
postgres_scanner が libpq を静的リンクしているのと同様のアプローチが取れる。

---

## コンポーネント設計

```
duckdb-oracle/
├── CMakeLists.txt
├── extension_config.cmake
├── src/
│   ├── oracle_extension.cpp          # エントリポイント・拡張登録
│   ├── oracle_connection.hpp/cpp     # 接続管理 (ODPI-C ラッパー)
│   ├── oracle_utils.hpp/cpp          # 接続文字列パース・型変換
│   ├── oracle_catalog.hpp/cpp        # DuckDB Catalog 実装
│   ├── oracle_schema_entry.hpp/cpp   # スキーマエントリ
│   ├── oracle_table_entry.hpp/cpp    # テーブルエントリ
│   ├── oracle_scan.hpp/cpp           # テーブルスキャン (TableFunction)
│   ├── oracle_query.hpp/cpp          # クエリ実行・結果フェッチ
│   ├── oracle_optimizer.hpp/cpp      # Pushdown 最適化
│   ├── oracle_storage.hpp/cpp        # StorageExtension 実装
│   └── oracle_type_mapping.hpp/cpp   # Oracle ⇔ DuckDB 型マッピング
├── third_party/
│   └── odpi/                         # ODPI-C サブモジュール
└── test/
    └── sql/
        ├── oracle_attach.test
        ├── oracle_types.test
        ├── oracle_pushdown.test
        └── oracle_write.test
```

---

## ユーザーインターフェース設計

### 1. ATTACH 構文

```sql
-- 基本接続
ATTACH 'host=localhost port=1521 sid=ORCL user=scott password=tiger'
  AS oracle_db (TYPE oracle);

-- TNS接続文字列
ATTACH 'tns=mydb user=scott password=tiger'
  AS oracle_db (TYPE oracle, READ_ONLY);

-- Easy Connect Plus (Oracle 19c+)
ATTACH '//myhost:1521/myservice user=scott password=tiger'
  AS oracle_db (TYPE oracle);

-- 特定スキーマのみ
ATTACH 'host=localhost port=1521 service=mydb user=scott password=tiger'
  AS oracle_db (TYPE oracle, SCHEMA 'HR');
```

### 2. SECRET による認証管理

```sql
CREATE SECRET oracle_secret (
    TYPE oracle,
    HOST 'myhost',
    PORT 1521,
    SERVICE 'myservice',
    USER 'scott',
    PASSWORD 'tiger'
);

ATTACH '' AS oracle_db (TYPE oracle, SECRET oracle_secret);
```

### 3. クエリ

```sql
USE oracle_db;

-- テーブル一覧
SHOW TABLES;

-- 通常クエリ（裏で Oracle に実行）
SELECT * FROM oracle_db.HR.EMPLOYEES WHERE DEPARTMENT_ID = 90;

-- DuckDB へコピー
CREATE TABLE local_emp AS SELECT * FROM oracle_db.HR.EMPLOYEES;

-- Parquet エクスポート
COPY oracle_db.HR.EMPLOYEES TO 'employees.parquet';
```

### 4. ユーティリティ関数

```sql
-- キャッシュクリア
SELECT oracle_clear_cache('oracle_db');

-- 接続情報確認
SELECT * FROM oracle_info('oracle_db');

-- 直接クエリ実行（Oracle SQL 方言で）
SELECT * FROM oracle_query('oracle_db', 'SELECT * FROM HR.EMPLOYEES WHERE ROWNUM <= 10');
```

---

## 型マッピング設計

| Oracle 型 | DuckDB 型 | 備考 |
|-----------|-----------|------|
| `NUMBER(p, 0)` | `BIGINT` / `INTEGER` | 精度による |
| `NUMBER(p, s)` | `DECIMAL(p, s)` | |
| `NUMBER` (精度なし) | `DOUBLE` | |
| `VARCHAR2` | `VARCHAR` | |
| `NVARCHAR2` | `VARCHAR` | UTF-8変換 |
| `CHAR` | `VARCHAR` | トリム処理 |
| `DATE` | `TIMESTAMP` | Oracleの DATE は時刻含む |
| `TIMESTAMP` | `TIMESTAMP` | |
| `TIMESTAMP WITH TIME ZONE` | `TIMESTAMPTZ` | |
| `CLOB` | `VARCHAR` | ストリーム読み取り |
| `BLOB` | `BLOB` | |
| `RAW` | `BLOB` | |
| `FLOAT` | `DOUBLE` | |
| `BINARY_FLOAT` | `FLOAT` | |
| `BINARY_DOUBLE` | `DOUBLE` | |
| `INTERVAL YEAR TO MONTH` | `INTERVAL` | |
| `ROWID` | `VARCHAR` | |

---

## コアクラス設計

### OracleConnectionParameters

```cpp
struct OracleConnectionParameters {
    string host;
    int port = 1521;
    string service_name;   // SERVICE_NAME
    string sid;            // SID (旧来)
    string tns_name;       // TNS alias
    string user;
    string password;
    string wallet_location; // SSL/TLS ウォレット
    bool read_only = false;
    string schema;         // ATTACHするスキーマ
    
    static OracleConnectionParameters ParseConnectionString(const string &conn_str);
};
```

### OracleConnection

```cpp
class OracleConnection {
public:
    static shared_ptr<OracleConnection> Open(const OracleConnectionParameters &params);
    
    // クエリ実行 → DataChunk のベクターで返す
    void ExecuteQuery(const string &query, 
                      const vector<LogicalType> &types,
                      std::function<void(DataChunk &)> callback);
    
    // スキーマ情報取得
    vector<OracleTableInfo> GetTables(const string &schema);
    vector<OracleColumnInfo> GetColumns(const string &schema, const string &table);
    
private:
    dpiConn *conn_;          // ODPI-C接続ハンドル
    dpiContext *ctx_;        // ODPI-C コンテキスト
    mutex query_lock_;       // スレッドセーフ用
};
```

### StorageExtension 登録（エントリポイント）

```cpp
void OracleExtension::Load(DuckDB &db) {
    // StorageExtension 登録
    auto &config = DBConfig::GetConfig(*db.instance);
    
    auto oracle_storage = make_uniq<StorageExtension>();
    oracle_storage->attach = OracleCatalog::Attach;
    oracle_storage->create_transaction_manager = OracleTransactionManager::Create;
    config.storage_extensions["oracle"] = std::move(oracle_storage);
    
    // SECRET プロバイダー登録
    CreateSecretFunction oracle_secret("oracle", OracleSecretManager::CreateSecret);
    SecretManager::Get(*db.instance).RegisterSecretType(oracle_secret);
    
    // ユーティリティ関数登録
    RegisterOracleUtilityFunctions(*db.instance);
}
```

---

## クエリ最適化（Pushdown）

`OracleOptimizer` を DuckDB のオプティマイザパイプラインに組み込み、以下をプッシュダウン：

```
Pushdown 対象:
├── フィルタ (WHERE 句)
│   ├── 比較演算子: =, <, >, <=, >=, !=
│   ├── IN / NOT IN
│   ├── IS NULL / IS NOT NULL
│   └── LIKE (Oracleと互換する場合)
├── プロジェクション (SELECT カラム選択)
├── LIMIT / OFFSET → ROWNUM / FETCH FIRST (バージョン対応)
└── ORDER BY
```

Oracle バージョン対応：
- **Oracle 12c+**: `FETCH FIRST n ROWS ONLY`
- **Oracle 11g 以前**: `ROWNUM <= n` で代替

---

## 並列スキャン設計

postgres_scanner 同様、大テーブルの並列読み取りをサポート：

```
並列化戦略:
1. ROWID 範囲分割 (デフォルト)
   → テーブルの ROWID 範囲を DuckDB のスレッド数で分割
   
2. PARTITION 分割 (パーティションテーブル)
   → Oracle パーティション情報を取得して各スレッドに割り当て
   
設定パラメータ:
- oracle_pages_per_task: タスクあたりの取得行数（デフォルト: 100000）
- oracle_max_threads: 最大並列スレッド数
```

---

## 書き込みサポート

| DuckDB 操作 | Oracle 変換 | 備考 |
|-------------|------------|------|
| `INSERT INTO oracle_db.tbl` | `INSERT INTO tbl` | バッチ insert |
| `UPDATE oracle_db.tbl SET ...` | `UPDATE tbl SET ...` | |
| `DELETE FROM oracle_db.tbl` | `DELETE FROM tbl` | |
| `CREATE TABLE oracle_db.tbl` | `CREATE TABLE tbl` | 型変換あり |
| `DROP TABLE oracle_db.tbl` | `DROP TABLE tbl` | |
| `COPY oracle_db.tbl FROM 'file.parquet'` | バッチ INSERT | Array DML 使用 |

---

## ビルド設計

```cmake
# CMakeLists.txt の主要部分

# ODPI-C をサブモジュールとして静的リンク
add_subdirectory(third_party/odpi)

# Oracle Instant Client が必要（実行時）
# → OCI ライブラリへの依存はランタイムのみ（dlopen）

duckdb_extension_load(oracle_scanner
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
    LINKED_LIBS odpi
)
```

**実行時の依存関係：**
- Oracle Instant Client（ユーザーが別途インストール）
- ODPI-C が OCI を動的ロードするため、コンパイル時のリンクは不要

---

## スコープ・実装フェーズ

### Phase 1（MVP）
- [ ] ODPI-C サブモジュール統合
- [ ] 接続パラメータ解析
- [ ] StorageExtension / Catalog 実装
- [ ] 基本的なテーブルスキャン（全件取得）
- [ ] 主要型マッピング
- [ ] READ_ONLY モード

### Phase 2
- [ ] フィルタ / プロジェクション Pushdown
- [ ] SECRET サポート
- [ ] LIMIT Pushdown（Oracle バージョン対応）
- [ ] スキーマキャッシュ + `oracle_clear_cache()`

### Phase 3
- [ ] 書き込みサポート（INSERT / UPDATE / DELETE）
- [ ] 並列スキャン（ROWID 分割）
- [ ] CLOB / BLOB ストリーミング
- [ ] `oracle_query()` 直接クエリ関数

### Phase 4
- [ ] パーティション並列スキャン
- [ ] SSL / ウォレット認証
- [ ] Community Extensions への登録

---

## 既存実装との差別化

| | odbc_scanner (Oracle ODBC) | ofquack | 本拡張 |
|--|--------------------------|---------|--------|
| ATTACH 構文 | ❌ | ❌ | ✅ |
| 書き込み | ❌ | ❌ | ✅ |
| フィルタ Pushdown | ❌ | ❌ | ✅ |
| 並列スキャン | ❌ | ❌ | ✅ |
| 型変換 | 基本的 | VARCHAR のみ | 完全 |
| Secret 管理 | ❌ | ❌ | ✅ |
| Oracle SQL 方言 | ✅ | Oracle Fusion のみ | ✅ |

---

## 参考リポジトリ

- [duckdb/duckdb-postgres](https://github.com/duckdb/duckdb-postgres) — メイン参考実装
- [duckdb/duckdb-mysql](https://github.com/duckdb/duckdb-mysql) — 型マッピング参考
- [oracle/odpi](https://github.com/oracle/odpi) — ODPI-C ライブラリ
- [krokozyab/ofquack](https://github.com/krokozyab/ofquack) — Oracle Fusion 接続参考
- [duckdb/extension-template](https://github.com/duckdb/extension-template) — 拡張テンプレート