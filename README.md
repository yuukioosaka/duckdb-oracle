# DuckDB Oracle Extension

Oracle Database へ直接接続する DuckDB 拡張機能。`ATTACH` 構文で Oracle に接続し、SQL でシームレスにクエリできます。

## クイックスタート

```sql
-- Oracle に接続
ATTACH 'host=myhost port=1521 service=ORCL user=scott password=tiger'
  AS oracle_db (TYPE oracle);

-- テーブルをクエリ
SELECT * FROM oracle_db.HR.EMPLOYEES WHERE DEPARTMENT_ID = 90;

-- DuckDB へコピー
CREATE TABLE local_emp AS SELECT * FROM oracle_db.HR.EMPLOYEES;

-- Parquet にエクスポート
COPY oracle_db.HR.EMPLOYEES TO 'employees.parquet';

-- 切断
DETACH oracle_db;
```

## 接続文字列の形式

### キー・バリュー形式
```
host=<host> port=<port> service=<service_name> user=<user> password=<pass>
```

### EasyConnect 形式（Oracle 12c+）
```
//host:port/service user=<user> password=<pass>
```

### TNS 別名
```
tns=<alias> user=<user> password=<pass>
```

### SID 使用（旧来方式）
```
host=<host> port=1521 sid=<SID> user=<user> password=<pass>
```

## ATTACH オプション

| オプション | 説明 | デフォルト |
|-----------|------|-----------|
| `TYPE oracle` | Oracle 拡張を使用 | 必須 |
| `READ_ONLY` | 読み取り専用モード | なし |
| `SCHEMA 'HR'` | デフォルトスキーマを指定 | ユーザー名 |

## 対応する操作

| 操作 | サポート |
|------|---------|
| SELECT | ✅ |
| WHERE フィルタ (Pushdown) | ✅ |
| LIMIT / OFFSET (Pushdown) | ✅ |
| INSERT | ✅ |
| UPDATE / DELETE | ✅ |
| CREATE TABLE | ✅ |
| DROP TABLE | ✅ |
| COPY FROM parquet | ✅ |

## 型マッピング

| Oracle 型 | DuckDB 型 |
|-----------|-----------|
| `NUMBER(p,0)` | `INTEGER` / `BIGINT` / `HUGEINT` |
| `NUMBER(p,s)` | `DECIMAL(p,s)` |
| `NUMBER` | `DOUBLE` |
| `VARCHAR2` | `VARCHAR` |
| `NVARCHAR2` | `VARCHAR` |
| `DATE` | `TIMESTAMP` ※ |
| `TIMESTAMP` | `TIMESTAMP` |
| `TIMESTAMP WITH TIME ZONE` | `TIMESTAMPTZ` |
| `CLOB` | `VARCHAR` |
| `BLOB` | `BLOB` |
| `BINARY_FLOAT` | `FLOAT` |
| `BINARY_DOUBLE` | `DOUBLE` |

> **注**: Oracle の `DATE` 型は時刻情報を含むため `TIMESTAMP` にマップします。

## ユーティリティ関数

```sql
-- 接続情報を表示
SELECT * FROM oracle_info('oracle_db');

-- スキーマキャッシュをクリア（テーブル追加後など）
SELECT oracle_clear_cache('oracle_db');
```

## ビルド方法

```bash
# リポジトリをクローン
git clone --recurse-submodules https://github.com/yourname/duckdb-oracle.git
cd duckdb-oracle

# DuckDB もサブモジュールとして取得
git submodule update --init --recursive

# ビルド
make

# テスト
make test
```

### 前提条件

- **ビルド時**: CMake 3.5+, C++17 コンパイラ
- **実行時**: Oracle Instant Client (Basic または Basic Light パッケージ)
  - [Oracle Instant Client ダウンロード](https://www.oracle.com/database/technologies/instant-client/downloads.html)

Oracle Instant Client のパスを環境変数に設定:
```bash
# Linux
export LD_LIBRARY_PATH=/opt/oracle/instantclient_21_9:$LD_LIBRARY_PATH

# macOS
export DYLD_LIBRARY_PATH=/opt/oracle/instantclient_21_9:$DYLD_LIBRARY_PATH
```

## アーキテクチャ

```
oracle_extension.cpp  ← エントリポイント
      │
      ├── OracleStorageExtension  (StorageExtension 登録)
      │         │
      │         └── OracleCatalog  (ATTACH → Catalog)
      │                   │
      │                   └── OracleSchemaEntry  (スキーマ)
      │                             │
      │                             └── OracleTableEntry  (テーブル)
      │                                       │
      │                                       └── OracleScan  (スキャン)
      │
      └── OracleConnection  (ODPI-C ラッパー)
                │
                └── ODPI-C → Oracle Instant Client (OCI)
```

## ライセンス

MIT License

## 参考

- [ODPI-C ドキュメント](https://oracle.github.io/odpi/)
- [DuckDB 拡張開発ガイド](https://duckdb.org/docs/extensions/creating_extensions)
- [postgres_scanner](https://github.com/duckdb/duckdb-postgres)
