#include "oracle_storage.hpp"
#include "oracle_catalog.hpp"

namespace duckdb {

OracleStorageExtension::OracleStorageExtension() {
    attach = OracleCatalog::Attach;
    create_transaction_manager = OracleTransactionManager::Create;
}

} // namespace duckdb
