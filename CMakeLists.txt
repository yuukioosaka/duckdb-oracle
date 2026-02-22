cmake_minimum_required(VERSION 3.5)

set(TARGET_NAME oracle_scanner)
set(EXTENSION_NAME ${TARGET_NAME})
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include)

# ─── ODPI-C (third_party サブモジュール) ──────────────────────────────────────
# ODPI-C はヘッダオンリーではなく単一 C ファイルをコンパイルする
set(ODPI_SRC ${CMAKE_CURRENT_SOURCE_DIR}/third_party/odpi/src)
set(ODPI_INC ${CMAKE_CURRENT_SOURCE_DIR}/third_party/odpi/include)

file(GLOB ODPI_SOURCES "${ODPI_SRC}/*.c")

add_library(odpi STATIC ${ODPI_SOURCES})
target_include_directories(odpi PUBLIC ${ODPI_INC})
# ODPI-C は OCI を実行時に dlopen するので、コンパイル時リンクは不要
# ただし pthread は必要
find_package(Threads REQUIRED)
target_link_libraries(odpi Threads::Threads ${CMAKE_DL_LIBS})

# ─── 拡張ソース ────────────────────────────────────────────────────────────────
set(EXTENSION_SOURCES
    src/oracle_extension.cpp
    src/oracle_connection.cpp
    src/oracle_type_mapping.cpp
    src/oracle_catalog.cpp
    src/oracle_schema_entry.cpp
    src/oracle_table_entry.cpp
    src/oracle_scan.cpp
    src/oracle_storage.cpp
    src/oracle_utils.cpp
    src/oracle_optimizer.cpp
)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# ─── リンク ────────────────────────────────────────────────────────────────────
target_include_directories(${EXTENSION_NAME} PRIVATE ${ODPI_INC})
target_link_libraries(${EXTENSION_NAME} odpi)

target_include_directories(${LOADABLE_EXTENSION_NAME} PRIVATE ${ODPI_INC})
target_link_libraries(${LOADABLE_EXTENSION_NAME} odpi)

install(
    TARGETS ${EXTENSION_NAME}
    EXPORT "${DUCKDB_EXPORT_SET}"
    LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
    ARCHIVE DESTINATION "${INSTALL_LIB_DIR}"
)
