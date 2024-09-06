add_library(duckdb_pg_query STATIC
            postgres_parser.cpp
            pg_functions.cpp
            src_backend_parser_parser.cpp
            src_backend_parser_gram.cpp
            src_backend_nodes_list.cpp
            src_backend_parser_scansup.cpp
            src_backend_nodes_makefuncs.cpp
            src_backend_nodes_value.cpp
            src_backend_parser_scan.cpp
            src_common_keywords.cpp)

target_include_directories(
  duckdb_pg_query
  PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>)