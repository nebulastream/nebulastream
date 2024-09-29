project(parser_tests)

enable_testing()

add_executable(parser_tests simple_select_query.cpp
        select_query_with_where_test.cpp
        all_tests.cpp
        query_parsing_test.cpp)

target_link_libraries(parser_tests nebula_parser)
target_link_libraries(parser_tests GTest::gtest_main)

file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/queries.sql DESTINATION ${CMAKE_CURRENT_BINARY_DIR})