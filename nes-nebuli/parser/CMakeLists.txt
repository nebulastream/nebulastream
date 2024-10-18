find_package(antlr4-runtime CONFIG REQUIRED)
add_library(sql-parser NebulaSQLLexer.cpp NebulaSQLParser.cpp NebulaSQLBaseListener.cpp NebulaSQLListener.cpp)
target_link_libraries(sql-parser PRIVATE antlr4_static nes-common)
target_include_directories(sql-parser PUBLIC . ${ANTLR4_INCLUDE_DIR})