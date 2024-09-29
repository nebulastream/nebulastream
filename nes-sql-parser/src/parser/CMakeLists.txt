add_subdirectory(statements)
add_subdirectory(transformer)
add_subdirectory(nodes)
add_subdirectory(expressions)
add_library(nebula_parser_internal OBJECT nebula_parser.cpp)

add_library(nebula_parser SHARED
        $<TARGET_OBJECTS:pg_query>
        $<TARGET_OBJECTS:nebula_expressions>
        $<TARGET_OBJECTS:nebula_nodes>
        $<TARGET_OBJECTS:nebula_statements>
        $<TARGET_OBJECTS:nebula_transformer>
        $<TARGET_OBJECTS:nebula_parser_internal>
)