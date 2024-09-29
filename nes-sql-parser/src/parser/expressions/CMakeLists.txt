project(nebula_expressions)

add_library(nebula_expressions OBJECT parsed_expression.cpp column_expression.cpp star_expression.cpp
        constant_expression.cpp
        comparison_expression.cpp)
