project(nebula_transformer)
add_library(nebula_transformer OBJECT transformer.cpp
        transform_select.cpp
        transform_expression.cpp
        transform_operator.cpp
        transform_columnref.cpp
        transform_tableref.cpp
        transform_constant.cpp)