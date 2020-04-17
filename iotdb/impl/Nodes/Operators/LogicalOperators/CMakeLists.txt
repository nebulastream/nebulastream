add_source_iotdb(
        LogicalOperatorNode.cpp
        FilterLogicalOperatorNode.cpp
        MapLogicalOperatorNode.cpp
        WindowLogicalOperatorNode.cpp
)

add_subdirectory(Sinks)
add_subdirectory(Sources)