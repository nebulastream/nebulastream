add_source_nes(
        LogicalOperatorNode.cpp
        FilterLogicalOperatorNode.cpp
        MapLogicalOperatorNode.cpp
        WindowLogicalOperatorNode.cpp
        MergeLogicalOperatorNode.cpp
        LogicalOperatorFactory.cpp
        BroadcastLogicalOperatorNode.cpp
)

add_subdirectory(Sinks)
add_subdirectory(Sources)
add_subdirectory(SpecializedWindowOperators)