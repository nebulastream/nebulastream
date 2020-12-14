add_source_nes(
        LogicalOperatorNode.cpp
        FilterLogicalOperatorNode.cpp
        ProjectionLogicalOperatorNode.cpp
        MapLogicalOperatorNode.cpp
        MergeLogicalOperatorNode.cpp
        LogicalOperatorFactory.cpp
        BroadcastLogicalOperatorNode.cpp
        JoinLogicalOperatorNode.cpp
        WatermarkAssignerLogicalOperatorNode.cpp
)

add_subdirectory(Sinks)
add_subdirectory(Sources)
add_subdirectory(Windowing)