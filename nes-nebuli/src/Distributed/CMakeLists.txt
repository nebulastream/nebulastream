add_source_files(nes-optimizer
        Topology.cpp
        Placement.cpp
)

add_nes_common_test("TopologyTest" TopologyTest.cpp)
target_link_libraries(TopologyTest nes-nebuli)

add_nes_common_test("PlacementTest" PlacementTest.cpp)
target_link_libraries(PlacementTest nes-nebuli nes-sql-parser)
target_include_directories(PlacementTest PRIVATE ../../private)
