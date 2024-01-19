### NES Topology Tests ###
add_nes_unit_test(topology-tests
        TopologyTest.cpp
        TopologyPropertyTest.cpp
        TopologyIterationTest.cpp
        Prediction/TopologyTimelineTest.cpp
        Prediction/TopologyLinkInformationTest.cpp
        Prediction/TopologyChangelogTest.cpp
        Prediction/TopologyDeltaTest.cpp)