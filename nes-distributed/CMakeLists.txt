include(${PROJECT_SOURCE_DIR}/cmake/PluginRegistrationUtil.cmake)

activate_optional_plugin("Sources/NetworkSource" ON)
activate_optional_plugin("Sinks/NetworkSink" ON)

add_library(nes-distributed network/Bridge.cpp)
target_include_directories(nes-distributed
        PRIVATE .
        PUBLIC include
)

corrosion_add_cxxbridge(
        cxx_bridge
        CRATE bridge_of_bridges
        FILES ../network/lib.rs
)
target_include_directories(cxx_bridge PUBLIC network/include)
target_link_libraries(cxx_bridge PUBLIC nes-memory)

target_link_libraries(nes-distributed PUBLIC nes-common cxx_bridge)
