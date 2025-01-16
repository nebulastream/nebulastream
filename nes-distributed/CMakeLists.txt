include(${PROJECT_SOURCE_DIR}/cmake/PluginRegistrationUtil.cmake)

activate_optional_plugin("Sources/NetworkSource" ON)
activate_optional_plugin("Sinks/NetworkSink" ON)

add_library(nes-distributed bridge/Bridge.cpp)
target_include_directories(nes-distributed
        PRIVATE .
        PUBLIC include
)
include(FetchContent)
FetchContent_Declare(
        Corrosion
        GIT_REPOSITORY https://github.com/corrosion-rs/corrosion.git
        GIT_TAG v0.5 # Optionally specify a commit hash, version tag or branch here
)
FetchContent_MakeAvailable(Corrosion)
# Import targets defined in a package or workspace manifest `Cargo.toml` file
corrosion_import_crate(
        MANIFEST_PATH webulastream/bridge/Cargo.toml
        IMPORTED_CRATES CRATES
        CRATE_TYPES staticlib
)
corrosion_add_cxxbridge(
        cxx_bridge
        CRATE distributed_bridge
        MANIFEST_PATH webulastream/bridge
        FILES lib.rs
)
target_include_directories(cxx_bridge PUBLIC bridge/)
find_package(folly REQUIRED CONFIG)
target_link_libraries(cxx_bridge PUBLIC nes-memory folly::folly)

target_link_libraries(nes-distributed PUBLIC nes-common cxx_bridge)
