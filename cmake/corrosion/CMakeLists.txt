# Trimmed vendored entry point (see VENDORING.md): NebulaStream consumes Corrosion via
# add_subdirectory() only, so the upstream top-level concerns (tests, install/package
# rules, standalone options) are removed together with their files.
cmake_minimum_required(VERSION 3.22)
project(Corrosion
    VERSION 0.6.1
    LANGUAGES NONE
    HOMEPAGE_URL "https://corrosion-rs.github.io/corrosion/"
)

list(APPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/cmake)
include(Corrosion)
