# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

message(STATUS "Enable Arrow Plugin")

if (CMAKE_SYSTEM_NAME STREQUAL "Linux" AND NOT (CMAKE_SYSTEM_PROCESSOR MATCHES "arm" OR CMAKE_SYSTEM_PROCESSOR MATCHES "aarch"))
    # find Arrow package
    find_package(Arrow REQUIRED CONFIG)
    if (Arrow_FOUND)
        message(STATUS "Arrow version ${ARROW_VERSION}")
    else ()
        message(FATAL_ERROR "Unable to find Arrow library")
    endif ()
    set(ARROW_LIBRARIES ${ARROW_LIBRARIES} arrow_static)

    # explicitly find the brotli package from Arrow and include it
    find_package(unofficial-brotli REQUIRED)
    if (unofficial-brotli_FOUND)
        message(STATUS "Brotli lib required by Arrow found.")
        set(ARROW_LIBRARIES ${ARROW_LIBRARIES} unofficial::brotli::brotlienc-static unofficial::brotli::brotlidec-static unofficial::brotli::brotlicommon-static)
    else ()
        message(FATAL_ERROR "Brotli lib required by Arrow not found.")
    endif ()

    add_compile_definitions(ENABLE_ARROW_BUILD)
else ()
    message(FATAL_ERROR "Arrow plugin not supported on this platform")
endif ()

add_subdirectory(src)
get_source(nes-arrow NES_COMPILER_SOURCE_FILES)

add_library(nes-arrow SHARED ${NES_COMPILER_SOURCE_FILES})
add_dependencies(build_all_plugins nes-arrow)

target_include_directories(nes-arrow PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include/nebulastream/>)

set_target_properties(nes-arrow PROPERTIES
        LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/nes-plugins/)

target_link_libraries(nes-arrow PUBLIC nes-runtime nes-data-types nes-catalogs nes-operators nes-configurations ${ARROW_LIBRARIES} )

if (NES_ENABLES_TESTS)
    # Add tests with command
    add_compile_definitions(TEST_DATA_DIRECTORY="${CMAKE_CURRENT_SOURCE_DIR}/tests/testData/")

    ### ArrowSourceIntegrationTest ###
    add_nes_integration_test(arrow-source-integration-test "tests/Integration/Sources/ArrowSourceIntegrationTest.cpp")
    target_link_libraries(arrow-source-integration-test nes-arrow )
    add_subdirectory(tests/UnitTests)

    message(STATUS "Tests are enabled")
else ()
    message(STATUS "Tests are disabled")
endif ()

#Add the nes lib and headers to cpack
install(TARGETS nes-arrow
        EXPORT NebulaStreamTargets
        LIBRARY
        DESTINATION ${CMAKE_INSTALL_BINDIR}/nes-plugins
        COMPONENT nesLibrary)