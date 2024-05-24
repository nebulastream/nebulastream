# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

message(STATUS "Enable Tensorflow Plugin")

find_package(tensorflow-lite-c REQUIRED)

add_subdirectory(src)
get_source(nes-tensorflow NES_COMPILER_SOURCE_FILES)
add_library(nes-tensorflow SHARED
        ${NES_COMPILER_SOURCE_FILES}
)
target_link_libraries(nes-tensorflow PRIVATE nes-execution nes-client tensorflowlite_c::tensorflowlite_c)
add_dependencies(build_all_plugins nes-tensorflow)

target_include_directories(nes-tensorflow PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include/nebulastream/>)

set_target_properties(nes-tensorflow PROPERTIES
        LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/nes-plugins/)


if (NES_ENABLES_TESTS)
    add_compile_definitions(TEST_DATA_DIRECTORY="${CMAKE_CURRENT_SOURCE_DIR}/tests/testData/")
    # Add tests with command
    add_subdirectory(tests)
    message(STATUS "Tests are enabled")
else ()
    message(STATUS "Tests are disabled")
endif ()

#Add the nes lib and headers to cpack
install(TARGETS nes-tensorflow
        EXPORT NebulaStreamTargets
        LIBRARY
        DESTINATION ${CMAKE_INSTALL_BINDIR}/nes-plugins
        COMPONENT nesLibrary)

# Include tensorflow library in the deb package and install it in the lib folder
install(IMPORTED_RUNTIME_ARTIFACTS tensorflowlite_c::tensorflowlite_c DESTINATION ${CMAKE_INSTALL_LIBDIR})

