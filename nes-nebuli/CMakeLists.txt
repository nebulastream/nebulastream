# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_subdirectory(src)

find_program(CLANG_EXECUTABLE NAMES clang clang-18 REQUIRED)
message(STATUS "Found Clang Binary: ${CLANG_EXECUTABLE}")

set(PATH_TO_BINARY_DIR ${CMAKE_BINARY_DIR})
set(PATH_TO_NES_SOURCE_CODE ${PROJECT_SOURCE_DIR})
set(PATH_TO_DEB_SOURCE_CODE ${_VCPKG_INSTALLED_DIR}/${VCPKG_TARGET_TRIPLET})

get_source(nes-clients CLIENT_SOURCE_FILES)
get_source(nes-catalogs CATALOG_SOURCE_FILES)
get_source(nes-optimizer OPTIMIZER_SOURCE_FILES)
get_source(nes-operators OPERATORS_SOURCE_FILES)
get_source(nes-coordinator COORDINATOR_SOURCE_FILES)

add_library(nes-nebuli-lib STATIC src/NebuLI.cpp ${CATALOG_SOURCE_FILES} ${OPTIMIZER_SOURCE_FILES} ${OPERATORS_SOURCE_FILES} ${COORDINATOR_SOURCE_FILES} ${CLIENT_SOURCE_FILES})

target_compile_definitions(nes-nebuli-lib PRIVATE PATH_TO_BINARY_DIR="${PATH_TO_BINARY_DIR}")
target_compile_definitions(nes-nebuli-lib PRIVATE CLANG_EXECUTABLE="${CLANG_EXECUTABLE}")
target_compile_definitions(nes-nebuli-lib PRIVATE PATH_TO_NES_SOURCE_CODE="${PATH_TO_NES_SOURCE_CODE}")

find_package(yaml-cpp CONFIG REQUIRED)
find_package(argparse CONFIG REQUIRED)

target_include_directories(nes-nebuli-lib
        PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        PRIVATE $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/private>)

target_link_libraries(nes-nebuli-lib
        PUBLIC nes-operators
        PRIVATE nes-sql-parser yaml-cpp::yaml-cpp nes-client
)

add_executable(nes-nebuli src/NebuLIStarter.cpp)
# Nebuli loads a Query using `dlopen` this requires the executable to be dynamic
set_property(TARGET nes-nebuli PROPERTY ENABLE_EXPORTS ON)
target_link_libraries(nes-nebuli PUBLIC nes-nebuli-lib argparse::argparse)
target_include_directories(nes-nebuli
        PUBLIC $<INSTALL_INTERFACE:/include/nebulastream>
)


add_tests_if_enabled(tests)
