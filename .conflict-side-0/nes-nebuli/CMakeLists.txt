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

get_source(nes-clients CLIENT_SOURCE_FILES)
get_source(nes-catalogs CATALOG_SOURCE_FILES)
get_source(nes-legacy-optimizer LEGACYOPTIMIZER_SOURCE_FILES)
get_source(nes-coordinator COORDINATOR_SOURCE_FILES)
get_source(nes-yaml-binder YAML_BINDER_SOURCE_FILES)
get_source(nes-query-manager-grpc QUERY_MANAGER_GRPC_SOURCE_FILES)
get_source(nes-query-manager-embedded QUERY_MANAGER_EMBEDDED_SOURCE_FILES)

add_library(nes-nebuli-lib STATIC ${QUERY_MANAGER_GRPC_SOURCE_FILES} ${CATALOG_SOURCE_FILES} ${LEGACYOPTIMIZER_SOURCE_FILES} ${OPERATORS_SOURCE_FILES} ${COORDINATOR_SOURCE_FILES} ${CLIENT_SOURCE_FILES} ${YAML_BINDER_SOURCE_FILES})
add_library(nes-nebuli-embedded-lib STATIC ${QUERY_MANAGER_GRPC_SOURCE_FILES} ${CATALOG_SOURCE_FILES} ${LEGACYOPTIMIZER_SOURCE_FILES} ${OPERATORS_SOURCE_FILES} ${COORDINATOR_SOURCE_FILES} ${CLIENT_SOURCE_FILES} ${YAML_BINDER_SOURCE_FILES} ${QUERY_MANAGER_EMBEDDED_SOURCE_FILES} )

find_package(yaml-cpp CONFIG REQUIRED)
find_package(argparse CONFIG REQUIRED)
find_package(replxx CONFIG REQUIRED)

target_include_directories(nes-nebuli-lib
        PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        PRIVATE $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/private>)

target_link_libraries(nes-nebuli-lib
        PUBLIC nes-logical-operators nes-sql-parser
        PRIVATE yaml-cpp::yaml-cpp
)

target_compile_definitions(nes-nebuli-embedded-lib PUBLIC
        EMBED_ENGINE="1"
)

target_include_directories(nes-nebuli-embedded-lib
        PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        PRIVATE $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/private>)

target_link_libraries(nes-nebuli-embedded-lib
        PUBLIC nes-logical-operators nes-single-node-worker-lib nes-sql-parser
        PRIVATE nes-sql-parser yaml-cpp::yaml-cpp
)

add_executable(nes-nebuli src/NebuLIStarter.cpp src/Repl.cpp)
# Nebuli loads a Query using `dlopen` this requires the executable to be dynamic
set_property(TARGET nes-nebuli PROPERTY ENABLE_EXPORTS ON)
target_link_libraries(nes-nebuli PUBLIC nes-nebuli-lib argparse::argparse yaml-cpp::yaml-cpp replxx::replxx)

target_include_directories(nes-nebuli
        PUBLIC $<INSTALL_INTERFACE:/include/nebulastream>
)
target_include_directories(nes-nebuli
        PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        PRIVATE $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/private>)

add_executable(nes-nebuli-embedded src/NebuLIStarter.cpp src/Repl.cpp)
# Nebuli loads a Query using `dlopen` this requires the executable to be dynamic
set_property(TARGET nes-nebuli-embedded PROPERTY ENABLE_EXPORTS ON)
target_link_libraries(nes-nebuli-embedded PUBLIC nes-nebuli-embedded-lib nes-single-node-worker-lib argparse::argparse yaml-cpp::yaml-cpp replxx::replxx)

target_include_directories(nes-nebuli-embedded
        PUBLIC $<INSTALL_INTERFACE:/include/nebulastream>
)
target_include_directories(nes-nebuli-embedded
        PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        PRIVATE $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/private>)

function(add_nes_test_nebuli)
    add_nes_test(${ARGN})
    set(TARGET_NAME ${ARGV0})
    target_link_libraries(${TARGET_NAME} nes-nebuli-lib)
    target_include_directories(${TARGET_NAME}
            PRIVATE $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../private>)
endfunction()

add_test(
        NAME "repl-test"
#        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/test
        COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/test/test_repl.sh ${CMAKE_CURRENT_SOURCE_DIR}/test/input/test_large.sql ${CMAKE_CURRENT_BINARY_DIR}/nes-nebuli-embedded ${CMAKE_CURRENT_BINARY_DIR}/../nes-systests/testdata ${CMAKE_CURRENT_BINARY_DIR}/../nes-systests/working-dir ${CMAKE_CURRENT_SOURCE_DIR}/test/output/expected_output_large.json ${CMAKE_CURRENT_BINARY_DIR}/test
)

add_test(
        NAME "repl-test-fast"
        #        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/test
        COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/test/test_repl.sh ${CMAKE_CURRENT_SOURCE_DIR}/test/input/test_fast.sql ${CMAKE_CURRENT_BINARY_DIR}/nes-nebuli-embedded ${CMAKE_CURRENT_BINARY_DIR}/../nes-systests/testdata ${CMAKE_CURRENT_BINARY_DIR}/../nes-systests/working-dir ${CMAKE_CURRENT_SOURCE_DIR}/test/output/expected_output_fast.json ${CMAKE_CURRENT_BINARY_DIR}/test
)

