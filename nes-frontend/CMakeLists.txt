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

add_library(nes-frontend-lib STATIC ${QUERY_MANAGER_GRPC_SOURCE_FILES} ${CATALOG_SOURCE_FILES} ${LEGACYOPTIMIZER_SOURCE_FILES} ${OPERATORS_SOURCE_FILES} ${COORDINATOR_SOURCE_FILES} ${CLIENT_SOURCE_FILES} ${YAML_BINDER_SOURCE_FILES})
add_library(nes-frontend-embedded-lib STATIC ${QUERY_MANAGER_GRPC_SOURCE_FILES} ${CATALOG_SOURCE_FILES} ${LEGACYOPTIMIZER_SOURCE_FILES} ${OPERATORS_SOURCE_FILES} ${COORDINATOR_SOURCE_FILES} ${CLIENT_SOURCE_FILES} ${YAML_BINDER_SOURCE_FILES} ${QUERY_MANAGER_EMBEDDED_SOURCE_FILES} )

find_package(yaml-cpp CONFIG REQUIRED)
find_package(argparse CONFIG REQUIRED)
find_package(replxx CONFIG REQUIRED)

target_include_directories(nes-frontend-lib
        PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        PRIVATE $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/private>)

target_link_libraries(nes-frontend-lib
        PUBLIC nes-logical-operators nes-sql-parser nes-single-node-worker-interface
        PRIVATE yaml-cpp::yaml-cpp
)

target_compile_definitions(nes-frontend-embedded-lib PUBLIC
        EMBED_ENGINE="1"
)

target_include_directories(nes-frontend-embedded-lib
        PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        PRIVATE $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/private>)

target_link_libraries(nes-frontend-embedded-lib
        PUBLIC nes-logical-operators nes-single-node-worker-lib nes-sql-parser nes-single-node-worker-interface
        PRIVATE nes-sql-parser yaml-cpp::yaml-cpp
)

add_subdirectory(apps)
