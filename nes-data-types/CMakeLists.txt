# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

include(${PROJECT_SOURCE_DIR}/cmake/RuntimeRegistrationUtil.cmake)

find_package(reflectcpp REQUIRED)

# Declare the library up front so create_runtime_registry can attach the glue sub-library;
# sources are added below once src/ has registered them.
add_library(nes-data-types STATIC)
set_target_properties(nes-data-types PROPERTIES LINKER_LANGUAGE CXX)

# Runtime registry: entries are keyed by the DataType::Type enumerator and construct the type
# directly — no per-plugin classes exist.
create_runtime_registry(DataType nes-data-types
        ENTRY_TEMPLATE "[](DataTypeRegistryArguments arguments) { return DataType{DataType::Type::\${KEY}, arguments.nullable}; }")

add_subdirectory(src)
get_source(nes-data-types NES_COMPILER_SOURCE_FILES)
target_sources(nes-data-types PRIVATE ${NES_COMPILER_SOURCE_FILES})
target_link_libraries(nes-data-types PUBLIC nes-configurations PRIVATE reflectcpp::reflectcpp)
target_include_directories(nes-data-types PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include/nebulastream/>)
if (NES_ENABLE_PRECOMPILED_HEADERS)
    target_precompile_headers(nes-data-types REUSE_FROM nes-common)
endif ()

add_tests_if_enabled(tests)

