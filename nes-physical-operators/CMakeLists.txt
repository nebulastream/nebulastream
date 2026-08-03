# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# include registrar generation functions
include(${PROJECT_SOURCE_DIR}/cmake/RuntimeRegistrationUtil.cmake)


# Declare the library up front so create_runtime_registry can attach the glue sub-libraries;
# sources are added below once src/ has registered them.
add_library(nes-physical-operators STATIC)
set_target_properties(nes-physical-operators PROPERTIES LINKER_LANGUAGE CXX)

# Runtime registries: entries are static create<Key> / create members on the function classes.
create_runtime_registry(PhysicalFunction nes-physical-operators
        ENTRY_TEMPLATE "&\${PLUGIN_NAME}PhysicalFunction::create\${KEY}"
        HEADER_TEMPLATE "\${PLUGIN_NAME}PhysicalFunction.hpp")

create_runtime_registry(AggregationPhysicalFunction nes-physical-operators
        ENTRY_TEMPLATE "&\${PLUGIN_NAME}AggregationPhysicalFunction::create"
        HEADER_TEMPLATE "\${PLUGIN_NAME}AggregationPhysicalFunction.hpp")

add_subdirectory(src)
get_source(nes-physical-operators NES_PHYSICAL_OPERATORS_SOURCE_FILES)
target_sources(nes-physical-operators PRIVATE ${NES_PHYSICAL_OPERATORS_SOURCE_FILES})
find_package(OpenSSL REQUIRED)
target_link_libraries(nes-physical-operators PUBLIC nes-sources nes-sinks nes-nautilus nes-logical-operators nes-input-formatter-provider PRIVATE nes-input-formatters nes-inference-runtime OpenSSL::Crypto)
target_include_directories(nes-physical-operators PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include/nebulastream/>)


add_tests_if_enabled(tests)
