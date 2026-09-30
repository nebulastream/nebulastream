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

add_library(nes-async
        src/HandoffChannel.cpp
        src/AsyncRecordLayout.cpp
        src/AsyncExecutorRegistry.cpp
        src/DelayExecutor.cpp
        src/AsyncWiring.cpp)
target_include_directories(nes-async PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
find_package(reflectcpp REQUIRED)
target_link_libraries(nes-async PUBLIC nes-memory nes-data-types nes-schema nes-nautilus PRIVATE nes-common reflectcpp::reflectcpp)

# Operators register their executor implementation with add_registry_entry(AsyncExecutor <Name>).
create_runtime_registry(AsyncExecutor nes-async
        ENTRY_TEMPLATE "makeAsyncExecutor<\${PLUGIN_NAME}Executor>()"
        HEADER_TEMPLATE "Async/\${PLUGIN_NAME}Executor.hpp")

add_registry_entry(AsyncExecutor Delay)

# The generated glue translation units of a registry include the plugin header by basename and are
# compiled into that registry's glue library, which does not inherit the plugin's link
# dependencies. Our sink and source headers pull in nes-async's public headers, so the glue library
# needs them on its include path.
#
# This directory is configured before nes-sinks and nes-sources, so their glue libraries do not
# exist yet. The call is therefore deferred to the end of configuration, the same way
# add_registry_entry defers entries for registries it has not seen yet.
function(nes_async_add_glue_include_dir registry include_dir)
    get_property(glue_lib GLOBAL PROPERTY "${registry}_RUNTIME_REGISTRY_GLUE_LIB")
    if (NOT glue_lib)
        message(FATAL_ERROR "nes-async: registry '${registry}' has no glue library; cannot expose its include directory.")
    endif ()
    target_include_directories(${glue_lib} PRIVATE ${include_dir})
endfunction()

add_subdirectory(sink)
add_subdirectory(source)

add_tests_if_enabled(tests)
