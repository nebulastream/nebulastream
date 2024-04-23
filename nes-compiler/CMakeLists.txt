# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Set Sources and Headers
add_subdirectory(src)

get_source(nes-compiler NES_COMPILER_SOURCE_FILES)

# Add Library
add_library(nes-compiler ${NES_COMPILER_SOURCE_FILES})
target_link_libraries(nes-compiler PRIVATE nes-common)

target_include_directories(nes-compiler PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include/nebulastream/>)

if (NES_ENABLE_PRECOMPILED_HEADERS)
    target_precompile_headers(nes-compiler REUSE_FROM nes-common)
    # We need to compile with -fPIC to include with nes-common compiled headers as it uses PIC
    target_compile_options(nes-compiler PUBLIC "-fPIC")
endif ()
if (NES_ENABLES_TESTS)
    # Add tests with command
    add_subdirectory(tests)
    message(STATUS "Tests are enabled")
else ()
    message(STATUS "Tests are disabled")
endif ()

#Add the nes common and headers to cpack
install(TARGETS nes-compiler
        EXPORT NebulaStreamTargets
        COMPONENT nesLibrary
        DESTINATION lib)