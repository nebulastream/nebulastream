# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

include(../cmake/LLVMIRUtil.cmake)
include_directories(tests/include)

# Set Sources and Headers
add_subdirectory(src)
get_source(nes-nautilus NES_NAUTILUS_SOURCE_FILES)

# Add Library
add_library(nes-nautilus ${NES_NAUTILUS_SOURCE_FILES})
target_link_libraries(nes-nautilus PUBLIC nes-compiler nes-runtime)


# add the nautilus source dir as a subdirectory to the main cmake project.
# ${CMAKE_CURRENT_SOURCE_DIR}/.. should be the path to the nautilus source dir
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/nautilus ${CMAKE_CURRENT_BINARY_DIR}/nautilus)
# add the nautilus target to your project
target_link_libraries(nes-nautilus PRIVATE nautilus)
target_include_directories(nes-nautilus PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/nautilus/nautilus/include>
        $<INSTALL_INTERFACE:include/nebulastream/>
)
install(TARGETS nautilus
        EXPORT NebulaStreamTargets
        COMPONENT nesLibrary
        DESTINATION lib
)


if (NES_ENABLE_PRECOMPILED_HEADERS)
    target_precompile_headers(nes-nautilus REUSE_FROM nes-common)
    # We need to compile with -fPIC to include with nes-common compiled headers as it uses PIC
    target_compile_options(nes-nautilus PUBLIC "-fPIC")
endif ()

if (NES_ENABLES_TESTS)
    # Add tests with command
    add_subdirectory(tests)
    message(STATUS "Tests are enabled")
else ()
    message(STATUS "Tests are disabled")
endif ()


#Add the nes nautilus and headers to cpack
install(TARGETS nes-nautilus
        EXPORT NebulaStreamTargets
        COMPONENT nesLibrary
        DESTINATION lib)