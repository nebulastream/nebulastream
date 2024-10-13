# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# 1) Print the status
message(STATUS "Enable cuda Plugin")

# 2) Include necessary libraries
if (CMAKE_SYSTEM_NAME STREQUAL "Linux" AND NOT (CMAKE_SYSTEM_PROCESSOR MATCHES "arm" OR CMAKE_SYSTEM_PROCESSOR MATCHES "aarch"))
    ### find CUDA package
    find_package(CUDAToolkit REQUIRED)
    enable_language(CUDA)
    if (NOT ${CUDAToolkit_FOUND})
        message(FATAL_ERROR "Unable to find CUDAToolkit_FOUND")
    endif ()
    set(CMAKE_CUDA_STANDARD 17)
    set(LIBRARIES_CUDA CUDA::cudart CUDA::nvrtc)
    set(LIBRARIES ${LIBRARIES} ${LIBRARIES_CUDA})
    set(CMAKE_CUDA_FLAGS "${CMAKE_CUDA_FLAGS} -allow-unsupported-compiler")
    message("CUDA dependencies OK")
else ()
    message(FATAL_ERROR "CUDA plugin is not supported on this platform")
endif ()

# 3) Add source files
add_subdirectory(src)
get_source(nes-cuda NES_CUDA_SOURCE_FILES)
add_library(nes-cuda SHARED
        ${NES_CUDA_SOURCE_FILES}
        )

# 4) Register this CUDA plugin to build_all_plugin target
add_dependencies(build_all_plugins nes-cuda)

# 5) Add the include directory
target_include_directories(nes-cuda PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include/nebulastream/>)

set_target_properties(nes-cuda PROPERTIES
        LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/nes-plugins/)

target_link_libraries(nes-cuda PUBLIC nes-common nes-runtime nes-data-types nes-operators nes-execution nes-nautilus ${LIBRARIES_CUDA})

# 6) Enable and add tests
if (NES_ENABLES_TESTS)
    # Add tests with command
    add_subdirectory(tests)
    message(STATUS "Tests are enabled")
else ()
    message(STATUS "Tests are disabled")
endif ()

# 7) Add the nes lib and headers to cpack
install(TARGETS nes-cuda
        EXPORT NebulaStreamTargets
        LIBRARY
        DESTINATION ${CMAKE_INSTALL_BINDIR}/nes-plugins
        COMPONENT nesLibrary)
