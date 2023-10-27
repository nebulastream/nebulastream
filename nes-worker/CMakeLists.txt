# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Add Source Code
add_subdirectory(src)
include(GNUInstallDirs)

# Add nes-core library
get_source(worker NES_WORKER_SOURCE_FILES)
add_library(nes-worker SHARED ${NES_WORKER_SOURCE_FILES})

target_include_directories(nes-worker PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include/nebulastream/>)

target_link_libraries(nes-worker PUBLIC nes-runtime nes-grpc)

add_executable(nesWorker src/Executables/nesWorkerStarter.cpp)
target_link_libraries(nesWorker PUBLIC nes-worker)

if (NES_ENABLES_TESTS)
    # Add tests with command
    add_subdirectory(test)
    message(STATUS "Tests are enabled")
else ()
    message(STATUS "Tests are disabled")
endif ()

#Add the nes lib and headers to cpack
install(TARGETS nes-worker
        EXPORT NebulaStreamTargets
        LIBRARY
        COMPONENT nesLibrary)

install(TARGETS nesWorker
        RUNTIME
        COMPONENT nesExecutables)
