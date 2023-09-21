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
get_source(nes-core NES_CORE_SOURCE_FILES)
add_library(nes SHARED ${NES_CORE_SOURCE_FILES})

target_include_directories(nes PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include/nebulastream/>)
target_link_libraries(nes PUBLIC nes-compiler nes-runtime nes-optimizer nes-client)
if (NES_ENABLE_PRECOMPILED_HEADERS)
    target_precompile_headers(nes REUSE_FROM nes-common)
endif ()
if (NES_USE_TF)
    message(STATUS "Building with tensorflowlite")
    find_package(tensorflow-lite-c REQUIRED)
    target_link_libraries(nes PUBLIC tensorflowlite_c::tensorflowlite_c)
endif ()

get_target_property(LIBA_INCLUDES nes INCLUDE_DIRECTORIES)
foreach (dir ${LIBA_INCLUDES})
    string(APPEND INCLUDE_COMPILER_STRING "/I${dir} ")
    message(STATUS "LIBNES includes ${dir}")
endforeach ()

message(STATUS "CMAKE_CURRENT_SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}")

# Select fine-grained headers
register_public_header_dir(nes ${CMAKE_CURRENT_SOURCE_DIR}/include/Windowing Windowing)
register_public_header_dir(nes ${CMAKE_CURRENT_SOURCE_DIR}/include/Runtime/Execution/ Runtime/Execution/)
register_public_header_file(nes ${CMAKE_CURRENT_SOURCE_DIR}/include/QueryCompiler/QueryCompilerForwardDeclaration.hpp QueryCompiler/QueryCompilerForwardDeclaration.hpp)
register_public_header_file(nes ${CMAKE_CURRENT_SOURCE_DIR}/include/Runtime/RuntimeForwardRefs.hpp Runtime/RuntimeForwardRefs.hpp)
register_public_header_file(nes ${CMAKE_CURRENT_SOURCE_DIR}/include/Util/Experimental/HashMap.hpp Util/Experimental/HashMap.hpp)
register_public_header_file(nes ${CMAKE_CURRENT_SOURCE_DIR}/include/Util/Core.hpp Util/Core.hpp)
register_public_header_file(nes ${CMAKE_CURRENT_SOURCE_DIR}/include/Util/Experimental/Hash.hpp Util/Experimental/Hash.hpp)
register_public_header_file(nes ${CMAKE_CURRENT_SOURCE_DIR}/include/Util/Experimental/CRC32Hash.hpp Util/Experimental/CRC32Hash.hpp)
register_public_header_file(nes ${CMAKE_CURRENT_SOURCE_DIR}/include/Util/Experimental/MurMurHash3.hpp Util/Experimental/MurMurHash3.hpp)
register_public_header_file(nes ${CMAKE_CURRENT_SOURCE_DIR}/include/Exceptions/WindowProcessingException.hpp Exceptions/WindowProcessingException.hpp)
register_public_header_dir(nes ${NES_DEPENDENCIES_BINARY_ROOT}/include/folly/ folly)
register_public_header_dir(nes ${NES_DEPENDENCIES_BINARY_ROOT}/include/spdlog/ spdlog)
register_public_header_dir(nes ${NES_DEPENDENCIES_BINARY_ROOT}/include/fmt/ fmt)

add_executable(nesCoordinator src/Executables/nesCoordinatorStarter.cpp)
target_link_libraries(nesCoordinator PUBLIC nes)

add_executable(nesWorker src/Executables/nesWorkerStarter.cpp)
target_link_libraries(nesWorker PUBLIC nes)

if (NES_ENABLES_TESTS)
    # Add tests with command
    add_subdirectory(tests)
    message(STATUS "Tests are enabled")
else ()
    message(STATUS "Tests are disabled")
endif ()

#Add the nes lib and headers to cpack
install(TARGETS nes
        EXPORT NebulaStreamTargets
        LIBRARY
        COMPONENT nesLibrary)

install(TARGETS nesCoordinator nesWorker
        RUNTIME
        COMPONENT nesExecutables)

install(DIRECTORY ${CMAKE_BINARY_DIR}/include/nebulastream
        COMPONENT nesHeaders
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

install(DIRECTORY ${CLANG_INCLUDE_DIR}
        COMPONENT nesHeaders
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/clang/${LLVM_VERSION_MAJOR}/include/)
