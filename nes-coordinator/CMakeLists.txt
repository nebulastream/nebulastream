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

# Add nes-coordinator library
get_source(nes-coordinator NES_COORDINATOR_SOURCE_FILES)
add_library(nes-coordinator SHARED ${NES_COORDINATOR_SOURCE_FILES})

target_include_directories(nes-coordinator PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include/nebulastream/>)
target_link_libraries(nes-coordinator PRIVATE nes-common nes-grpc nes-operators nes-compiler nes-catalogs nes-runtime nes-optimizer nes-worker nes-client nes-data-types nes-configurations nes-execution nes-nautilus)
if (NES_ENABLE_PRECOMPILED_HEADERS)
    target_precompile_headers(nes-coordinator REUSE_FROM nes-common)
endif ()

#Oatpp for the rest server
find_package(oatpp REQUIRED)
target_link_libraries(nes-coordinator PRIVATE oatpp::oatpp oatpp::oatpp-test)
target_link_libraries(nes-coordinator PRIVATE cpr::cpr)

# Antlr
# Currently antlr dose not come with an own cmake file
message(STATUS Antlr ${NES_DEPENDENCIES_BINARY_ROOT})
target_include_directories(nes-coordinator PRIVATE ${NES_DEPENDENCIES_BINARY_ROOT}/include/antlr4-runtime)
if (NOT APPLE)
    target_link_libraries(nes-coordinator PUBLIC ${NES_DEPENDENCIES_BINARY_ROOT}/lib/libuuid.a)
endif ()
target_link_libraries(nes-coordinator PRIVATE ${NES_DEPENDENCIES_BINARY_ROOT}/lib/libantlr4-runtime.a)

get_target_property(LIBA_INCLUDES nes-coordinator INCLUDE_DIRECTORIES)
foreach (dir ${LIBA_INCLUDES})
    string(APPEND INCLUDE_COMPILER_STRING "/I${dir} ")
    message(STATUS "LIBNES includes ${dir}")
endforeach ()

message(STATUS "CMAKE_CURRENT_SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}")

add_executable(nesCoordinator src/Executables/nesCoordinatorStarter.cpp)
target_link_libraries(nesCoordinator PRIVATE  nes-common nes-coordinator nes-catalogs nes-configurations )

if (NES_USE_OPENCL)
    find_package(OpenCL REQUIRED)
    if (${OpenCL_FOUND})
        MESSAGE(STATUS "Found OpenCL ${OpenCL_VERSION_STRING}")
        MESSAGE(STATUS "OpenCL libraries: ${OpenCL_LIBRARIES}")
        MESSAGE(STATUS "OpenCL includes: ${OpenCL_INCLUDE_DIRS}")
        target_link_libraries(nes-coordinator PRIVATE ${OpenCL_LIBRARIES})
        target_include_directories(nes-coordinator PRIVATE ${OpenCL_INCLUDE_DIRS})
        target_compile_definitions(nes-coordinator PRIVATE ENABLE_OPENCL)
    else ()
        message(FATAL_ERROR "Could not find OpenCL")
    endif ()
endif (NES_USE_OPENCL)

if (NES_ENABLES_TESTS)
    # Add tests with command
    add_subdirectory(tests)
    message(STATUS "Tests are enabled")
else ()
    message(STATUS "Tests are disabled")
endif ()

#Add the nes lib and headers to cpack
install(TARGETS nes-coordinator
        EXPORT NebulaStreamTargets
        LIBRARY
        COMPONENT nesLibrary)

install(TARGETS nesCoordinator nesWorker
        RUNTIME
        COMPONENT nesExecutables)

install(DIRECTORY ${CMAKE_BINARY_DIR}/include/nebulastream
        COMPONENT nesHeaders
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

message(status "LLVM_VERSION: ${LLVM_VERSION}")
install(DIRECTORY ${CLANG_INCLUDE_DIR}
        COMPONENT nesHeaders
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/clang/${LLVM_VERSION}/include/)
