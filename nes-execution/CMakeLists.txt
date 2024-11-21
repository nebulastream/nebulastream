# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# include(../cmake/LLVMIRUtil.cmake)
include_directories(tests/include)

# Set Sources and Headers
add_subdirectory(src)

get_source(nes-execution NES_EXECUTION_SOURCE_FILES)
# Detect the platform
if (APPLE)
    message(STATUS "Building on MEOS in nes-execution")
    # Set the include and library directories for Homebrew (macOS)
    set(MEOS_INCLUDE_DIR "/opt/homebrew/include" CACHE PATH "Path to MEOS include directory")
    set(MEOS_PLUGIN_INCLUDE_DIR "/Users/marianaduarte/Desktop/nebulastream/nes-plugins/meos/include" CACHE PATH "Path to MEOS plugin include directory")
    set(POSTGRESQL_LIB_DIR "/opt/homebrew/lib/postgresql@14" CACHE PATH "Path to PostgreSQL library directory")
    set(POSTGRESQL_INCLUDE_DIR "/opt/homebrew/include/postgresql@14" CACHE PATH "Path to PostgreSQL include directory")
    include_directories(SYSTEM ${MEOS_INCLUDE_DIR} ${MEOS_PLUGIN_INCLUDE_DIR} ${POSTGRESQL_INCLUDE_DIR})
    link_directories(${MEOS_LIBRARY_DIR} ${POSTGRESQL_LIB_DIR})

else()
    message(STATUS "Building on Linux")

    # Default behavior for Linux
    find_package(meos REQUIRED)
    include_directories(SYSTEM ${MEOS_INCLUDE_DIR})
endif()

# Add Library
add_library(nes-execution ${NES_EXECUTION_SOURCE_FILES})
# link libraries
target_link_libraries(nes-execution PUBLIC nes-nautilus /opt/homebrew/lib/libmeos.dylib)
target_include_directories(nes-execution PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include/nebulastream/>)

if (NES_ENABLE_EXPERIMENTAL_EXECUTION_JNI)
    target_link_libraries(nes-runtime PUBLIC ${JNI_LIBRARIES})
    # Define map java udf utils class file
    SET(NES_JAVA_UTILS_SRC_PATH "src/Execution/Operators/Relational/JavaUDF")
    if (NES_JAVA_UDF_UTILS_PATH)
        # Use explicit path to Java helpers when building Debian package for the nes-executable-image Docker image.
        SET(JAVA_UDF_UTILS_JAR "${CMAKE_INSTALL_PREFIX}/${NES_JAVA_UDF_UTILS_PATH}/JavaUtils.jar")
        target_compile_definitions(nes-runtime PUBLIC JAVA_UDF_UTILS_JAR="${JAVA_UDF_UTILS_JAR}")
        message(STATUS "Setting JAR with Java helpers to: ${JAVA_UDF_UTILS_JAR}")
        # Install Java Utils JAR
        INSTALL(FILES "${CMAKE_CURRENT_BINARY_DIR}/${NES_JAVA_UTILS_SRC_PATH}/JavaUtils.jar" DESTINATION ${NES_JAVA_UDF_UTILS_PATH})
    else ()
        # Use Java helpers from test directory when working in CLion.
        target_compile_definitions(nes-runtime PUBLIC JAVA_UDF_UTILS_JAR="${CMAKE_CURRENT_BINARY_DIR}/${NES_JAVA_UTILS_SRC_PATH}/JavaUtils.jar")
    endif()
endif()

if (NES_ENABLE_PRECOMPILED_HEADERS)
    target_precompile_headers(nes-execution REUSE_FROM nes-common)
    # We need to compile with -fPIC to include with nes-common compiled headers as it uses PIC
    target_compile_options(nes-execution PUBLIC "-fPIC")
endif ()

if (NES_ENABLES_TESTS)
    # Add tests with command
    add_subdirectory(tests)
    message(STATUS "Tests are enabled")
else ()
    message(STATUS "Tests are disabled")
endif ()

message(STATUS "Execution benchmarks status: ${NES_BUILD_BENCHMARKS}")
if (NES_BUILD_BENCHMARKS)
    # Add tests with command
    add_subdirectory(benchmark)
    message(STATUS "Execution benchmarks are enabled")
endif ()

#Add the nes common and headers to cpack
install(TARGETS nes-execution
        EXPORT NebulaStreamTargets
        COMPONENT nesLibrary
        DESTINATION lib)