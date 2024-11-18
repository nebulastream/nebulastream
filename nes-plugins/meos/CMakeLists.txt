# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

message(STATUS "Enable meos Plugin")

# Detect the platform
if (APPLE)
    message(STATUS "Building on macOS")

    # Set the include and library directories for Homebrew (macOS)
    set(meos_INCLUDE_DIR "/opt/homebrew/include")
    set(meos_PLUGIN_INCLUDE_DIR "/Users/marianaduarte/Desktop/nebulastream/nes-plugins/meos/include")  # Include Meos.hpp directory
    set(POSTGRESQL_LIB_DIR "/opt/homebrew/lib/postgresql@14")
    set(POSTGRESQL_INCLUDE_DIR "/opt/homebrew/include/postgresql@14")

    include_directories(SYSTEM ${meos_INCLUDE_DIR} ${meos_PLUGIN_INCLUDE_DIR} ${POSTGRESQL_INCLUDE_DIR})
    link_directories(${meos_LIBRARY_DIR} ${POSTGRESQL_LIB_DIR})

else()
    message(STATUS "Building on Linux")

    # Default behavior for Linux
    find_package(meos REQUIRED)
    include_directories(SYSTEM ${meos_INCLUDE_DIR})
endif()

# Manually specify the source files for nes-meos
set(NES_COMPILER_SOURCE_FILES
    ${CMAKE_CURRENT_SOURCE_DIR}/src/Execution/Operators/Meos/Meos.cpp  # Add necessary files here
)

add_library(nes-meos SHARED ${NES_COMPILER_SOURCE_FILES})

# Link libraries
if (APPLE)
    target_link_libraries(nes-meos nes-common nes-runtime pq nes-data-types nes-compiler nes-expressions nes-catalogs nes-operators nes-configurations nes-execution nes-coordinator nes-client nes-nautilus /opt/homebrew/lib/libmeos.dylib pq)
else()
    target_link_libraries(nes-meos nes-common nes-runtime pq nes-data-types nes-compiler nes-expressions nes-catalogs nes-operators nes-configurations nes-execution nes-coordinator nes-client nes-nautilus meos pq)
endif()

link_directories(${meos_INCLUDE_DIR})

 # Only needed if building connection with postgres
 #include_directories(/usr/include/postgresql)
 #link_directories(/usr/lib/x86_64-linux-gnu/libpq.so)

 add_dependencies(build_all_plugins nes-meos)

 target_include_directories(nes-meos PUBLIC
         $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
         $<INSTALL_INTERFACE:include/nebulastream/>)

 set_target_properties(nes-meos PROPERTIES
         LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/nes-plugins/)


 if (NES_ENABLES_TESTS)
     add_compile_definitions(TEST_DATA_DIRECTORY="${CMAKE_CURRENT_SOURCE_DIR}/tests/testData/")
     # Add tests with command
     add_subdirectory(tests)
     message(STATUS "meosL Tests are enabled")
 else ()
     message(STATUS "meosL Tests are disabled")
 endif ()


 #Add the nes lib and headers to cpack
 install(TARGETS nes-meos
         EXPORT NebulaStreamTargets
         LIBRARY
         DESTINATION ${CMAKE_INSTALL_BINDIR}/nes-plugins
         COMPONENT nesLibrary)

 # Include meos library in the deb package and install it in the lib folder
 install(IMPORTED_RUNTIME_ARTIFACTS nes-meos DESTINATION ${CMAKE_INSTALL_LIBDIR})

 message(STATUS "Added nes-meos") 