# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

message(STATUS "Enable MEOS Plugin")

# Detect the platform
if (APPLE)
    message(STATUS "Building on macOS")
    # Set the include and library directories for Homebrew (macOS)
    set(MEOS_INCLUDE_DIR "/opt/homebrew/include" CACHE PATH "Path to MEOS include directory")
    set(MEOS_PLUGIN_INCLUDE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/include" CACHE PATH "Path to MEOS plugin include directory")
    include_directories(SYSTEM ${MEOS_INCLUDE_DIR} ${MEOS_PLUGIN_INCLUDE_DIR})
    include_directories(SYSTEM ${MEOS_INCLUDE_DIR} ${MEOS_PLUGIN_INCLUDE_DIR})
    link_directories(${MEOS_LIBRARY_DIR} )

else()
    message(STATUS "Building on Linux")

    # Default behavior for Linux
    find_package(meos REQUIRED)
    include_directories(SYSTEM ${MEOS_INCLUDE_DIR})
endif()

# Manually specify the source files for nes-meos
set(NES_COMPILER_SOURCE_FILES
    ${CMAKE_CURRENT_SOURCE_DIR}/src/Execution/Operators/Meos/Meos.cpp  # Add necessary files here
)

add_library(nes-meos SHARED ${NES_COMPILER_SOURCE_FILES})

# Link libraries
if (APPLE)
    target_link_libraries(nes-meos nes-common nes-runtime nes-data-types nes-catalogs nes-operators nes-configurations nes-execution nes-nautilus /opt/homebrew/lib/libmeos.dylib)
else()
    target_link_libraries(nes-meos nes-common nes-runtime nes-data-types nes-catalogs nes-operators nes-configurations nes-execution nes-nautilus meos)
endif()

link_directories(${MEOS_INCLUDE_DIR})


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
     message(STATUS "MEOS Tests are enabled")
 else ()
     message(STATUS "MEOS Tests are disabled")
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