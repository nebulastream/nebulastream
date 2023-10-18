# Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

get_source(nes-data-types NES_COMPILER_SOURCE_FILES)

# Add Library
add_library(nes-data-types SHARED ${NES_COMPILER_SOURCE_FILES})
target_link_libraries(nes-data-types PUBLIC nes-configurations)
target_include_directories(nes-data-types PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include/nebulastream/>)
if (NES_ENABLE_PRECOMPILED_HEADERS)
    target_precompile_headers(nes-data-types REUSE_FROM nes-common)
endif ()

# Add Library
add_library(nes-unikernel-data-types STATIC ${NES_COMPILER_SOURCE_FILES})
target_link_libraries(nes-unikernel-data-types PUBLIC nes-unikernel-common)
target_include_directories(nes-unikernel-data-types PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include/nebulastream/>)
if (NES_ENABLE_PRECOMPILED_HEADERS)
    target_precompile_headers(nes-unikernel-data-types REUSE_FROM nes-unikernel-common)
endif ()

if (NES_ENABLES_TESTS)
    # Add tests with command
    add_subdirectory(tests)
    message(STATUS "Tests are enabled")
else ()
    message(STATUS "Tests are disabled")
endif ()

register_public_header_dir(nes-data-types ${CMAKE_CURRENT_SOURCE_DIR}/include/Common/DataTypes Common/DataTypes)
register_public_header_dir(nes-data-types ${CMAKE_CURRENT_SOURCE_DIR}/include/Common/ExecutableType Common/ExecutableType)
register_public_header_dir(nes-data-types ${CMAKE_CURRENT_SOURCE_DIR}/include/Common/PhysicalTypes Common/PhysicalTypes)
register_public_header_dir(nes-data-types ${CMAKE_CURRENT_SOURCE_DIR}/include/API/ API)

#Add the nes common and headers to cpack
install(TARGETS nes-data-types
        EXPORT NebulaStreamTargets
        COMPONENT nesLibrary
        DESTINATION lib)