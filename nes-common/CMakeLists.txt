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

get_source(nes-common NES_COMMON_SOURCE_FILES)

# Add Library
add_library(nes-common SHARED ${NES_COMMON_SOURCE_FILES})
target_link_libraries(nes-common PUBLIC ${LIBRARIES})
target_include_directories(nes-common PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include/nebulastream/>)

set_property(TARGET nes-common PROPERTY INTERFACE_LINK_LIBRARIES
        $<BUILD_INTERFACE:${LIBRARIES}>
        $<INSTALL_INTERFACE:>)


register_public_header_dir(nes-common ${CMAKE_CURRENT_SOURCE_DIR}/include/Util/ Util)
register_public_header_dir(nes-common ${CMAKE_CURRENT_SOURCE_DIR}/include/Exceptions/ Exceptions)

if (NES_ENABLES_TESTS)
    # Add tests with command
    add_subdirectory(tests)
    message(STATUS "Tests are enabled")
else ()
    message(STATUS "Tests are disabled")
endif ()

#Add the nes common and headers to cpack
install(TARGETS nes-common
        EXPORT NebulaStreamTargets
        COMPONENT nesLibrary
        DESTINATION lib)