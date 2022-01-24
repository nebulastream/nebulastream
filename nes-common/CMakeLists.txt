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

get_source(nes_common SOURCE_FILES_LOCAL)
set(nes_common_SOURCE_FILES ${SOURCE_FILES_LOCAL})
get_header_nes_client(nes_client_HEADER_FILES)

message(STATUS "Common Headers: ${nes_common_HEADER_FILES}")
message(STATUS "Common Source: ${nes_common_SOURCE_FILES}")

# Add Library
add_library(nes_common SHARED ${nes_common_HEADER_FILES} ${nes_common_SOURCE_FILES})
target_include_directories(nes_common PUBLIC "include")

if (NES_ENABLES_TESTS)
    # Add tests with command
#    add_subdirectory(tests)
    message(STATUS "Tests are enabled")
else ()
    message(STATUS "Tests are disabled")
endif ()

#Add the nes client and headers to cpack
install(TARGETS nes_common
        COMPONENT nesLibrary
        DESTINATION lib)

install(DIRECTORY "include"
        COMPONENT nesHeaders
        DESTINATION "/nebulastream")