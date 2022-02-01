
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
get_source(nes_client nes_client_SOURCE_FILES)

message(STATUS "Client Source Files: ${nes_client_SOURCE_FILES}")

# Add Library
add_library(nes_client SHARED ${nes_client_SOURCE_FILES})
target_include_directories(nes_client PUBLIC "include")
target_link_libraries(nes_client PRIVATE nes)

if (NES_ENABLES_TESTS)
    # Add tests with command
    add_subdirectory(tests)
    message(STATUS "Tests are enabled")
else ()
    message(STATUS "Tests are disabled")
endif ()

#Add the nes client and headers to cpack
install(TARGETS nes_client
        COMPONENT nesLibrary
        DESTINATION lib)

install(DIRECTORY "include"
        COMPONENT nesHeaders
        DESTINATION "/nebulastream/nes-client")