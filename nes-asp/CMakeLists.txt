# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

message(STATUS "Building approximate stream processing...")


# Include dirs
include_directories("include/")

# Add source code
add_subdirectory(src)

# Get all source files
get_source(nes-asp NES_APPROX_SOURCE_FILES)

# Add nes-benchmark library
add_library(nes-asp SHARED ${NES_APPROX_SOURCE_FILES})
target_link_libraries(nes-asp PUBLIC nes-runtime nes)


# Enabling test, if cmake flag is set
if (NES_ENABLES_TESTS)
    # Add tests with command
    add_subdirectory(tests)
    message(STATUS "Tests for nes-asp are enabled")
else ()
    message(STATUS "Tests for nes-asp are disabled")
endif ()