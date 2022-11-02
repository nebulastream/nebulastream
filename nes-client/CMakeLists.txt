
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
get_source(nes-client NES_CLIENT_SOURCE_FILES)

# CPR
find_package(cpr REQUIRED)
if (cpr_FOUND)
    find_package(CURL REQUIRED)
    message(STATUS "Found CURL : ${CURL_LIBRARIES}")
    message(STATUS "Found cpr: ${cpr_CONFIG} (found version ${cpr_VERSION})")
endif ()

# Add Library
add_library(nes-client SHARED ${NES_CLIENT_SOURCE_FILES})
target_link_libraries(nes-client PRIVATE ${CURL_LIBRARIES} cpr::cpr nes)
target_include_directories(nes-client PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include/nebulastream/>)

if (NES_ENABLES_TESTS)
    # Add tests with command
    add_subdirectory(tests)
    message(STATUS "Tests are enabled")
else ()
    message(STATUS "Tests are disabled")
endif ()

#Add the nes client and headers to cpack
install(TARGETS nes-client
        EXPORT NebulaStreamTargets
        LIBRARY
        COMPONENT nesLibrary)

include(GNUInstallDirs)
install(DIRECTORY "include/"
        COMPONENT nesHeaders
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/nebulastream/)