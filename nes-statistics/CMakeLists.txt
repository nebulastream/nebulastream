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
get_source(nes-statistics NES_STATISTICS_SOURCE_FILES)

# Add Library
add_library(nes-statistics SHARED ${NES_STATISTICS_SOURCE_FILES})
target_link_libraries(nes-statistics PUBLIC nes-operators)
target_include_directories(nes-statistics PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include/nebulastream/>)

register_public_header_dir(nes-statistics ${CMAKE_CURRENT_SOURCE_DIR}/include/StatisticCollection StatisticCollection)
register_public_header_dir(nes-statistics ${CMAKE_CURRENT_SOURCE_DIR}/include/Statistics Statistics)

# Find folly
find_package(folly CONFIG REQUIRED)
target_link_libraries(nes-statistics PRIVATE folly::folly)



#Add the nes statistics and headers to cpack
install(TARGETS nes-statistics
        EXPORT NebulaStreamTargets
        COMPONENT nesLibrary
        DESTINATION lib)