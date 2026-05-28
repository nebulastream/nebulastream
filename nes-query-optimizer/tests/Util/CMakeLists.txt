# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_library(nes-query-optimizer-test-utils-lib OptimizerTestUtils.cpp)
target_include_directories(nes-query-optimizer-test-utils-lib PUBLIC . ${CMAKE_SOURCE_DIR}/nes-query-optimizer/tests)
target_link_libraries(nes-query-optimizer-test-utils-lib PUBLIC nes-common PRIVATE nes-query-optimizer nes-logical-operators nes-test-util)
