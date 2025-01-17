# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

macro(add_query_engine_test)
    add_nes_test(${ARGN})
    set(TARGET_NAME ${ARGV0})
    target_link_libraries(${TARGET_NAME} nes-sources-test-utils nes-memory-test-utils nes-query-engine nes-query-manager-test-utils-lib)
    target_include_directories(${TARGET_NAME} PRIVATE ..)
endmacro()

add_query_engine_test(query-engine-test QueryEngineTest.cpp)
add_query_engine_test(running-query-plan-test QueryPlanTest.cpp)
add_query_engine_test(query-engine-configuration-test QueryEngineConfigurationTest.cpp)

add_subdirectory(Util)
