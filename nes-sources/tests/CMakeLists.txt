# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_library(nes-sources-test-utils Util/TestSource.cpp)
target_include_directories(nes-sources-test-utils PUBLIC ../private Util)
target_link_libraries(nes-sources-test-utils PUBLIC nes-sources nes-input-formatters)

function(add_nes_source_test)
    add_nes_test(${ARGN})
    set(TARGET_NAME ${ARGV0})
    target_link_libraries(${TARGET_NAME} nes-sources-test-utils)
endfunction()

add_nes_unit_test(shared-runtime-values-test SharedRuntimeValuesTest.cpp)
target_link_libraries(shared-runtime-values-test nes-sources reflectcpp::reflectcpp)
target_include_directories(shared-runtime-values-test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/Util)
add_nes_unit_test(async-call-graph-test AsyncCallGraphTest.cpp)
target_link_libraries(async-call-graph-test nes-sources reflectcpp::reflectcpp)
add_nes_unit_test(async-handoff-layout-test AsyncHandoffLayoutTest.cpp)
target_link_libraries(async-handoff-layout-test nes-sources)
target_include_directories(async-handoff-layout-test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/Util)
target_include_directories(async-call-graph-test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/Util)
add_nes_unit_test(async-operator-registry-test AsyncOperatorRegistryTest.cpp)
target_link_libraries(async-operator-registry-test nes-sources reflectcpp::reflectcpp)
target_include_directories(async-operator-registry-test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/Util)

add_nes_source_test(source-thread-test SourceThreadTest.cpp)
add_nes_source_test(source-catalog-test SourceCatalogTest.cpp)
