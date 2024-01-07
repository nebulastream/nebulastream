
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_library(nes-execution-test-util SHARED
        "src/AbstractCompilationBackendTest.cpp"
        "src/MockedPipelineExecutionContext.cpp"
        "src/CollectOperator.cpp"
        "src/UtilityFunctions.cpp")

message(STATUS "Test CMAKE_CURRENT_SOURCE_DIR Directory: ${CMAKE_CURRENT_SOURCE_DIR}")
target_include_directories(nes-execution-test-util PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/include")
target_link_libraries(nes-execution-test-util PUBLIC nes-execution nes-common nes-data-types nes-operators nes-grpc nes-runtime nes-test-util nes-configurations nes-nautilus)
# target_link_libraries(nes-execution-test-util PUBLIC nes-execution nes-common nes-data-types nes-operators nes-grpc nes-execution nes-test-util nes-configurations)
target_compile_definitions(nes-execution-test-util INTERFACE JAVA_UDF_TEST_DATA="${CMAKE_CURRENT_SOURCE_DIR}/../testData/JavaUDFTestData")