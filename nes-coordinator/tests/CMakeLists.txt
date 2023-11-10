# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Google Testing Framework ----------------------------------------------------
include(ExternalProject)
# Copy Test Data --------------------------------------------------------------
file(COPY test_data DESTINATION ${CMAKE_CURRENT_BINARY_DIR})
add_compile_definitions(TEST_DATA_DIRECTORY="${CMAKE_CURRENT_BINARY_DIR}/test_data/")
message(STATUS "Test Data Directory: ${TEST_DATA_DIRECTORY}/test_data")

#-------------------------------##############----------------------------------
#------------------------------- # Nes Utils for Test Suite --------------------------------

include_directories(include)
add_library(nes-core-test-util SHARED "src/Util/DummySink.cpp"
        "src/Util/TestHarness/TestHarness.cpp"
        "src/Util/TestHarness/TestHarnessWorkerConfiguration.cpp"
        "src/Util/TestUtils.cpp"
        "src/Util/TestPhaseProvider.cpp"
        "src/Util/NonRunnableDataSource.cpp"
        "src/Util/TestExecutionEngine.cpp"
        "src/Util/TestSourceProvider.cpp"
        "src/Util/TestSourceDescriptor.cpp"
        "src/Util/TestSinkProvider.cpp"
        "src/Util/TestSinkDescriptor.cpp"
        "src/Util/MetricValidator.cpp"
        "src/Util/TestSink.cpp"
        "src/Util/PythonUDFDescriptorBuilder.cpp")
target_include_directories(nes-core-test-util PUBLIC "./include")

message(STATUS "Test CMAKE_CURRENT_SOURCE_DIR Directory: ${CMAKE_CURRENT_SOURCE_DIR}")
target_link_libraries(nes-core-test-util PUBLIC nes-common nes-data-types nes-operators nes-grpc nes-catalogs nes-optimizer nes-compiler nes-runtime nes-test-util nes-client nes-worker nes-coordinator nes-configurations)
target_link_libraries(nes-core-test-util PUBLIC nes-operators-test-util)
target_link_libraries(nes-core-test-util PUBLIC cpr::cpr )
find_package(oatpp REQUIRED)
target_link_libraries(nes-core-test-util PUBLIC oatpp::oatpp oatpp::oatpp-test)

if (CODE_COVERAGE)
    target_code_coverage(nes-coordinator  AUTO)
endif ()


add_subdirectory(UnitTests/QueryExecution)
add_subdirectory(E2e)
add_subdirectory(Integration)
add_subdirectory(UnitTests)

set_tests_properties(${Tests} PROPERTIES TIMEOUT 35)