# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

function(add_benchmark_test TARGET_NAME CONFIG_FILE_NAME)
    add_test(NAME ${TARGET_NAME} COMMAND e2e-benchmark-main --configPath=${PROJECT_SOURCE_DIR}/${CONFIG_FILE_NAME} --logPath=${TARGET_NAME}.log)
    message(STATUS "Added benchmark test ${TARGET_NAME} with config file ${CONFIG_FILE_NAME}")
endfunction()

message(STATUS "Building benchmarks tests...")

# Copy Test Configs --------------------------------------------------------------
file(COPY test-configs DESTINATION .)
add_compile_definitions(TEST_CONFIGS_DIRECTORY="${CMAKE_CURRENT_BINARY_DIR}/test-configs/")
message(STATUS "Test Data Directory: ${TEST_CONFIGS_DIRECTORY}/test-configs")

# Adding unit test for GenericDataGenerator
add_nes_unit_test(generic-data-generator-test "UnitTests/DataGeneration/GenericDataGeneratorDebug.cpp")
target_link_libraries(generic-data-generator-test nes-benchmark)

# Adding unit test for DefaultDataGenerator
add_nes_unit_test(default-data-generator-test "UnitTests/DataGeneration/DefaultDataGeneratorTest.cpp")
target_link_libraries(default-data-generator-test nes-benchmark)

# Adding unit test for ZipfianDataGenerator
add_nes_unit_test(zipfian-data-generator-test "UnitTests/DataGeneration/ZipfianDataGeneratorTest.cpp")
target_link_libraries(zipfian-data-generator-test nes-benchmark)

# Adding unit test for InternalProvider
add_nes_unit_test(internal-provider-test "UnitTests/DataProvider/InternalProviderTest.cpp")
target_link_libraries(internal-provider-test nes-benchmark)

# Adding unit test for ExternalProvider
add_nes_unit_test(external-provider-test "UnitTests/DataProvider/ExternalProviderTest.cpp")
target_link_libraries(external-provider-test nes-benchmark)

# Adding unit test for E2ESingleRunTest
add_nes_unit_test(e2e-single-run-test "UnitTests/E2E/E2ESingleRunTest.cpp")
target_link_libraries(e2e-single-run-test nes-benchmark)

# Adding unit test for E2EBenchmarkConfigPerRunTest
add_nes_unit_test(e2e-benchmark-config-per-run-test "UnitTests/E2E/Configurations/E2EBenchmarkConfigPerRunTest.cpp")
target_link_libraries(e2e-benchmark-config-per-run-test nes-benchmark)

# Adding unit test for E2EBenchmarkConfigOverAllRunsTest
add_nes_unit_test(e2e-benchmark-config-over-all-runs-test "UnitTests/E2E/Configurations/E2EBenchmarkConfigOverAllRunsTest.cpp")
target_link_libraries(e2e-benchmark-config-over-all-runs-test nes-benchmark)

# Adding unit test for different E2EBenchmark tests
add_nes_unit_test(e2e-benchmark-runner-test "UnitTests/E2E/E2ERunnerTest.cpp")
target_link_libraries(e2e-benchmark-runner-test nes-benchmark)

# Adding unit test for all ingestion rate generators
add_nes_unit_test(ingestion-rate-generation-test "UnitTests/IngestionRateGeneration/IngestionRateGenerationTest.cpp")
target_link_libraries(ingestion-rate-generation-test nes-benchmark)

message(STATUS "Done building benchmarks tests")
