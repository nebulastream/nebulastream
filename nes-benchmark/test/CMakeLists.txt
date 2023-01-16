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
    #add_test(NAME ${TARGET_NAME} COMMAND e2e-benchmark-runner --configPath=${PROJECT_SOURCE_DIR}/${CONFIG_FILE_NAME} --logPath=${TARGET_NAME}.log)
    message(STATUS "Added benchmark test ${TARGET_NAME} with config file ${CONFIG_FILE_NAME}")
endfunction()

message(STATUS "Building benchmarks tests...")


# TODO create unit tests and a similar test infrastructure than the other folders see issue #3227
# Adding e2e tests
add_benchmark_test(e2e-benchmark-test-filter nes-benchmark/test/test-configs/filter_one_source.yaml)

# Adding unit test for DefaultDataGenerator
add_nes_unit_test(default-data-generator-test "UnitTests/DataGeneration/DefaultDataGeneratorTest.cpp")
target_link_libraries(default-data-generator-test nes-benchmark)

# Adding unit test for ZipfianDataGenerator
add_nes_unit_test(zipfian-data-generator-test "UnitTests/DataGeneration/ZipfianDataGeneratorTest.cpp")
target_link_libraries(zipfian-data-generator-test nes-benchmark)

# Adding unit test for InternalProvider
add_nes_unit_test(internal-provider-test "UnitTests/DataProvider/InternalProviderTest.cpp")
target_link_libraries(internal-provider-test nes-benchmark)

message(STATUS "Done building benchmarks tests")
