# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Copy Test Data
file(COPY test-configs DESTINATION .)
add_compile_definitions(TEST_CONFIGS_DIRECTORY="${CMAKE_CURRENT_BINARY_DIR}/test-configs/")
message(STATUS "Test Data Directory: ${TEST_DATA_DIRECTORY}/test-configs")

# Adding unit test for SynopsisConfigurationTest
add_nes_unit_test(synopsis-configuration-test "UnitTests/Experimental/Benchmarking/Parsing/SynopsisConfigurationTest.cpp")
target_link_libraries(synopsis-configuration-test nes-asp)

# Adding unit test for YamlAggregationTest
add_nes_unit_test(yaml-aggregation-test "UnitTests/Experimental/Benchmarking/Parsing/YamlAggregationTest.cpp")
target_link_libraries(yaml-aggregation-test nes-asp)

# Adding unit test for MicroBenchmarkASPUtilTest
add_nes_unit_test(micro-benchmarking-asp-util-test "UnitTests/Experimental/Benchmarking/MicroBenchmarkASPUtilTest.cpp")
target_link_libraries(micro-benchmarking-asp-util-test nes-asp)

# Adding unit test for MicroBenchmarkResultTest
add_nes_unit_test(micro-benchmarking-result-test "UnitTests/Experimental/Benchmarking/MicroBenchmarkResultTest.cpp")
target_link_libraries(micro-benchmarking-result-test nes-asp)

# Adding unit test for MicroBenchmarkRun.cpp
add_nes_unit_test(micro-benchmarking-run-test "UnitTests/Experimental/Benchmarking/MicroBenchmarkRunTest.cpp")
target_link_libraries(micro-benchmarking-run-test nes-asp)