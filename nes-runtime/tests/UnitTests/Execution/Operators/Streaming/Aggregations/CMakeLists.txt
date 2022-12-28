
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_executable(runtime-global-slice-pre-aggregation-test "GlobalSlicePreAggregationTest.cpp")
target_link_libraries(runtime-global-slice-pre-aggregation-test nes-runtime-tests ${GTEST_LIBRARIES})
add_test(NAME runtime-global-slice-pre-aggregation-test COMMAND runtime-global-slice-pre-aggregation-test)


add_executable(runtime-multi-origin-watermark-processor-test "MultiOriginWatermarkProcessorTest.cpp")
target_link_libraries(runtime-multi-origin-watermark-processor-test nes-runtime-tests nes-tests-util)
add_test(NAME runtime-multi-origin-watermark-processor-test COMMAND runtime-multi-origin-watermark-processor-test)


add_executable(runtime-slice-assigner-test "SliceAssignerTest.cpp")
target_link_libraries(runtime-slice-assigner-test nes-runtime-tests nes-tests-util)
add_test(NAME runtime-slice-assigner-test COMMAND runtime-slice-assigner-test)