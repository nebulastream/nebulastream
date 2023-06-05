
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_nes_runtime_test(runtime-statistics-collector-test "StatisticsCollectorTest.cpp")
add_nes_runtime_test(runtime-profiler-test "ProfilerTest.cpp")
add_nes_runtime_test(runtime-multiple-pipelines-test "MultiplePipelinesTest.cpp")
add_nes_runtime_test(runtime-selectivity-test "SelectivityTest.cpp")
add_nes_runtime_test(runtime-cache-misses-test "CacheMissesTest.cpp")
add_nes_runtime_test(runtime-change-detection-test "ChangeDetectionTest.cpp")
add_nes_runtime_test(runtime-performance-test "PerformanceTest.cpp")
add_nes_runtime_test(runtime-change-detectors-test "ChangeDetectorsBenchmark.cpp")