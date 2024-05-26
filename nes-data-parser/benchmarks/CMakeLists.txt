# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_library(nes-data-parser-benchmark INTERFACE)
target_include_directories(nes-data-parser-benchmark INTERFACE .)

find_package(benchmark REQUIRED)
add_executable(schema-buffer-benchmark "SchemaBufferBenchmark.cpp")
target_link_libraries(schema-buffer-benchmark PRIVATE nes-data-parser benchmark::benchmark scn::scn simdjson nes-runtime nes-data-parser-benchmark)



