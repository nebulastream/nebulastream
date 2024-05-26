# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_nes_unit_test(schema-buffer-test "SchemaBufferTest.cpp" "RuntimeSchemaAdapterTest.cpp")
target_link_libraries(schema-buffer-test nes-data-parser nes-data-parser-benchmark simdjson scn::scn)

add_nes_unit_test(parser-factory-test "ParserFactoryTest.cpp")
target_link_libraries(parser-factory-test nes-data-parser)

add_nes_unit_test(simdjson-test "JSONTest.cpp")
target_link_libraries(simdjson-test nes-data-parser nes-data-parser-benchmark simdjson)
target_compile_options(simdjson-test PRIVATE "-fsanitize=address" "-fsanitize=undefined")
target_link_options(simdjson-test PRIVATE "-fsanitize=address")

add_nes_unit_test(csv-test "CSVTest.cpp")
target_link_libraries(csv-test nes-data-parser scn::scn nes-data-parser-benchmark)
target_compile_options(csv-test PRIVATE "-fsanitize=address" "-fsanitize=undefined")
target_link_options(csv-test PRIVATE "-fsanitize=address")
