# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

### QueryExecutionTest ###
add_nes_unit_test(query-execution-test
        "FilterQueryExecutionTest.cpp"
        "ProjectionQueryExecutionTest.cpp"
        "MergeQueryExecutionTest.cpp"
        "MapQueryExecutionTest.cpp"
        "UnionQueryExecutionTest.cpp")

add_nes_unit_test(demo-city-query-execution-test "DemoCityQueryExecutionTest.cpp")
add_nes_unit_test(map-java-udf-query-execution-test "MapJavaUDFExecutionTest.cpp")
add_nes_unit_test(flatmap-java-udf-query-execution-test "FlatMapJavaUDFExecutionTest.cpp")
add_nes_unit_test(map-python-udf-query-execution-test "MapPythonUDFExecutionTest.cpp")
add_nes_unit_test(stream-join-execution-test "StreamJoinExecutionTest.cpp")
add_nes_unit_test(limit-query-execution-test "LimitQueryExecutionTest.cpp")

add_subdirectory(Windowing)