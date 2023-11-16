
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

# QueryTest
add_nes_unit_test(query-tests "API/QueryAPITest.cpp")
# SpatialQueryTest
add_nes_unit_test(spatial-query-tests "API/SpatialQueryTest.cpp")

add_nes_unit_test(remote-client-test "Client/RemoteClientTest.cpp")
target_link_libraries(remote-client-test nes-client nes-coordinator nes-common)
set_tests_properties(${Tests} PROPERTIES TIMEOUT 35)

