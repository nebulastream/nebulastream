# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_source_files(nes-legacy-optimizer
        DistributedQueryId.cpp
        NetworkTopology.cpp
        OperatorPlacement.cpp
        QueryDecomposition.cpp
        WorkerStatus.cpp
)

add_nes_common_test("NetworkTopologyTest" NetworkTopologyTest.cpp)
target_link_libraries(NetworkTopologyTest nes-nebuli)
target_include_directories(NetworkTopologyTest PRIVATE ../../private)

add_nes_common_test("DistributedPlanningTest" DistributedPlanningTest.cpp)
target_link_libraries(DistributedPlanningTest nes-nebuli nes-sql-parser)
target_include_directories(DistributedPlanningTest PRIVATE ../../private)
