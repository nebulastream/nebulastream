# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

### NES Topology Tests ###
add_nes_unit_test(topology-tests
        TopologyTest.cpp
        TopologyPropertyTest.cpp
        TopologyIterationTest.cpp
        Prediction/TopologyTimelineTest.cpp
        Prediction/TopologyLinkInformationTest.cpp
        Prediction/TopologyChangelogTest.cpp
        Prediction/TopologyDeltaTest.cpp)