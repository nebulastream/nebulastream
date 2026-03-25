# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

corrosion_add_cxxbridge(
        nes-coordinator-bridge
        CRATE coordinator_bridge
        FILES ../coordinator/lib.rs
)
target_sources(nes-coordinator-bridge PRIVATE
        src/CatalogReadInterface.cpp
        src/SqlPlannerBridge.cpp)
target_include_directories(nes-coordinator-bridge
        PRIVATE .
        PUBLIC include)
target_link_libraries(nes-coordinator-bridge PUBLIC sql-planner)
