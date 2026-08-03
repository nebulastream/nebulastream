# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_source_files(nes-query-optimizer
        DecideJoinTypesRule.cpp
        DecideMemoryLayoutRule.cpp
        PredicatePushdownRule.cpp
        RedundantProjectionRemovalRule.cpp
        ProjectionPushdownRule.cpp
        WatermarkAssignerPushdownRule.cpp
        DecideFieldMappings.cpp
        DecideFieldOrder.cpp
        OriginIdInferenceRule.cpp)

add_registry_entry(PlanRule DecideJoinTypesRule KEY DecideJoinTypes)
add_registry_entry(PlanRule DecideMemoryLayoutRule KEY DecideMemoryLayout)
add_registry_entry(PlanRule PredicatePushdownRule KEY PredicatePushdown)
add_registry_entry(PlanRule RedundantProjectionRemovalRule KEY RedundantProjectionRemoval)
add_registry_entry(PlanRule ProjectionPushdownRule KEY ProjectionPushdown)
add_registry_entry(PlanRule WatermarkAssignerPushdownRule KEY WatermarkAssignerPushdown)
add_registry_entry(PlanRule DecideFieldMappings)
add_registry_entry(PlanRule DecideFieldOrder)
add_registry_entry(PlanRule OriginIdInferenceRule KEY OriginIdInference)
