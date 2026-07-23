# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_registry_entry(NLJoin LoweringRule LowerToPhysicalNLJoin.cpp)
add_registry_entry(HashJoin LoweringRule LowerToPhysicalHashJoin.cpp)
add_registry_entry(Selection LoweringRule LowerToPhysicalSelection.cpp)
add_registry_entry(Projection LoweringRule LowerToPhysicalProjection.cpp)
add_registry_entry(WindowedAggregation LoweringRule LowerToPhysicalWindowedAggregation.cpp)
add_registry_entry(Union LoweringRule LowerToPhysicalUnion.cpp)
add_registry_entry(Sink LoweringRule LowerToPhysicalSink.cpp)
add_registry_entry(SourceDescriptor LoweringRule LowerToPhysicalSource.cpp)
add_registry_entry(IngestionTimeWatermarkAssigner LoweringRule LowerToPhysicalIngestionTimeWatermarkAssigner.cpp)
add_registry_entry(EventTimeWatermarkAssigner LoweringRule LowerToPhysicalEventTimeWatermarkAssigner.cpp)
add_registry_entry(InferModel LoweringRule LowerToPhysicalInferModel.cpp)
