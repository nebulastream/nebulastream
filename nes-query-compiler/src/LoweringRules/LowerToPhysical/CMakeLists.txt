# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

register_entry(NLJoin LoweringRule LowerToPhysicalNLJoin.cpp)
register_entry(HashJoin LoweringRule LowerToPhysicalHashJoin.cpp)
register_entry(Selection LoweringRule LowerToPhysicalSelection.cpp)
register_entry(Projection LoweringRule LowerToPhysicalProjection.cpp)
register_entry(WindowedAggregation LoweringRule LowerToPhysicalWindowedAggregation.cpp)
register_entry(Union LoweringRule LowerToPhysicalUnion.cpp)
register_entry(Sink LoweringRule LowerToPhysicalSink.cpp)
register_entry(SourceDescriptor LoweringRule LowerToPhysicalSource.cpp)
register_entry(IngestionTimeWatermarkAssigner LoweringRule LowerToPhysicalIngestionTimeWatermarkAssigner.cpp)
register_entry(EventTimeWatermarkAssigner LoweringRule LowerToPhysicalEventTimeWatermarkAssigner.cpp)
register_entry(InferModel LoweringRule LowerToPhysicalInferModel.cpp)
