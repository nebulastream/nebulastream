# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_source_files(nes-query-compiler
        LowerToPhysicalNLJoin.cpp
        LowerToPhysicalHashJoin.cpp
        LowerToPhysicalSelection.cpp
        LowerToPhysicalProjection.cpp
        LowerToPhysicalWindowedAggregation.cpp
        LowerToPhysicalUnion.cpp
        LowerToPhysicalSink.cpp
        LowerToPhysicalSource.cpp
        LowerToPhysicalIngestionTimeWatermarkAssigner.cpp
        LowerToPhysicalEventTimeWatermarkAssigner.cpp
        LowerToPhysicalInferModel.cpp
)

add_registry_entry(LoweringRule NLJoin)
add_registry_entry(LoweringRule HashJoin)
add_registry_entry(LoweringRule Selection)
add_registry_entry(LoweringRule Projection)
add_registry_entry(LoweringRule WindowedAggregation)
add_registry_entry(LoweringRule Union)
add_registry_entry(LoweringRule Sink)
add_registry_entry(LoweringRule Source KEY SourceDescriptor)
add_registry_entry(LoweringRule IngestionTimeWatermarkAssigner)
add_registry_entry(LoweringRule EventTimeWatermarkAssigner)
add_registry_entry(LoweringRule InferModel)
