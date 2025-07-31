# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_plugin(NLJoin LoweringRule nes-query-compiler LowerToPhysicalNLJoin.cpp)
add_plugin(HashJoin LoweringRule nes-query-compiler LowerToPhysicalHashJoin.cpp)
add_plugin(Selection LoweringRule nes-query-compiler LowerToPhysicalSelection.cpp)
add_plugin(Projection LoweringRule nes-query-compiler LowerToPhysicalProjection.cpp)
add_plugin(WindowedAggregation LoweringRule nes-query-compiler LowerToPhysicalWindowedAggregation.cpp)
add_plugin(Union LoweringRule nes-query-compiler LowerToPhysicalUnion.cpp)
add_plugin(Sink LoweringRule nes-query-compiler LowerToPhysicalSink.cpp)
add_plugin(SourceDescriptor LoweringRule nes-query-compiler LowerToPhysicalSource.cpp)
add_plugin(IngestionTimeWatermarkAssigner LoweringRule nes-query-compiler LowerToPhysicalIngestionTimeWatermarkAssigner.cpp)
add_plugin(EventTimeWatermarkAssigner LoweringRule nes-query-compiler LowerToPhysicalEventTimeWatermarkAssigner.cpp)
