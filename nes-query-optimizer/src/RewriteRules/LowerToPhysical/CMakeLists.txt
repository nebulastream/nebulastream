# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_plugin(Join RewriteRule nes-query-optimizer LowerToPhysicalNLJoin.cpp)
add_plugin(Map RewriteRule nes-query-optimizer LowerToPhysicalMap.cpp)
add_plugin(Selection RewriteRule nes-query-optimizer LowerToPhysicalSelection.cpp)
add_plugin(Projection RewriteRule nes-query-optimizer LowerToPhysicalProjection.cpp)
add_plugin(WindowedAggregation RewriteRule nes-query-optimizer LowerToPhysicalWindowedAggregation.cpp)
add_plugin(Union RewriteRule nes-query-optimizer LowerToPhysicalUnion.cpp)
add_plugin(Sink RewriteRule nes-query-optimizer LowerToPhysicalSink.cpp)
add_plugin(Source RewriteRule nes-query-optimizer LowerToPhysicalSource.cpp)
add_plugin(Sequence RewriteRule nes-query-optimizer LowerToPhysicalSequence.cpp)
add_plugin(IngestionTimeWatermarkAssigner RewriteRule nes-query-optimizer LowerToPhysicalIngestionTimeWatermarkAssigner.cpp)
add_plugin(EventTimeWatermarkAssigner RewriteRule nes-query-optimizer LowerToPhysicalEventTimeWatermarkAssigner.cpp)
