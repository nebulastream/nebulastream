# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_plugin(LowerToPhysicalNLJoin RewriteRule nes-optimizer LowerToPhysicalNLJoin.cpp)
add_plugin(LowerToPhysicalMap RewriteRule nes-optimizer LowerToPhysicalMap.cpp)
add_plugin(LowerToPhysicalSelection RewriteRule nes-optimizer LowerToPhysicalSelection.cpp)
add_plugin(LowerToPhysicalWindowedAggregation RewriteRule nes-optimizer LowerToPhysicalWindowedAggregation.cpp)
