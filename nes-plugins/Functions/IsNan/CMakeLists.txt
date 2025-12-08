# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

include(${PROJECT_SOURCE_DIR}/cmake/PluginRegistrationUtil.cmake)
# add_plugin(ConstantValue LogicalFunction nes-logical-operators ConstantValueLogicalFunction.cpp)
# add_plugin(Concat PhysicalFunction nes-physical-operators ConcatPhysicalFunction.cpp)

add_plugin(IsNaN LogicalFunction nes-logical-operators IsNaNLogicalFunction.cpp)
add_plugin(IsNaN PhysicalFunction nes-physical-operators IsNaNPhysicalFunction.cpp)