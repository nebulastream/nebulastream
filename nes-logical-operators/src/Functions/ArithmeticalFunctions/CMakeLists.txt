# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_plugin(Absolute LogicalFunction nes-logical-operators AbsoluteLogicalFunction.cpp)
add_plugin(Add LogicalFunction nes-logical-operators AddLogicalFunction.cpp)
add_plugin(Ceil LogicalFunction nes-logical-operators CeilLogicalFunction.cpp)
add_plugin(Div LogicalFunction nes-logical-operators DivLogicalFunction.cpp)
add_plugin(Exp LogicalFunction nes-logical-operators ExpLogicalFunction.cpp)
add_plugin(Floor LogicalFunction nes-logical-operators FloorLogicalFunction.cpp)
add_plugin(Mod LogicalFunction nes-logical-operators ModuloLogicalFunction.cpp)
add_plugin(Mul LogicalFunction nes-logical-operators MulLogicalFunction.cpp)
add_plugin(Pow LogicalFunction nes-logical-operators PowLogicalFunction.cpp)
add_plugin(Round LogicalFunction nes-logical-operators RoundLogicalFunction.cpp)
add_plugin(Sqrt LogicalFunction nes-logical-operators SqrtLogicalFunction.cpp)
add_plugin(Sub LogicalFunction nes-logical-operators SubLogicalFunction.cpp)
