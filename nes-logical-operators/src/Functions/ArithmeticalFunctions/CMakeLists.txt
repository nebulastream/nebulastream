# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_plugin(Absolute UnaryLogicalFunction nes-logical-operators AbsoluteLogicalFunction.cpp)
add_plugin(Add BinaryLogicalFunction nes-logical-operators AddLogicalFunction.cpp)
add_plugin(Ceil UnaryLogicalFunction nes-logical-operators CeilLogicalFunction.cpp)
add_plugin(Div BinaryLogicalFunction nes-logical-operators DivLogicalFunction.cpp)
add_plugin(Exp UnaryLogicalFunction nes-logical-operators ExpLogicalFunction.cpp)
add_plugin(Floor UnaryLogicalFunction nes-logical-operators FloorLogicalFunction.cpp)
add_plugin(Modulo BinaryLogicalFunction nes-logical-operators ModuloLogicalFunction.cpp)
add_plugin(Mul BinaryLogicalFunction nes-logical-operators MulLogicalFunction.cpp)
add_plugin(Pow BinaryLogicalFunction nes-logical-operators PowLogicalFunction.cpp)
add_plugin(Round UnaryLogicalFunction nes-logical-operators RoundLogicalFunction.cpp)
add_plugin(Sqrt UnaryLogicalFunction nes-logical-operators SqrtLogicalFunction.cpp)
add_plugin(Sub BinaryLogicalFunction nes-logical-operators SubLogicalFunction.cpp)
