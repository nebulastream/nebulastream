# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

register_entry(Abs LogicalFunction AbsoluteLogicalFunction.cpp)
register_entry(Add LogicalFunction AddLogicalFunction.cpp)
register_entry(Ceil LogicalFunction CeilLogicalFunction.cpp)
register_entry(Div LogicalFunction DivLogicalFunction.cpp)
register_entry(Exp LogicalFunction ExpLogicalFunction.cpp)
register_entry(Floor LogicalFunction FloorLogicalFunction.cpp)
register_entry(Mod LogicalFunction ModuloLogicalFunction.cpp)
register_entry(Mul LogicalFunction MulLogicalFunction.cpp)
register_entry(Pow LogicalFunction PowLogicalFunction.cpp)
register_entry(Round LogicalFunction RoundLogicalFunction.cpp)
register_entry(Sqrt LogicalFunction SqrtLogicalFunction.cpp)
register_entry(Sub LogicalFunction SubLogicalFunction.cpp)

add_unreflection_entry(LogicalFunction Absolute KEY Abs)
add_unreflection_entry(LogicalFunction Add)
add_unreflection_entry(LogicalFunction Ceil)
add_unreflection_entry(LogicalFunction Div)
add_unreflection_entry(LogicalFunction Exp)
add_unreflection_entry(LogicalFunction Floor)
add_unreflection_entry(LogicalFunction Modulo KEY Mod)
add_unreflection_entry(LogicalFunction Mul)
add_unreflection_entry(LogicalFunction Pow)
add_unreflection_entry(LogicalFunction Round)
add_unreflection_entry(LogicalFunction Sqrt)
add_unreflection_entry(LogicalFunction Sub)
