# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


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

add_source_files(nes-logical-operators
        AbsoluteLogicalFunction.cpp
        AddLogicalFunction.cpp
        CeilLogicalFunction.cpp
        DivLogicalFunction.cpp
        ExpLogicalFunction.cpp
        FloorLogicalFunction.cpp
        ModuloLogicalFunction.cpp
        MulLogicalFunction.cpp
        PowLogicalFunction.cpp
        RoundLogicalFunction.cpp
        SqrtLogicalFunction.cpp
        SubLogicalFunction.cpp
)

add_registry_entry(LogicalFunction Absolute KEY Abs)
add_registry_entry(LogicalFunction Add)
add_registry_entry(LogicalFunction Ceil)
add_registry_entry(LogicalFunction Div)
add_registry_entry(LogicalFunction Exp)
add_registry_entry(LogicalFunction Floor)
add_registry_entry(LogicalFunction Modulo KEY Mod)
add_registry_entry(LogicalFunction Mul)
add_registry_entry(LogicalFunction Pow)
add_registry_entry(LogicalFunction Round)
add_registry_entry(LogicalFunction Sqrt)
add_registry_entry(LogicalFunction Sub)
