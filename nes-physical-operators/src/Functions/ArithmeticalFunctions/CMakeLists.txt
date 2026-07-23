# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_source_files(nes-physical-operators
        AbsolutePhysicalFunction.cpp
        AddPhysicalFunction.cpp
        CeilPhysicalFunction.cpp
        DivPhysicalFunction.cpp
        FloorPhysicalFunction.cpp
        ModPhysicalFunction.cpp
        MulPhysicalFunction.cpp
        PowPhysicalFunction.cpp
        RoundPhysicalFunction.cpp
        SubPhysicalFunction.cpp
        SqrtPhysicalFunction.cpp
)

# Runtime registry entries
add_registry_entry(PhysicalFunction Absolute KEY Abs)
add_registry_entry(PhysicalFunction Add)
add_registry_entry(PhysicalFunction Ceil)
add_registry_entry(PhysicalFunction Div)
add_registry_entry(PhysicalFunction Floor)
add_registry_entry(PhysicalFunction Mod)
add_registry_entry(PhysicalFunction Mul)
add_registry_entry(PhysicalFunction Pow)
add_registry_entry(PhysicalFunction Round)
add_registry_entry(PhysicalFunction Sqrt)
add_registry_entry(PhysicalFunction Sub)
