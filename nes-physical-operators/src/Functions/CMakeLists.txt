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
        FunctionProvider.cpp
        FieldAccessPhysicalFunction.cpp
        ConstantValueVariableSizePhysicalFunction.cpp
        CastToPhysicalFunction.cpp
        )

add_plugin(Concat PhysicalFunction ConcatPhysicalFunction.cpp)
add_plugin(OCTET_LENGTH PhysicalFunction OctetLengthPhysicalFunction.cpp)
add_plugin(CHAR_LENGTH PhysicalFunction CharLengthPhysicalFunction.cpp)
add_plugin(FROM_BASE64 PhysicalFunction FromBase64PhysicalFunction.cpp)
add_plugin(TO_BASE64 PhysicalFunction ToBase64PhysicalFunction.cpp)
add_plugin(CastToType PhysicalFunction CastToPhysicalFunction.cpp)
add_plugin(CastToUnixTs PhysicalFunction CastToUnixTimestampPhysicalFunction.cpp)
add_plugin(CastFromUnixTs PhysicalFunction CastFromUnixTimestampPhysicalFunction.cpp)
add_plugin(VarSizedToNumeric PhysicalFunction CastVarSizedToNumericPhysicalFunction.cpp)
add_plugin(Day_Of PhysicalFunction ExtractFromTimestampPhysicalFunction.cpp)
add_plugin(Month_Of PhysicalFunction ExtractFromTimestampPhysicalFunction.cpp)
add_plugin(Year_Of PhysicalFunction ExtractFromTimestampPhysicalFunction.cpp)

add_subdirectory(ArithmeticalFunctions)
add_subdirectory(ComparisonFunctions)
add_subdirectory(BooleanFunctions)
