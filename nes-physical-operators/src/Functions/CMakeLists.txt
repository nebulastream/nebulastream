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

register_entry(Concat PhysicalFunction ConcatPhysicalFunction.cpp)
register_entry(OCTET_LENGTH PhysicalFunction OctetLengthPhysicalFunction.cpp)
register_entry(CHAR_LENGTH PhysicalFunction CharLengthPhysicalFunction.cpp)
register_entry(FROM_BASE64 PhysicalFunction FromBase64PhysicalFunction.cpp)
register_entry(TO_BASE64 PhysicalFunction ToBase64PhysicalFunction.cpp)
register_entry(CastToType PhysicalFunction CastToPhysicalFunction.cpp)
register_entry(CastToUnixTs PhysicalFunction CastToUnixTimestampPhysicalFunction.cpp)
register_entry(CastFromUnixTs PhysicalFunction CastFromUnixTimestampPhysicalFunction.cpp)
register_entry(VarSizedToNumeric PhysicalFunction CastVarSizedToNumericPhysicalFunction.cpp)
register_entry(Day_Of PhysicalFunction ExtractFromTimestampPhysicalFunction.cpp)
register_entry(Month_Of PhysicalFunction ExtractFromTimestampPhysicalFunction.cpp)
register_entry(Year_Of PhysicalFunction ExtractFromTimestampPhysicalFunction.cpp)

add_subdirectory(ArithmeticalFunctions)
add_subdirectory(ComparisonFunctions)
add_subdirectory(BooleanFunctions)
