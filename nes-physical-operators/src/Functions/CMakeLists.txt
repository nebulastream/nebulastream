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
        PhysicalFunctionRegistry.cpp
        FunctionProvider.cpp
        FieldAccessPhysicalFunction.cpp
        ConstantValueVariableSizePhysicalFunction.cpp
        CastToPhysicalFunction.cpp
        )


add_subdirectory(ArithmeticalFunctions)
add_subdirectory(ComparisonFunctions)
add_subdirectory(BooleanFunctions)

add_source_files(nes-physical-operators
        ConcatPhysicalFunction.cpp
        OctetLengthPhysicalFunction.cpp
        CharLengthPhysicalFunction.cpp
        FromBase64PhysicalFunction.cpp
        ToBase64PhysicalFunction.cpp
        CastToPhysicalFunction.cpp
        CastToUnixTimestampPhysicalFunction.cpp
        CastFromUnixTimestampPhysicalFunction.cpp
        CastVarSizedToNumericPhysicalFunction.cpp
        ExtractFromTimestampPhysicalFunction.cpp
)

# Runtime registry entries
add_registry_entry(PhysicalFunction CastFromUnixTimestamp KEY CastFromUnixTs)
add_registry_entry(PhysicalFunction CastTo KEY CastToType)
add_registry_entry(PhysicalFunction CastToUnixTimestamp KEY CastToUnixTs)
add_registry_entry(PhysicalFunction CastVarSizedToNumeric KEY VarSizedToNumeric)
add_registry_entry(PhysicalFunction CharLength KEY CHAR_LENGTH)
add_registry_entry(PhysicalFunction Concat)
add_registry_entry(PhysicalFunction ExtractFromTimestamp KEY Day_Of)
add_registry_entry(PhysicalFunction ExtractFromTimestamp KEY Month_Of)
add_registry_entry(PhysicalFunction ExtractFromTimestamp KEY Year_Of)
add_registry_entry(PhysicalFunction FromBase64 KEY FROM_BASE64)
add_registry_entry(PhysicalFunction OctetLength KEY OCTET_LENGTH)
add_registry_entry(PhysicalFunction ToBase64 KEY TO_BASE64)
