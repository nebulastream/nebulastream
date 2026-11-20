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
        NullValuePhysicalFunction.cpp
        OctetLengthPhysicalFunction.cpp
        CharLengthPhysicalFunction.cpp
        FromBase64PhysicalFunction.cpp
        ToBase64PhysicalFunction.cpp
        CastToPhysicalFunction.cpp
        CastToUnixTimestampPhysicalFunction.cpp
        CastFromUnixTimestampPhysicalFunction.cpp
        CastVarSizedToNumericPhysicalFunction.cpp
        ExtractFromTimestampPhysicalFunction.cpp
        SqlPhysicalFunction.cpp
)

add_registry_entry(PhysicalFunction CharLength KEY CHAR_LENGTH)
add_registry_entry(PhysicalFunction Concat)
add_registry_entry(PhysicalFunction FromBase64 KEY FROM_BASE64)
add_registry_entry(FunctionSignature FromBase64 KEY FROM_BASE64)
add_registry_entry(PhysicalFunction OctetLength KEY OCTET_LENGTH)
add_registry_entry(PhysicalFunction ToBase64 KEY TO_BASE64)
add_registry_entry(PhysicalFunction Sql KEY Not_Equal)
add_registry_entry(PhysicalFunction Sql KEY Is_Not_Null)
add_registry_entry(PhysicalFunction Sql KEY Is_Not_True)
add_registry_entry(PhysicalFunction Sql KEY Is_Nan)
add_registry_entry(PhysicalFunction Sql KEY Exp)
add_registry_entry(PhysicalFunction Sql KEY Negate)
add_registry_entry(PhysicalFunction Sql KEY Cast)
add_registry_entry(PhysicalFunction Sql KEY Timestamp_Millis)
add_registry_entry(PhysicalFunction Sql KEY Unix_Millis)
add_registry_entry(PhysicalFunction Sql KEY Extract)
add_registry_entry(PhysicalFunction Sql KEY Format_Datetime)
add_registry_entry(PhysicalFunction Sql KEY Timestamp)
