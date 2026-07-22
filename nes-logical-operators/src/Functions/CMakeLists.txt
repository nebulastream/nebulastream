# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_subdirectory(BooleanFunctions)
add_subdirectory(ArithmeticalFunctions)
add_subdirectory(ComparisonFunctions)

add_source_files(nes-logical-operators
        LogicalFunctionProvider.cpp
)

register_entry(ConstantValue LogicalFunction ConstantValueLogicalFunction.cpp)
register_entry(FieldAccess LogicalFunction FieldAccessLogicalFunction.cpp)
register_entry(Concat LogicalFunction ConcatLogicalFunction.cpp)
register_entry(OCTET_LENGTH LogicalFunction OctetLengthLogicalFunction.cpp)
register_entry(CHAR_LENGTH LogicalFunction CharLengthLogicalFunction.cpp)
register_entry(FROM_BASE64 LogicalFunction FromBase64LogicalFunction.cpp)
register_entry(TO_BASE64 LogicalFunction ToBase64LogicalFunction.cpp)
register_entry(CastToType LogicalFunction CastToTypeLogicalFunction.cpp)
register_entry(UnboundFieldAccess LogicalFunction UnboundFieldAccessLogicalFunction.cpp)
register_entry(CastToUnixTs LogicalFunction CastToUnixTimestampLogicalFunction.cpp)
register_entry(CastFromUnixTs LogicalFunction CastFromUnixTimestampLogicalFunction.cpp)
register_entry(VarSizedToNumeric LogicalFunction VarSizedToNumericLogicalFunction.cpp)
register_entry(Day_Of LogicalFunction ExtractFromTimestampLogicalFunction.cpp)
register_entry(Month_Of LogicalFunction ExtractFromTimestampLogicalFunction.cpp)
register_entry(Year_Of LogicalFunction ExtractFromTimestampLogicalFunction.cpp)

add_unreflection_entry(LogicalFunction ConstantValue)
add_unreflection_entry(LogicalFunction FieldAccess)
add_unreflection_entry(LogicalFunction Concat)
add_unreflection_entry(LogicalFunction CharLength KEY CHAR_LENGTH)
add_unreflection_entry(LogicalFunction OctetLength KEY OCTET_LENGTH)
add_unreflection_entry(LogicalFunction FromBase64 KEY FROM_BASE64)
add_unreflection_entry(LogicalFunction ToBase64 KEY TO_BASE64)
add_unreflection_entry(LogicalFunction CastToType)
add_unreflection_entry(LogicalFunction CastToUnixTimestamp KEY CastToUnixTs)
add_unreflection_entry(LogicalFunction CastFromUnixTimestamp KEY CastFromUnixTs)
add_unreflection_entry(LogicalFunction VarSizedToNumeric)
add_unreflection_entry(LogicalFunction ExtractFromTimestamp KEY Day_Of)
add_unreflection_entry(LogicalFunction ExtractFromTimestamp KEY Month_Of)
add_unreflection_entry(LogicalFunction ExtractFromTimestamp KEY Year_Of)
