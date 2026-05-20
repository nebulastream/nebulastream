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

add_plugin(ConstantValue LogicalFunction ConstantValueLogicalFunction.cpp)
add_plugin(FieldAccess LogicalFunction FieldAccessLogicalFunction.cpp)
add_plugin(Concat LogicalFunction ConcatLogicalFunction.cpp)
add_plugin(OCTET_LENGTH LogicalFunction OctetLengthLogicalFunction.cpp)
add_plugin(CHAR_LENGTH LogicalFunction CharLengthLogicalFunction.cpp)
add_plugin(FROM_BASE64 LogicalFunction FromBase64LogicalFunction.cpp)
add_plugin(TO_BASE64 LogicalFunction ToBase64LogicalFunction.cpp)
add_plugin(CastToType LogicalFunction CastToTypeLogicalFunction.cpp)
add_plugin(UnboundFieldAccess LogicalFunction UnboundFieldAccessLogicalFunction.cpp)
add_plugin(CastToUnixTs LogicalFunction CastToUnixTimestampLogicalFunction.cpp)
add_plugin(CastFromUnixTs LogicalFunction CastFromUnixTimestampLogicalFunction.cpp)

add_unreflection_plugin(LogicalFunction ConstantValue)
add_unreflection_plugin(LogicalFunction FieldAccess)
add_unreflection_plugin(LogicalFunction Concat)
add_unreflection_plugin(LogicalFunction CharLength KEY CHAR_LENGTH)
add_unreflection_plugin(LogicalFunction OctetLength KEY OCTET_LENGTH)
add_unreflection_plugin(LogicalFunction FromBase64 KEY FROM_BASE64)
add_unreflection_plugin(LogicalFunction ToBase64 KEY TO_BASE64)
add_unreflection_plugin(LogicalFunction CastToType)
add_unreflection_plugin(LogicalFunction CastToUnixTimestamp KEY CastToUnixTs)
add_unreflection_plugin(LogicalFunction CastFromUnixTimestamp KEY CastFromUnixTs)
