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
        LogicalFunction.cpp
        LogicalFunctionProvider.cpp
)

add_plugin(ConstantValue LogicalFunction nes-logical-operators ConstantValueLogicalFunction.cpp)
add_plugin(FieldAccess LogicalFunction nes-logical-operators FieldAccessLogicalFunction.cpp)
add_plugin(FieldAssignment LogicalFunction nes-logical-operators FieldAssignmentLogicalFunction.cpp)
add_plugin(Rename LogicalFunction nes-logical-operators RenameLogicalFunction.cpp)
add_plugin(Concat LogicalFunction nes-logical-operators ConcatLogicalFunction.cpp)
add_plugin(CastToType LogicalFunction nes-logical-operators CastToTypeLogicalFunction.cpp)
