# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_subdirectory(LogicalFunctions)
add_subdirectory(ArithmeticalFunctions)

add_source_files(nes-logical-operators
        Function.cpp
)

add_plugin(FieldAssignment Function nes-logical-operators FieldAssignmentFunction.cpp)
add_plugin(Rename Function nes-logical-operators RenameFunction.cpp)
add_plugin(Concat Function nes-logical-operators ConcatFunction.cpp)
add_plugin(ConstantValue Function nes-logical-operators ConstantValueFunction.cpp)
add_plugin(FieldAccess Function nes-logical-operators FieldAccessFunction.cpp)
