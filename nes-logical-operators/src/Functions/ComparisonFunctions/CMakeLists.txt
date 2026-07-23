# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


add_unreflection_entry(LogicalFunction Greater)
add_unreflection_entry(LogicalFunction GreaterEquals)
add_unreflection_entry(LogicalFunction Less)
add_unreflection_entry(LogicalFunction LessEquals)

add_source_files(nes-logical-operators
        GreaterLogicalFunction.cpp
        GreaterEqualsLogicalFunction.cpp
        LessLogicalFunction.cpp
        LessEqualsLogicalFunction.cpp
)

add_registry_entry(LogicalFunction Greater)
add_registry_entry(LogicalFunction GreaterEquals)
add_registry_entry(LogicalFunction Less)
add_registry_entry(LogicalFunction LessEquals)
