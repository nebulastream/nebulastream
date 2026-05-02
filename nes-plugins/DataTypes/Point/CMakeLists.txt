# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Point logical type registration (LogicalType plugins still attached to nes-data-types)
add_plugin(Point LogicalType nes-data-types PointTypeRegistration.cpp)

# Point logical functions
add_plugin(Point LogicalFunction nes-logical-operators PointLogicalFunctions.cpp)
add_plugin(PointAdd LogicalFunction nes-logical-operators PointLogicalFunctions.cpp)
add_plugin(PointSub LogicalFunction nes-logical-operators PointLogicalFunctions.cpp)
add_plugin(PointScale LogicalFunction nes-logical-operators PointLogicalFunctions.cpp)
add_plugin(PointDistance LogicalFunction nes-logical-operators PointLogicalFunctions.cpp)
add_plugin(PointEquals LogicalFunction nes-logical-operators PointLogicalFunctions.cpp)
# Mangled overload entries — the parser emits Unbound("<op>", args) for binary
# operators and the resolver builds the key as "<op>_<lhsType>_<rhsType>",
# which these plugins satisfy.
add_plugin(Equals_Point_Point LogicalFunction nes-logical-operators PointLogicalFunctions.cpp)
add_plugin(Add_Point_Point LogicalFunction nes-logical-operators PointLogicalFunctions.cpp)
add_plugin(Mul_Point_INTEGER LogicalFunction nes-logical-operators PointLogicalFunctions.cpp)
add_plugin(Mul_INTEGER_Point LogicalFunction nes-logical-operators PointLogicalFunctions.cpp)

# Point lowering: PhysicalLayout = [(_X,F64),(_Y,F64),(_Z,F64)]
add_plugin(Point LogicalTypeLowering nes-physical-types PointLowering.cpp)

# Point physical functions
add_plugin(Point PhysicalFunction nes-physical-operators PointPhysicalFunctions.cpp)
add_plugin(PointAdd PhysicalFunction nes-physical-operators PointPhysicalFunctions.cpp)
add_plugin(PointSub PhysicalFunction nes-physical-operators PointPhysicalFunctions.cpp)
add_plugin(PointScale PhysicalFunction nes-physical-operators PointPhysicalFunctions.cpp)
add_plugin(PointDistance PhysicalFunction nes-physical-operators PointPhysicalFunctions.cpp)
add_plugin(PointEquals PhysicalFunction nes-physical-operators PointPhysicalFunctions.cpp)

# Make the logical-fn header visible to translation units in nes-logical-operators
target_include_directories(nes-logical-operators PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_include_directories(nes-physical-operators PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
