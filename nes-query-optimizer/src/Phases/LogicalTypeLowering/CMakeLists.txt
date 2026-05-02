# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_source_files(nes-query-optimizer
        PhysicalLayout.cpp
)

# Built-in lowerings: each primitive logical type maps to its same-named physical
# DataType as a single-component scalar layout (suffix "").
add_plugin(BOOLEAN LogicalTypeLowering nes-query-optimizer BuiltinLowerings.cpp)
add_plugin(CHAR LogicalTypeLowering nes-query-optimizer BuiltinLowerings.cpp)
add_plugin(FLOAT32 LogicalTypeLowering nes-query-optimizer BuiltinLowerings.cpp)
add_plugin(FLOAT64 LogicalTypeLowering nes-query-optimizer BuiltinLowerings.cpp)
add_plugin(INT8 LogicalTypeLowering nes-query-optimizer BuiltinLowerings.cpp)
add_plugin(INT16 LogicalTypeLowering nes-query-optimizer BuiltinLowerings.cpp)
add_plugin(INT32 LogicalTypeLowering nes-query-optimizer BuiltinLowerings.cpp)
add_plugin(INT64 LogicalTypeLowering nes-query-optimizer BuiltinLowerings.cpp)
add_plugin(UINT8 LogicalTypeLowering nes-query-optimizer BuiltinLowerings.cpp)
add_plugin(UINT16 LogicalTypeLowering nes-query-optimizer BuiltinLowerings.cpp)
add_plugin(UINT32 LogicalTypeLowering nes-query-optimizer BuiltinLowerings.cpp)
add_plugin(UINT64 LogicalTypeLowering nes-query-optimizer BuiltinLowerings.cpp)
add_plugin(UNDEFINED LogicalTypeLowering nes-query-optimizer BuiltinLowerings.cpp)
add_plugin(VARSIZED LogicalTypeLowering nes-query-optimizer BuiltinLowerings.cpp)
