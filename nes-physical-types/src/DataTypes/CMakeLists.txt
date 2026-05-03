# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_source_files(nes-physical-types
        DataType.cpp
        DataTypeProvider.cpp
        PhysicalLayout.cpp
        SchemaLowering.cpp
        )

# Register plugins
add_plugin(INTEGER DataType nes-physical-types DataType.cpp)
add_plugin(FLOAT DataType nes-physical-types DataType.cpp)
add_plugin(TEXT DataType nes-physical-types DataType.cpp)
add_plugin(BOOL DataType nes-physical-types DataType.cpp)

add_plugin(INTEGER LogicalTypeLowering nes-physical-types LogicalTypeLoweringRegistrations.cpp)
add_plugin(FLOAT LogicalTypeLowering nes-physical-types LogicalTypeLoweringRegistrations.cpp)
add_plugin(TEXT LogicalTypeLowering nes-physical-types LogicalTypeLoweringRegistrations.cpp)
add_plugin(BOOL LogicalTypeLowering nes-physical-types LogicalTypeLoweringRegistrations.cpp)
