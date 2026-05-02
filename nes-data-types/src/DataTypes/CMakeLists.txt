# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_source_files(nes-data-types
        Schema.cpp
        DataType.cpp
        DataTypeProvider.cpp
        LogicalType.cpp
        LogicalSchema.cpp
        TimeUnit.cpp
        )

# Register plugins
add_plugin(BOOLEAN DataType nes-data-types DataType.cpp)
add_plugin(CHAR DataType nes-data-types DataType.cpp)
add_plugin(FLOAT32 DataType nes-data-types DataType.cpp)
add_plugin(FLOAT64 DataType nes-data-types DataType.cpp)
add_plugin(INT8 DataType nes-data-types DataType.cpp)
add_plugin(INT16 DataType nes-data-types DataType.cpp)
add_plugin(INT32 DataType nes-data-types DataType.cpp)
add_plugin(INT64 DataType nes-data-types DataType.cpp)
add_plugin(UINT8 DataType nes-data-types DataType.cpp)
add_plugin(UINT16 DataType nes-data-types DataType.cpp)
add_plugin(UINT32 DataType nes-data-types DataType.cpp)
add_plugin(UINT64 DataType nes-data-types DataType.cpp)
add_plugin(UNDEFINED DataType nes-data-types DataType.cpp)
add_plugin(VARSIZED DataType nes-data-types DataType.cpp)

# Built-in logical-type plugins. Each shares the name of the underlying physical
# DataType so legacy primitive lookups continue to round-trip cleanly.
add_plugin(BOOLEAN LogicalType nes-data-types LogicalTypeRegistrations.cpp)
add_plugin(CHAR LogicalType nes-data-types LogicalTypeRegistrations.cpp)
add_plugin(FLOAT32 LogicalType nes-data-types LogicalTypeRegistrations.cpp)
add_plugin(FLOAT64 LogicalType nes-data-types LogicalTypeRegistrations.cpp)
add_plugin(INT8 LogicalType nes-data-types LogicalTypeRegistrations.cpp)
add_plugin(INT16 LogicalType nes-data-types LogicalTypeRegistrations.cpp)
add_plugin(INT32 LogicalType nes-data-types LogicalTypeRegistrations.cpp)
add_plugin(INT64 LogicalType nes-data-types LogicalTypeRegistrations.cpp)
add_plugin(UINT8 LogicalType nes-data-types LogicalTypeRegistrations.cpp)
add_plugin(UINT16 LogicalType nes-data-types LogicalTypeRegistrations.cpp)
add_plugin(UINT32 LogicalType nes-data-types LogicalTypeRegistrations.cpp)
add_plugin(UINT64 LogicalType nes-data-types LogicalTypeRegistrations.cpp)
add_plugin(UNDEFINED LogicalType nes-data-types LogicalTypeRegistrations.cpp)
add_plugin(VARSIZED LogicalType nes-data-types LogicalTypeRegistrations.cpp)


