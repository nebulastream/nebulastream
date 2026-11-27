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
        DataType.cpp
        DataTypeProvider.cpp
        DataTypeRegistry.cpp
        ScalarTypes.cpp
        TimeUnit.cpp
        )

# Register per-type scalar plugins and algebraic type constructors
add_registry_entry(DataType Boolean KEY BOOLEAN)
add_registry_entry(DataType Char KEY CHAR)
add_registry_entry(DataType Float32 KEY FLOAT32)
add_registry_entry(DataType Float64 KEY FLOAT64)
add_registry_entry(DataType Int8 KEY INT8)
add_registry_entry(DataType Int16 KEY INT16)
add_registry_entry(DataType Int32 KEY INT32)
add_registry_entry(DataType Int64 KEY INT64)
add_registry_entry(DataType UInt8 KEY UINT8)
add_registry_entry(DataType UInt16 KEY UINT16)
add_registry_entry(DataType UInt32 KEY UINT32)
add_registry_entry(DataType UInt64 KEY UINT64)
add_registry_entry(DataType Undefined KEY UNDEFINED)
add_registry_entry(DataType VarSized KEY VARSIZED)
add_registry_entry(DataType Optional KEY OPTIONAL)
