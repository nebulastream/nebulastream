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
        TimeUnit.cpp
        )

# Register plugins
register_entry(BOOLEAN DataType DataType.cpp)
register_entry(CHAR DataType DataType.cpp)
register_entry(FLOAT32 DataType DataType.cpp)
register_entry(FLOAT64 DataType DataType.cpp)
register_entry(INT8 DataType DataType.cpp)
register_entry(INT16 DataType DataType.cpp)
register_entry(INT32 DataType DataType.cpp)
register_entry(INT64 DataType DataType.cpp)
register_entry(UINT8 DataType DataType.cpp)
register_entry(UINT16 DataType DataType.cpp)
register_entry(UINT32 DataType DataType.cpp)
register_entry(UINT64 DataType DataType.cpp)
register_entry(UNDEFINED DataType DataType.cpp)
register_entry(VARSIZED DataType DataType.cpp)


