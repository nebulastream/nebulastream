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
        Numeric.cpp
        DataTypeProvider.cpp
        Boolean.cpp
        Char.cpp
        Float.cpp
        Integer.cpp
        Undefined.cpp
        VariableSizedDataType.cpp
        )

# Register plugins
add_plugin(BOOLEAN DataType)
add_plugin(CHAR DataType)
add_plugin(FLOAT32 DataType)
add_plugin(FLOAT64 DataType)
add_plugin(INT8 DataType)
add_plugin(INT16 DataType)
add_plugin(INT32 DataType)
add_plugin(INT64 DataType)
add_plugin(UINT8 DataType)
add_plugin(UINT16 DataType)
add_plugin(UINT32 DataType)
add_plugin(UINT64 DataType)
add_plugin(UNDEFINED DataType)
add_plugin(VARSIZED DataType)


