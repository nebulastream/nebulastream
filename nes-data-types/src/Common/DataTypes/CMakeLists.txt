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
)

add_source_files_as_plugins(nes-data-types
        BOOLEAN DataType Boolean.cpp
        CHAR DataType Char.cpp
        FLOAT32 DataType Float.cpp
        FLOAT64 DataType Float.cpp
        INT8 DataType Integer.cpp
        INT16 DataType Integer.cpp
        INT32 DataType Integer.cpp
        INT64 DataType Integer.cpp
        UINT8 DataType Integer.cpp
        UINT16 DataType Integer.cpp
        UINT32 DataType Integer.cpp
        UINT64 DataType Integer.cpp
        UNDEFINED DataType Undefined.cpp
        VARSIZED DataType VariableSizedDataType.cpp
)



