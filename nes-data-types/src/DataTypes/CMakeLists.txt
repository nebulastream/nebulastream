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
        BasicDataTypes.cpp
        DataType.cpp
        DataTypeProvider.cpp
        DataTypeRegistry.cpp
        TimeUnit.cpp
        )

# PRIVATE: lets this component's sources use <BasicDataTypes.hpp> without exposing src/DataTypes to consumers.
target_include_directories(nes-data-types PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

# Register the built-in types.
add_registry_entry(DataType BOOLEAN HEADER "BasicDataTypes.hpp")
add_registry_entry(DataType CHAR HEADER "BasicDataTypes.hpp")
add_registry_entry(DataType FLOAT32 HEADER "BasicDataTypes.hpp")
add_registry_entry(DataType FLOAT64 HEADER "BasicDataTypes.hpp")
add_registry_entry(DataType INT8 HEADER "BasicDataTypes.hpp")
add_registry_entry(DataType INT16 HEADER "BasicDataTypes.hpp")
add_registry_entry(DataType INT32 HEADER "BasicDataTypes.hpp")
add_registry_entry(DataType INT64 HEADER "BasicDataTypes.hpp")
add_registry_entry(DataType UINT8 HEADER "BasicDataTypes.hpp")
add_registry_entry(DataType UINT16 HEADER "BasicDataTypes.hpp")
add_registry_entry(DataType UINT32 HEADER "BasicDataTypes.hpp")
add_registry_entry(DataType UINT64 HEADER "BasicDataTypes.hpp")
add_registry_entry(DataType UNDEFINED HEADER "BasicDataTypes.hpp")
add_registry_entry(DataType VARSIZED HEADER "BasicDataTypes.hpp")
