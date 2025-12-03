/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#pragma once

#include <string>
#include <rfl.hpp>

namespace NES
{
enum class SerializedDataType
{
    UINT8, UINT16, UINT32, UINT64, INT8, INT16, INT32, INT64, FLOAT32, FLOAT64, BOOLEAN, CHAR, UNDEFINED, VARSIZED
};

struct SerializedField
{
    std::string name;
    rfl::Box<SerializedDataType> type;
};

enum class SerializedMemoryLayout
{
    ROW_LAYOUT, COL_LAYOUT
};

struct SerializedSchema
{
    SerializedMemoryLayout memoryLayout;
    std::vector<SerializedField> fields;
};

struct SerializedFunction
{
    std::vector<SerializedFunction> children;
    std::string functionType;
    SerializedDataType dataType;
    std::map<std::string, rfl::Generic> config;
};
}