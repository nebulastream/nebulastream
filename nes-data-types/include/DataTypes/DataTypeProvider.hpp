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

#include <memory>
#include <string>
#include <DataTypes/DataType.hpp>

namespace NES::DataTypeProvider
{

/// @return a shared pointer to a logical data type
/// @param type name of the logical data type
/// @brief Currently supported are the types BOOLEAN, CHAR, FLOAT32, FLOAT64, INT8, INT16, INT32, INT64, UINT8, UINT16, UINT32, UINT64, VARSIZED and UNDEFINED.
/// Throws a runtime error, if the name does not match any of the mentioned data types.
DataType provideDataType(const std::string& type);

DataType provideDataType(PhysicalType::Type type);
}
