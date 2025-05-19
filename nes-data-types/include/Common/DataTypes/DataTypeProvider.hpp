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

#include <cstdint>
#include <memory>
#include <optional>
#include <string>

#include <Common/DataTypes/BasicTypes.hpp>
#include <Common/DataTypes/DataType.hpp>

namespace NES
{
/// Enum containing our logical data types
enum class LogicalType : uint8_t
{
    INT8,
    UINT8,
    INT16,
    UINT16,
    INT32,
    UINT32,
    INT64,
    FLOAT32,
    UINT64,
    FLOAT64,
    BOOLEAN,
    CHAR,
    VARSIZED,
    UNDEFINED,
};


}

namespace NES::DataTypeProvider
{
constexpr static std::string NULLABLE_POSTFIX = "_NULLABLE";

/// Checks if the field corresponding to the given name is nullable.
/// For now, we assume that a field is nullable if the name ends with NULLABLE.
bool isNullable(std::string_view fieldName);


std::optional<std::shared_ptr<DataType>> tryProvideDataType(const std::string& type, bool nullable = false);

/// @return a shared pointer to a logical data type
/// @param type name of the logical data type
/// @param nullable whether the data type is nullable or not
/// @brief Currently supported are the types BOOLEAN, CHAR, FLOAT32, FLOAT64, INT8, INT16, INT32, INT64, UINT8, UINT16, UINT32, UINT64, VARSIZED and UNDEFINED.
/// Throws a runtime error, if the name does not match any of the mentioned data types.
std::shared_ptr<DataType> provideDataType(const std::string& type, bool nullable = false);

/// @return a shared pointer to a basic logical data type
/// @param type object of the BasicType enum class whose corresponding data type should be provided
/// @param nullable whether the data type is nullable or not
std::shared_ptr<DataType> provideBasicType(BasicType type, bool nullable = false);
std::shared_ptr<DataType> provideDataType(LogicalType type, bool nullable = false);
}
