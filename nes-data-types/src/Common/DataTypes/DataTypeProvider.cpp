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

#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

#include <magic_enum/magic_enum.hpp>

#include <DataTypeRegistry.hpp>
#include <Common/DataTypes/BasicTypes.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeProvider.hpp>

namespace NES::DataTypeProvider
{

bool isNullable(const std::string_view fieldName)
{
    return fieldName.size() > NULLABLE_POSTFIX.length()
        && fieldName.substr(fieldName.size() - NULLABLE_POSTFIX.length()) == NULLABLE_POSTFIX;
}

std::shared_ptr<DataType> provideDataType(const std::string& type, const bool nullable)
{
    auto args = DataTypeRegistryArguments{.nullable = nullable};
    if (auto dataType = DataTypeRegistry::instance().create(type, args))
    {
        std::shared_ptr<DataType> sharedType = std::move(dataType.value());
        return sharedType;
    }
    throw std::runtime_error("Failed to create data type of type: " + type);
}

std::shared_ptr<DataType> provideDataType(const LogicalType type, const bool nullable)
{
    return provideDataType(std::string(magic_enum::enum_name(type)), nullable);
}

std::shared_ptr<DataType> provideBasicType(const BasicType type, const bool nullable)
{
    return provideDataType(std::string(magic_enum::enum_name(type)), nullable);
}
}
