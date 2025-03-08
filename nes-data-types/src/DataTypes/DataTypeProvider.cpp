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
#include <utility>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <magic_enum/magic_enum.hpp>
#include <DataTypeRegistry.hpp>

namespace NES::DataTypeProvider
{

DataType provideDataType(const std::string& type)
{
    /// Empty argument struct, since we do not have data types that take arguments at the moment.
    /// However, we provide the empty struct to be consistent with the design of our registries.
    auto args = DataTypeRegistryArguments{};
    if (auto dataType = DataTypeRegistry::instance().create(type, args))
    {
        return dataType.value();
    }
    throw std::runtime_error("Failed to create data type of type: " + type);
}

DataType provideDataType(const PhysicalType::Type type)
{
    auto typeAsString = std::string(magic_enum::enum_name(type));
    return provideDataType(std::move(typeAsString));
}

}
