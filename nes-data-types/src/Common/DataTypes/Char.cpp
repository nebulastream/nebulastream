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
#include <Util/Common.hpp>
#include <DataTypeRegistry.hpp>
#include <magic_enum.hpp>
#include <Common/DataTypes/BasicTypes.hpp>
#include <Common/DataTypes/Char.hpp>
#include <Common/DataTypes/DataTypeProvider.hpp>

namespace NES
{
Char::Char(const bool nullable) : DataType(nullable)
{
}

bool Char::operator==(const NES::DataType& other) const
{
    return dynamic_cast<const Char*>(&other) != nullptr && nullable == other.nullable;
}

std::shared_ptr<DataType> Char::join(const std::shared_ptr<DataType> otherDataType)
{
    const auto resultIsNullable = nullable || otherDataType->nullable;
    if (NES::Util::instanceOf<Char>(otherDataType))
    {
        return DataTypeProvider::provideDataType(LogicalType::CHAR, resultIsNullable);
    }
    return DataTypeProvider::provideDataType(LogicalType::UNDEFINED, resultIsNullable);
}

std::string Char::toString()
{
    return std::string(magic_enum::enum_name(BasicType::CHAR));
}

std::unique_ptr<DataTypeRegistryReturnType> DataTypeGeneratedRegistrar::RegisterCHARDataType(DataTypeRegistryArguments args)
{
    return std::make_unique<Char>(args.nullable);
}

}
