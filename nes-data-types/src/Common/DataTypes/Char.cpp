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
#include <string>
#include <Util/Common.hpp>
#include <magic_enum/magic_enum.hpp>
#include <DataTypeRegistry.hpp>
#include <Common/DataTypes/BasicTypes.hpp>
#include <Common/DataTypes/Char.hpp>
#include <Common/DataTypes/DataTypeProvider.hpp>

namespace NES
{

bool Char::operator==(const NES::DataType& other) const
{
    return dynamic_cast<const Char*>(&other) != nullptr;
}

std::shared_ptr<DataType> Char::join(const DataType& otherDataType) const
{
    if (dynamic_cast<const Char*>(&otherDataType) != nullptr)
    {
        return DataTypeProvider::provideDataType(LogicalType::CHAR);
    }
    return DataTypeProvider::provideDataType(LogicalType::UNDEFINED);
}

std::string Char::toString() const
{
    return std::string(magic_enum::enum_name(BasicType::CHAR));
}

DataTypeRegistryReturnType DataTypeGeneratedRegistrar::RegisterCHARDataType(DataTypeRegistryArguments)
{
    return std::make_unique<Char>();
}

}
