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
#include <Common/DataTypes/DataTypeProvider.hpp>
#include <Common/DataTypes/VariableSizedDataType.hpp>
#include <Common/DataTypes/Undefined.hpp>

namespace NES
{

bool VariableSizedDataType::operator==(const NES::DataType& other) const
{
    return dynamic_cast<const VariableSizedDataType*>(&other) != nullptr;
}

/// A VariableSizedData type cannot be joined with another type.
std::shared_ptr<DataType> VariableSizedDataType::join(const DataType&) const
{
    return DataTypeProvider::provideDataType(LogicalType::UNDEFINED);
}

std::string VariableSizedDataType::toString() const
{
    return "VARSIZED";
}

DataTypeRegistryReturnType DataTypeGeneratedRegistrar::RegisterVARSIZEDDataType(DataTypeRegistryArguments)
{
    return std::make_unique<VariableSizedDataType>();
}

}
