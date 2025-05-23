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
#include <Common/DataTypes/Undefined.hpp>

namespace NES
{

bool Undefined::operator==(const DataType& other) const
{
    return dynamic_cast<const Undefined*>(&other) != nullptr;
}

std::shared_ptr<DataType> Undefined::join(const DataType&) const
{
    return DataTypeProvider::provideDataType(LogicalType::UNDEFINED);
}
std::string Undefined::toString() const
{
    return "Undefined";
}

DataTypeRegistryReturnType DataTypeGeneratedRegistrar::RegisterUNDEFINEDDataType(DataTypeRegistryArguments)
{
    return std::make_unique<Undefined>();
}


}
