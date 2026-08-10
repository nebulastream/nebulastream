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

#include <string>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypeRegistry.hpp>

/// Time DataType. We represent it internally as microseconds since midnight.
namespace NES::DataTypeGeneratedRegistrar
{

DataTypeRegistryReturnType RegisterTimeDataType(DataTypeRegistryArguments args)
{
    const DataType microSecondsSinceMidnight = DataTypeProvider::provideDataType(DataType::Type::INT64, DataType::NULLABLE::NOT_NULLABLE);
    std::vector<std::pair<std::string, DataType>> fields;
    fields.emplace_back("microseconds_since_midnight", microSecondsSinceMidnight);
    return DataType{DataType::Type::STRUCT, args.nullable, std::string{"Time"}, std::move(fields)};
}
}