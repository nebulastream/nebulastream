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

/// Standalone plugin for the `Reading` STRUCT — a tiny composite carrying two
/// scalar measurements (`temperature`, `humidity`). Constructed in SQL via the
/// type-constructor syntax `Reading(temperature, humidity)`. Lives in its own
/// plugin so domain-specific Reading operations (`Add_Reading_Reading`,
/// `Greater_Reading_Reading`, ...) can be added without touching the Image /
/// thermal-camera plugin family.
namespace NES::DataTypeGeneratedRegistrar
{

DataTypeRegistryReturnType RegisterReadingDataType(DataTypeRegistryArguments args)
{
    std::vector<std::pair<std::string, DataType>> fields;
    fields.emplace_back("temperature", DataTypeProvider::provideDataType(DataType::Type::FLOAT64, DataType::NULLABLE::NOT_NULLABLE));
    fields.emplace_back("humidity", DataTypeProvider::provideDataType(DataType::Type::FLOAT64, DataType::NULLABLE::NOT_NULLABLE));
    return DataType{DataType::Type::STRUCT, args.nullable, std::string{"Reading"}, std::move(fields)};
}

}
