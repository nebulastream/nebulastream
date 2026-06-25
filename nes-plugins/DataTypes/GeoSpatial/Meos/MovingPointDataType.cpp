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

namespace NES::DataTypeGeneratedRegistrar
{

/// A moving point is a point with a timestamp. Currently, we support 2-dimensional points consisting of a longitude, latitude and a timestamp.
/// We currently map them to the TGEOMETRY temptype in meos.
DataTypeRegistryReturnType RegisterMovingPointDataType(DataTypeRegistryArguments args)
{
    std::vector<std::pair<std::string, DataType>> fields;
    fields.emplace_back("lon", DataTypeProvider::provideDataType(DataType::Type::FLOAT64, DataType::NULLABLE::NOT_NULLABLE));
    fields.emplace_back("lat", DataTypeProvider::provideDataType(DataType::Type::FLOAT64, DataType::NULLABLE::NOT_NULLABLE));
    fields.emplace_back("ts", DataTypeProvider::provideDataType(DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE));
    return DataType{DataType::Type::STRUCT, args.nullable, std::string{"MovingPoint"}, std::move(fields)};
}
}