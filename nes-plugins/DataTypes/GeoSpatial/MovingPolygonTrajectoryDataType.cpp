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

/// DataType for moving polygons trajectory consisting of 3 moving polygons. The purpose of this type is to check if fixedsized can hold structs
namespace NES::DataTypeGeneratedRegistrar
{

DataTypeRegistryReturnType RegisterMovingPolygonTrajectoryDataType(DataTypeRegistryArguments args)
{
    const DataType trajectory{DataType::Type::FIXEDSIZED, DataType::NULLABLE::NOT_NULLABLE, DataTypeProvider::provideDataType("MovingPolygon"), 3};
    std::vector<std::pair<std::string, DataType>> fields;
    fields.emplace_back("polygons", trajectory);
    return DataType{DataType::Type::STRUCT, args.nullable, std::string{"MovingPolygonTrajectory"}, std::move(fields)};
}
}
