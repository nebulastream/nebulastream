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
#include <TemporalPointSequenceDataType.hpp>

#include <string>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypeRegistry.hpp>

namespace NES
{

/// A data type to describe a sequence of temporal points, also known as a trajectory. This type allows to display a temporal sequence of points in one tuple,
/// instead of the points being distributed over multiple tuples. The type can be used as aggregation result or if the source transmits whole trajectories already.
/// Currently, we treat the trajectories as discrete. This means that in functions, we will not interpolate the trajectory as a function to find points for a timestamp that was not explicitly provided.
DataTypeRegistryReturnType provideTemporalPointSequenceDataType(DataTypeRegistryArguments args)
{
    std::vector<std::pair<std::string, DataType>> fields;
    const DataType tSequence{DataType::Type::VECTOR, DataType::NULLABLE::NOT_NULLABLE, DataTypeProvider::provideDataType("TemporalPoint")};
    fields.emplace_back("sequence", tSequence);
    return DataType{DataType::Type::STRUCT, args.nullable, std::string{"TemporalPointSequence"}, std::move(fields)};
}
}
