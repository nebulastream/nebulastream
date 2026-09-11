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

/// UnsignedTimestamp DataType. We represent it internally as microseconds since unix epoch in an unsigned integer, meaning that points in
/// time before the unix epoch are not representable. This is the counterpart of the Timestamp DataType, which uses a signed representation.
/// It exists, because the timestamps our windowing logic operates on are unsigned as well. As soon as the entire window and watermark logic was rewired to signed,
/// we can remove this plugin.
namespace NES::DataTypeGeneratedRegistrar
{

DataTypeRegistryReturnType RegisterUnsignedTimestampDataType(DataTypeRegistryArguments args)
{
    const DataType microSecondsSinceUEpoch = DataTypeProvider::provideDataType(DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE);
    std::vector<std::pair<std::string, DataType>> fields;
    fields.emplace_back("microseconds_since_unix_epoch", microSecondsSinceUEpoch);
    return DataType{DataType::Type::STRUCT, args.nullable, std::string{"UnsignedTimestamp"}, std::move(fields)};
}
}
