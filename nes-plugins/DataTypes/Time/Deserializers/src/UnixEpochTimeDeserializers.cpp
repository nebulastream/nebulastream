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

#include <UnixEpochTimeDeserializers.hpp>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypesUtil.hpp>
#include <DataTypes/StructData.hpp>
#include <DataTypes/VarVal.hpp>
#include <Arena.hpp>
#include <ErrorHandling.hpp>
#include <ValueDeserializer.hpp>
#include <ValueDeserializerRegistry.hpp>
#include <ValueDeserializerUtil.hpp>
#include <function.hpp>
#include <static.hpp>
#include <val_arith.hpp>
#include <val_ptr.hpp>
#include <val_std.hpp>

namespace NES
{

VarVal UnixEpochTimestampValueDeserializer::deserializeToVarVal(
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues,
    const std::unordered_map<DeserializerKey, std::string>& deserializerTypes,
    const DataType& valueType,
    ArenaRef& arena) const
{
    /// For now, we ignore the "quoted" parameter, as these values are textual integers and have no reason to be quoted
    /// Get the int64 deserializer
    const ValueDeserializerConfig config{.nullable = false, .quoted = true, .hasTrailingSpaces = true};
    const std::unique_ptr<ValueDeserializer> deserializer
        = provideDeserializerForType(valueType.fields[0].second, config, deserializerTypes);
    const nautilus::val<int8_t*> buffer = arena.allocateMemory(valueType.getSizeInBytesWithoutNull());
    deserializer->deserializeIntoBuffer(fieldAddress, fieldSize, nullValues, deserializerTypes, valueType.fields[0].second, arena, buffer);
    const StructData structData{buffer, valueType.fields};
    return VarVal{structData, false, false};
}

void UnixEpochTimestampValueDeserializer::deserializeIntoBuffer(
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues,
    const std::unordered_map<DeserializerKey, std::string>& deserializerTypes,
    const DataType& valueType,
    ArenaRef& arena,
    const nautilus::val<int8_t*>& bufferAddress) const
{
    const ValueDeserializerConfig config{.nullable = false, .quoted = true, .hasTrailingSpaces = true};
    const std::unique_ptr<ValueDeserializer> deserializer
        = provideDeserializerForType(valueType.fields[0].second, config, deserializerTypes);
    deserializer->deserializeIntoBuffer(
        fieldAddress, fieldSize, nullValues, deserializerTypes, valueType.fields[0].second, arena, bufferAddress);
}

ValueDeserializerRegistryReturnType
ValueDeserializerGeneratedRegistrar::RegisterUnixEpochTimestampValueDeserializer(ValueDeserializerRegistryArguments)
{
    return std::make_unique<UnixEpochTimestampValueDeserializer>(false, false);
}
}
