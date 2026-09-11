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

#include <UnixEpochTimeSerializers.hpp>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <simdjson.h>
#include <DataTypes/DataType.hpp>
#include <DataTypes/StructData.hpp>
#include <DataTypes/VarVal.hpp>
#include <Interface/RecordBuffer.hpp>
#include <OutputFormatters/OutputFormatterUtil.hpp>
#include <OutputFormatters/ValueSerializer.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <ValueSerializerRegistry.hpp>
#include <function.hpp>
#include <select.hpp>
#include <static.hpp>
#include <val_arith.hpp>
#include <val_ptr.hpp>

namespace NES
{
nautilus::val<uint64_t> UnixEpochTimestampValueSerializer::serializeAndWrite(
    const VarVal& value,
    const nautilus::val<uint64_t>& remainingSize,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    const nautilus::val<int8_t*>& startingAddress,
    const std::unordered_map<SerializerKey, std::string>& serializerTypes,
    const DataType& valueType) const
{
    const StructData castedVal = value.getRawValueAs<StructData>();
    const VarVal microSecondsSinceUnixEpoch = castedVal.at(0);
    /// Fetch the INT64 serializer
    const ValueSerializerConfig config{.quoted = false};
    const std::unique_ptr<ValueSerializer> intSerializer = provideSerializerForType(valueType.fields[0].second, config, serializerTypes);
    return intSerializer->serializeAndWrite(
        microSecondsSinceUnixEpoch,
        remainingSize,
        recordBuffer,
        bufferProvider,
        startingAddress,
        serializerTypes,
        valueType.fields[0].second);
}

ValueSerializerRegistryReturnType
ValueSerializerGeneratedRegistrar::RegisterUnixEpochTimestampValueSerializer(ValueSerializerRegistryArguments)
{
    return std::make_unique<UnixEpochTimestampValueSerializer>(false);
}

/// The serializer merely delegates to the serializer of the underlying integer field, so the same implementation serves the unsigned
/// representation of timestamps, where that field is an UINT64 instead of an INT64.
ValueSerializerRegistryReturnType
ValueSerializerGeneratedRegistrar::RegisterUnixEpochUnsignedTimestampValueSerializer(ValueSerializerRegistryArguments)
{
    return std::make_unique<UnixEpochTimestampValueSerializer>(false);
}
}
