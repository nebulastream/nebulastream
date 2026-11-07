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

#include <Serializers/DefaultDecimalSerializer.hpp>

#include <cstdint>
#include <format>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>

#include <DataTypes/DataType.hpp>
#include <DataTypes/StructData.hpp>
#include <DataTypes/VarVal.hpp>
#include <Interface/RecordBuffer.hpp>
#include <OutputFormatters/OutputFormatterUtil.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <ValueSerializerRegistry.hpp>
#include <val_arith.hpp>
#include <val_base.hpp>
#include <val_ptr.hpp>

namespace NES
{
namespace DecimalSerializer
{
uint64_t serializeDecimal(
    const int64_t unscaledValue,
    const int32_t scale,
    const uint64_t remainingSpace,
    TupleBuffer* buffer,
    AbstractBufferProvider* bufferProvider,
    int8_t* bufferStartingAddress)
{
    /// Serialize the unscaled value to string
    std::string unscaledString = std::to_string(unscaledValue);
    if (scale == 0)
    {
        return writeValueToBuffer(
            unscaledString.data(), unscaledString.size(), remainingSpace, buffer, bufferProvider, bufferStartingAddress);
    }
    /// If scale >= unscaledString.size(), we have to prepend 0s
    unscaledString = std::format("{:0>{}}", unscaledString, scale + 1);
    /// We need to place a point to restore the original number
    const std::string_view unscaledStringView = unscaledString;
    const std::string_view integerView = unscaledStringView.substr(0, unscaledStringView.size() - scale);
    const std::string_view fractionalView = unscaledStringView.substr(unscaledStringView.size() - scale);
    const std::string textualDecimal = std::format("{}.{}", integerView, fractionalView);
    return writeValueToBuffer(textualDecimal.data(), textualDecimal.size(), remainingSpace, buffer, bufferProvider, bufferStartingAddress);
}
}

nautilus::val<uint64_t> DefaultDecimalValueSerializer::serializeAndWrite(
    const VarVal& value,
    const nautilus::val<uint64_t>& remainingSize,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    const nautilus::val<int8_t*>& startingAddress,
    const std::unordered_map<DataType::Type, std::string>&,
    const DataType&) const
{
    const auto decimalStruct = value.getRawValueAs<StructData>();
    const auto unscaledValue = decimalStruct.at("unscaled").getRawValueAs<nautilus::val<int64_t>>();
    const auto scale = decimalStruct.at("scale").getRawValueAs<nautilus::val<int32_t>>();
    return nautilus::invoke(
        DecimalSerializer::serializeDecimal,
        unscaledValue,
        scale,
        remainingSize,
        recordBuffer.getReference(),
        bufferProvider,
        startingAddress);
}

ValueSerializerRegistryReturnType DefaultDecimalValueSerializer::provideSerializer(ValueSerializerRegistryArguments)
{
    return std::make_unique<DefaultDecimalValueSerializer>();
}
}
