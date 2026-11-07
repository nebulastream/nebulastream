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

#include <Deserializers/DefaultDecimalDeserializer.hpp>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <format>
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
#include <Util/Strings.hpp>
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
namespace DecimalValueDeserializer
{
struct DeserializedDecimal
{
    int64_t unscaledValue;
    int32_t scale;
};

void deserializeDecimal(const int8_t* ptr, const uint64_t size, DeserializedDecimal* result)
{
    const std::string_view valueView(reinterpret_cast<const char*>(ptr), size);
    /// Locate the . in the value. If there is no point, the scale is 0
    const size_t pointLoc = valueView.find('.', 0);
    if (pointLoc == std::string_view::npos)
    {
        result->unscaledValue = NES::from_chars_with_exception<int64_t>(valueView);
        result->scale = 0;
    }
    else
    {
        /// Create a combined string of the content before and after the point
        const std::string_view integerView = valueView.substr(0, pointLoc);
        const std::string_view fractionView = valueView.substr(pointLoc + 1);
        const std::string combined = std::format("{}{}", integerView, fractionView);
        result->unscaledValue = NES::from_chars_with_exception<int64_t>(combined);
        result->scale = valueView.size() - pointLoc - 1;
    }
}
}

VarVal DefaultDecimalValueDeserializer::deserializeToVarVal(
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>&,
    const ArenaRef& arena,
    const std::unordered_map<DataType::Type, std::string>&,
    const DataType& valueType) const
{
    nautilus::val<DecimalValueDeserializer::DeserializedDecimal> deserializedResult;
    nautilus::invoke(DecimalValueDeserializer::deserializeDecimal, fieldAddress, fieldSize, &deserializedResult);
    /// Create a buffer for the value
    const nautilus::val<int8_t*> buffer = arena.allocateMemory(valueType.getSizeInBytesWithoutNull());
    /// Fetch deserialiazation results
    const nautilus::val<int64_t> unscaledValue = deserializedResult.get(&DecimalValueDeserializer::DeserializedDecimal::unscaledValue);
    const nautilus::val<int32_t> scale = deserializedResult.get(&DecimalValueDeserializer::DeserializedDecimal::scale);
    const StructData structData{buffer, valueType.fields};
    /// Write deserialized field values into struct
    structData.writeAt(0, VarVal{unscaledValue, false, false});
    structData.writeAt(1, VarVal{scale, false, false});
    return VarVal{structData, false, false};
}

void DefaultDecimalValueDeserializer::deserializeIntoBuffer(
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>&,
    const ArenaRef&,
    const std::unordered_map<DataType::Type, std::string>&,
    const DataType& valueType,
    const nautilus::val<int8_t*>& bufferAddress) const
{
    nautilus::val<DecimalValueDeserializer::DeserializedDecimal> deserializedResult;
    nautilus::invoke(DecimalValueDeserializer::deserializeDecimal, fieldAddress, fieldSize, &deserializedResult);
    /// Fetch deserialiazation results
    const nautilus::val<int64_t> unscaledValue = deserializedResult.get(&DecimalValueDeserializer::DeserializedDecimal::unscaledValue);
    const nautilus::val<int32_t> scale = deserializedResult.get(&DecimalValueDeserializer::DeserializedDecimal::scale);
    const StructData structData{bufferAddress, valueType.fields};
    /// Write deserialized field values into struct
    structData.writeAt(0, VarVal{unscaledValue, false, false});
    structData.writeAt(1, VarVal{scale, false, false});
}

ValueDeserializerRegistryReturnType DefaultDecimalValueDeserializer::provideDeserializer(ValueDeserializerRegistryArguments)
{
    return std::make_unique<DefaultDecimalValueDeserializer>();
}
}
