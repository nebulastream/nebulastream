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

#include <SIMDJSONFIF.hpp>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <optional>
#include <ranges>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include <simdjson.h>

#include <DataTypes/DataType.hpp>
#include <Nautilus/DataTypes/DataTypesUtil.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <Nautilus/Interface/Record.hpp>

#include <Arena.hpp>
#include <ErrorHandling.hpp>
#include <RawTupleBuffer.hpp>
#include <function.hpp>
#include <val.hpp>
#include <val_ptr.hpp>
#include <common/FunctionAttributes.hpp>

namespace NES
{
[[nodiscard]] nautilus::val<bool>
SIMDJSONFIF::applyHasNext(const nautilus::val<uint64_t>&, const nautilus::val<SIMDJSONFIF*>& fieldIndexFunction)
{
    const nautilus::val<bool> isAtLastTuple = *getMemberWithOffset<bool>(fieldIndexFunction, offsetof(SIMDJSONFIF, isAtLastTuple));
    return not isAtLastTuple;
}

VarVal SIMDJSONFIF::parseJsonVarSized(
    const nautilus::val<FieldIndex>& fieldIndex,
    const nautilus::val<SIMDJSONFIF*>& fieldIndexFunction,
    const nautilus::val<SIMDJSONMetaData*>& metaData,
    const bool nullable)
{
    if (nullable)
    {
        const auto varSizedResult = nautilus::invoke(
            {.modRefInfo = nautilus::ModRefInfo::Ref}, parseJsonVarSizedProxy<true>, fieldIndex, fieldIndexFunction, metaData);
        const VariableSizedData varSizedString{
            *getMemberWithOffset<int8_t*>(varSizedResult, offsetof(ParsedResultVariableSized, varSizedPointer)),
            *getMemberWithOffset<uint64_t>(varSizedResult, offsetof(ParsedResultVariableSized, size))};
        const nautilus::val<bool> isNull = *getMemberWithOffset<bool>(varSizedResult, offsetof(ParsedResultVariableSized, isNull));
        return VarVal{VariableSizedData{varSizedString}, nullable, isNull};
    }

    const auto varSizedResult = nautilus::invoke(
        {.modRefInfo = nautilus::ModRefInfo::Ref}, parseJsonVarSizedProxy<false>, fieldIndex, fieldIndexFunction, metaData);
    const VariableSizedData varSizedString{
        *getMemberWithOffset<int8_t*>(varSizedResult, offsetof(ParsedResultVariableSized, varSizedPointer)),
        *getMemberWithOffset<uint64_t>(varSizedResult, offsetof(ParsedResultVariableSized, size))};
    return VarVal{VariableSizedData{varSizedString}, nullable, false};
}

bool checkIsNullJsonProxy(FieldIndex fieldIndex, const SIMDJSONFIF* fieldIndexFunction, const SIMDJSONMetaData* metaData) noexcept
{
    const auto& fieldName = metaData->getFieldNameInJsonAt(fieldIndex);
    auto currentDoc = *fieldIndexFunction->getDocStreamIterator();

    /// First, we check if the key is not in the doc. If this is the case, we can return true, as this counts as null
    if (not currentDoc[fieldName].has_value())
    {
        return true;
    }

    /// Second, we need to check if the key is equal to one of the null values
    if (SIMDJSONFIF::accessSIMDJsonFieldOrThrow(currentDoc, fieldName).is_null())
    {
        return true;
    }
    return false;
}

void SIMDJSONFIF::writeValueToRecord(
    const DataType dataType,
    Record& record,
    const std::string& fieldName,
    const nautilus::val<FieldIndex>& fieldIndex,
    const nautilus::val<SIMDJSONFIF*>& fieldIndexFunction,
    const nautilus::val<const SIMDJSONMetaData*>& metaData) const
{
    switch (dataType.type)
    {
        case DataType::Type::INT8: {
            record.write(fieldName, parseJsonFixedSizeIntoVarVal<int8_t>(dataType.nullable, fieldIndex, fieldIndexFunction, metaData));
            return;
        }
        case DataType::Type::INT16: {
            record.write(fieldName, parseJsonFixedSizeIntoVarVal<int16_t>(dataType.nullable, fieldIndex, fieldIndexFunction, metaData));
            return;
        }
        case DataType::Type::INT32: {
            record.write(fieldName, parseJsonFixedSizeIntoVarVal<int32_t>(dataType.nullable, fieldIndex, fieldIndexFunction, metaData));
            return;
        }
        case DataType::Type::INT64: {
            record.write(fieldName, parseJsonFixedSizeIntoVarVal<int64_t>(dataType.nullable, fieldIndex, fieldIndexFunction, metaData));
            return;
        }
        case DataType::Type::UINT8: {
            record.write(fieldName, parseJsonFixedSizeIntoVarVal<uint8_t>(dataType.nullable, fieldIndex, fieldIndexFunction, metaData));
            return;
        }
        case DataType::Type::UINT16: {
            record.write(fieldName, parseJsonFixedSizeIntoVarVal<uint16_t>(dataType.nullable, fieldIndex, fieldIndexFunction, metaData));
            return;
        }
        case DataType::Type::UINT32: {
            record.write(fieldName, parseJsonFixedSizeIntoVarVal<uint32_t>(dataType.nullable, fieldIndex, fieldIndexFunction, metaData));
            return;
        }
        case DataType::Type::UINT64: {
            record.write(fieldName, parseJsonFixedSizeIntoVarVal<uint64_t>(dataType.nullable, fieldIndex, fieldIndexFunction, metaData));
            return;
        }
        case DataType::Type::FLOAT32: {
            record.write(fieldName, parseJsonFixedSizeIntoVarVal<float>(dataType.nullable, fieldIndex, fieldIndexFunction, metaData));
            return;
        }
        case DataType::Type::FLOAT64: {
            record.write(fieldName, parseJsonFixedSizeIntoVarVal<double>(dataType.nullable, fieldIndex, fieldIndexFunction, metaData));
            return;
        }
        case DataType::Type::CHAR: {
            record.write(fieldName, parseJsonFixedSizeIntoVarVal<char>(dataType.nullable, fieldIndex, fieldIndexFunction, metaData));
            return;
        }
        case DataType::Type::BOOLEAN: {
            record.write(fieldName, parseJsonFixedSizeIntoVarVal<bool>(dataType.nullable, fieldIndex, fieldIndexFunction, metaData));
            return;
        }
        case DataType::Type::VARSIZED: {
            record.write(fieldName, parseJsonVarSized(fieldIndex, fieldIndexFunction, metaData, dataType.nullable));
            return;
        }
        case DataType::Type::UNDEFINED:
            throw NotImplemented("Cannot parse undefined type.");
    }
    std::unreachable();
}

/// Resets the indexes and pointers, calculates and sets the number of tuples in the current buffer, returns the total number of tuples.
void SIMDJSONFIF::markNoTupleDelimiters()
{
    this->offsetOfFirstTuple = std::numeric_limits<FieldIndex>::max();
    this->offsetOfLastTuple = std::numeric_limits<FieldIndex>::max();
}

void SIMDJSONFIF::markWithTupleDelimiters(const FieldIndex offsetToFirstTuple, const std::optional<FieldIndex> offsetToLastTuple)
{
    this->offsetOfFirstTuple = offsetToFirstTuple;
    this->offsetOfLastTuple = offsetToLastTuple.value_or(std::numeric_limits<FieldIndex>::max());
}

std::pair<bool, FieldIndex> SIMDJSONFIF::indexJSON(const std::string_view jsonSV)
{
    return indexJSON(jsonSV, simdjson::ondemand::DEFAULT_BATCH_SIZE);
}

std::pair<bool, FieldIndex> SIMDJSONFIF::indexJSON(const std::string_view jsonSV, size_t batchSize)
{
    const simdjson::padded_string_view paddedJSONSV{jsonSV.data(), jsonSV.size(), jsonSV.size() + simdjson::SIMDJSON_PADDING};
    this->parser = std::make_shared<simdjson::ondemand::parser>();
    this->parser->threaded = false;
    if (jsonSV.size() > batchSize)
    {
        throw CannotFormatSourceData("Size of raw buffer: {} exceeds SIMDJSONs configured batch_size: {}", jsonSV.size(), batchSize);
    }
    docStream = std::make_shared<simdjson::ondemand::document_stream>(parser->iterate_many(paddedJSONSV, batchSize));
    docStreamIterator = docStream->begin();
    isAtLastTuple = docStreamIterator == docStream->end();
    return {docStreamIterator.at_end(), docStream->truncated_bytes()};
}
}
