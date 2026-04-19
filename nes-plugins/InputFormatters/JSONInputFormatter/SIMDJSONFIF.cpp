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
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <Nautilus/Interface/Record.hpp>

#include <Arena.hpp>
#include <ErrorHandling.hpp>
#include <function.hpp>
#include <val.hpp>
#include <val_ptr.hpp>

namespace NES
{
[[nodiscard]] nautilus::val<bool>
SIMDJSONFIF::applyHasNext(const nautilus::val<uint64_t>&, const nautilus::val<SIMDJSONFIF*>& fieldIndexFunction)
{
    const nautilus::val<bool> isAtLastTuple = *getMemberWithOffset<bool>(fieldIndexFunction, offsetof(SIMDJSONFIF, isAtLastTuple));
    return not isAtLastTuple;
}

struct VarSizedResult
{
    const char* varSizedPointer;
    uint64_t size{};
};

VariableSizedData SIMDJSONFIF::parseStringIntoNautilusRecord(
    const nautilus::val<FieldIndex>& fieldIdx,
    const nautilus::val<SIMDJSONFIF*>& fieldIndexFunction,
    const nautilus::val<SIMDJSONMetaData*>& metaData)
{
    const nautilus::val<VarSizedResult*> varSizedResult = nautilus::invoke(
        +[](FieldIndex fieldIndex, SIMDJSONFIF* fieldIndexFunction, SIMDJSONMetaData* metaData)
        {
            thread_local auto result = VarSizedResult{};
            INVARIANT(
                fieldIndex < metaData->getNumberOfFields(),
                "fieldIndex {} is out or bounds for schema keys of size: {}",
                fieldIndex,
                metaData->getNumberOfFields());
            auto currentDoc = *fieldIndexFunction->docStreamIterator;
            const auto& fieldName = metaData->getFieldNameInJsonAt(fieldIndex);

            /// Get the value from the document and convert it to a span of bytes
            const std::string_view value = accessSIMDJsonFieldOrThrow(currentDoc, fieldName);

            /// Get memory from arena that holds length of the string and the bytes of the string
            result = VarSizedResult{.varSizedPointer = value.data(), .size = value.size()};
            return &result;
        },
        fieldIdx,
        fieldIndexFunction,
        metaData);

    VariableSizedData varSizedString{
        *getMemberWithOffset<int8_t*>(varSizedResult, offsetof(VarSizedResult, varSizedPointer)),
        *getMemberWithOffset<uint64_t>(varSizedResult, offsetof(VarSizedResult, size))};
    return varSizedString;
}

std::pair<nautilus::val<int8_t*>, nautilus::val<uint64_t>> SIMDJSONFIF::extractRawFieldBytes(
    const nautilus::val<FieldIndex>& fieldIdx,
    const nautilus::val<SIMDJSONFIF*>& fieldIndexFunction,
    const nautilus::val<const SIMDJSONMetaData*>& metaData)
{
    const nautilus::val<VarSizedResult*> varSizedResult = nautilus::invoke(
        +[](FieldIndex fieldIndex, SIMDJSONFIF* fieldIndexFunction, const SIMDJSONMetaData* metaData)
        {
            thread_local auto result = VarSizedResult{};
            INVARIANT(
                fieldIndex < metaData->getNumberOfFields(),
                "fieldIndex {} is out or bounds for schema keys of size: {}",
                fieldIndex,
                metaData->getNumberOfFields());
            auto currentDoc = *fieldIndexFunction->docStreamIterator;
            const auto& fieldName = metaData->getFieldNameInJsonAt(fieldIndex);

            auto simdJsonResult = accessSIMDJsonFieldOrThrow(currentDoc, fieldName);
            /// raw_json() consumes the value and returns its exact raw text from the input buffer.
            /// raw_json_token() does not consume the value, so its reported length can extend past the value
            /// (it extends up to the start of the next token, which may include trailing whitespace).
            auto rawJsonResult = simdJsonResult.raw_json();
            if (not rawJsonResult.has_value())
            {
                throw FormattingError(
                    "SimdJson could not extract raw json for field '{}' with error: {}",
                    fieldName,
                    magic_enum::enum_name(rawJsonResult.error()));
            }
            std::string_view rawToken = rawJsonResult.value();
            /// raw_json() may include surrounding whitespace; trim both ends.
            while (not rawToken.empty()
                   && (rawToken.front() == ' ' || rawToken.front() == '\t' || rawToken.front() == '\n' || rawToken.front() == '\r'))
            {
                rawToken.remove_prefix(1);
            }
            while (not rawToken.empty()
                   && (rawToken.back() == ' ' || rawToken.back() == '\t' || rawToken.back() == '\n' || rawToken.back() == '\r'))
            {
                rawToken.remove_suffix(1);
            }
            result = VarSizedResult{.varSizedPointer = rawToken.data(), .size = rawToken.size()};
            return &result;
        },
        fieldIdx,
        fieldIndexFunction,
        metaData);

    auto fieldAddress = *getMemberWithOffset<int8_t*>(varSizedResult, offsetof(VarSizedResult, varSizedPointer));
    auto fieldSize = *getMemberWithOffset<uint64_t>(varSizedResult, offsetof(VarSizedResult, size));
    return {fieldAddress, fieldSize};
}

void SIMDJSONFIF::writeValueToRecord(
    const DataType::Type physicalType,
    Record& record,
    const std::string& fieldName,
    const nautilus::val<FieldIndex>& fieldIdx,
    const nautilus::val<SIMDJSONFIF*>& fieldIndexFunction,
    const nautilus::val<const SIMDJSONMetaData*>& metaData) const
{
    /// VARSIZED is eager zero-copy (mirrors parseLazyValueIntoRecord in RawValueParser).
    if (physicalType == DataType::Type::VARSIZED)
    {
        record.write(fieldName, parseStringIntoNautilusRecord(fieldIdx, fieldIndexFunction, metaData));
        return;
    }
    if (physicalType == DataType::Type::UNDEFINED)
    {
        throw NotImplemented("Cannot parse undefined type.");
    }

    /// For all scalar types: eagerly pull the raw JSON token bytes out of the current simdjson document
    /// (safe because the iterator still points at this record and the underlying JSON buffer is pinned),
    /// then defer the from_chars conversion via parseLazyValueIntoRecord. Fields that are never read by
    /// downstream operators skip the conversion entirely.
    /// raw_json_token returns the JSON token verbatim: `"foo"` for strings (incl. CHAR), bare `123`/`true`
    /// for numbers/bools. That maps directly onto parseLazyValueIntoRecord's QuotationType contract.
    const auto [fieldAddress, fieldSize] = extractRawFieldBytes(fieldIdx, fieldIndexFunction, metaData);
    const QuotationType quotationType
        = (physicalType == DataType::Type::CHAR) ? QuotationType::DOUBLE_QUOTE : QuotationType::NONE;
    parseLazyValueIntoRecord(physicalType, record, fieldAddress, fieldSize, fieldName, quotationType);
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
