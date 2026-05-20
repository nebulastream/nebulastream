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

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string_view>
#include <utility>
#include <vector>

#include <simdjson.h>
#include <Nautilus/Interface/BufferRef/TupleBufferRef.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <InputFormatIndexer.hpp>
#include <InputParserUtil.hpp>
#include <RawBufferIndex.hpp>
#include <RawTupleBuffer.hpp>
#include <SIMDJSONInputFormatIndexer.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>
#include <val_concepts.hpp>
#include <val_ptr.hpp>

namespace NES
{
class SIMDJSONRawBufferIndex final : public RawBufferIndex
{
public:
    SIMDJSONRawBufferIndex();
    ~SIMDJSONRawBufferIndex() override = default;

    [[nodiscard]] nautilus::val<bool>
    hasNext(const nautilus::val<uint64_t>& tupleIdx, const nautilus::val<RawBufferIndex*>& rawBufferIndex) const override;

    [[nodiscard]] Record readSpanningRecord(
        const std::vector<Record::RecordFieldIdentifier>& projections,
        const nautilus::val<int8_t*>& /*recordBufferPtr*/,
        const nautilus::val<uint64_t>& /*recordIndex*/,
        const InputFormatIndexer& indexer,
        nautilus::val<RawBufferIndex*> rawBufferIndex,
        const TupleBufferRef& bufferRef) const override;

    [[nodiscard]] TupleDelimiterOffsets getTupleDelimiterOffsets() const override
    {
        return {.first = offsetOfFirstTuple, .last = offsetOfLastTuple};
    }

    /// SIMDJSON's tuple count is not known up-front; return 0 — the InputFormatter relies on hasNext() iteration instead.
    [[nodiscard]] size_t getNumberOfTuples() const override { return 0; }

    /// Resets the indexes and pointers, calculates and sets the number of tuples in the current buffer, returns the total number of tuples.
    void markNoTupleDelimiters();

    void markWithTupleDelimiters(FieldIndex offsetToFirstTuple, std::optional<FieldIndex> offsetToLastTuple);

    std::pair<bool, FieldIndex> indexJSON(std::string_view jsonSV);

    std::pair<bool, FieldIndex> indexJSON(std::string_view jsonSV, size_t batchSize);

    [[nodiscard]] simdjson::ondemand::document_stream::iterator getDocStreamIterator() const { return docStreamIterator; }

private:
    bool isAtLastTuple{false};
    FieldIndex offsetOfFirstTuple{};
    FieldIndex offsetOfLastTuple{};
    std::shared_ptr<simdjson::ondemand::parser> parser;
    std::shared_ptr<simdjson::ondemand::document_stream> docStream;
    simdjson::ondemand::document_stream::iterator docStreamIterator;
};

/// This is obtained after accessing the raw (unparsed) value of a specific field
/// Sets isNull to true if the access of the raw json value fails
struct RawJsonAccessResult
{
    const int8_t* ptrToRawJson;
    uint64_t sizeOfRawJson;
    bool isNull;
};

template <bool Nullable>
RawJsonAccessResult* getRawValueFromIndex(FieldIndex fieldIndex, RawBufferIndex* rawBufferIndex, const InputFormatIndexer* indexer);

inline simdjson::simdjson_result<simdjson::ondemand::value> accessSIMDJsonFieldOrThrow(
    simdjson::simdjson_result<simdjson::ondemand::document_reference>& simdJsonReference, const std::string_view fieldName)
{
    const auto simdJsonResult = simdJsonReference[fieldName];
    if (not simdJsonResult.has_value())
    {
        throw FieldNotFound(
            "SimdJson has not found the fieldName {} with error: {}", fieldName, magic_enum::enum_name(simdJsonResult.error()));
    }
    return simdJsonResult;
}

inline bool checkIsNullJsonProxy(
    const FieldIndex fieldIndex, const SIMDJSONRawBufferIndex* simdJsonRawBufferIndex, const SIMDJSONInputFormatIndexer* simdIndexer)
{
    const auto& fieldName = simdIndexer->getFieldNameInJsonAt(fieldIndex);
    auto currentDoc = *simdJsonRawBufferIndex->getDocStreamIterator();

    /// First, we check if the key is not in the doc. If this is the case, we can return true, as this counts as null
    if (not currentDoc[fieldName].has_value())
    {
        return true;
    }

    /// Second, we need to check if the key is equal to one of the null values
    if (accessSIMDJsonFieldOrThrow(currentDoc, fieldName).is_null())
    {
        return true;
    }
    return false;
}

/// (Proxy) functions being called via nautilus::invoke() can not be member functions. Thus, we need to implement them outside of the class
template <bool Nullable>
RawJsonAccessResult* getRawValueFromIndex(const FieldIndex fieldIndex, RawBufferIndex* rawBufferIndex, const InputFormatIndexer* indexer)
{
    PRECONDITION(dynamic_cast<SIMDJSONRawBufferIndex*>(rawBufferIndex) != nullptr, "rawBufferIndex must be a SIMDJSONRawBufferIndex");
    PRECONDITION(dynamic_cast<const SIMDJSONInputFormatIndexer*>(indexer) != nullptr, "indexer must be a SIMDJSONInputFormatIndexer");
    /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast): type verified by PRECONDITION above.
    auto* simdJsonRawBufferIndex = static_cast<SIMDJSONRawBufferIndex*>(rawBufferIndex);
    /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast): type verified by PRECONDITION above.
    const auto* simdIndexer = static_cast<const SIMDJSONInputFormatIndexer*>(indexer);
    PRECONDITION(
        fieldIndex < simdIndexer->getNumberOfFields(),
        "fieldIndex {} is out of bounds for schema keys of size: {}",
        fieldIndex,
        simdIndexer->getNumberOfFields());

    /// We use the thread local to return multiple values.
    /// C++ guarantees that the returned address is valid throughout the lifetime of this thread.
    thread_local static RawJsonAccessResult result;
    result.isNull = false;
    result.ptrToRawJson = nullptr;
    result.sizeOfRawJson = 0;

    /// Checking if the field is null but only if the field is nullable
    /// This null-check includes a check, if the key of the field exists in the object or if the value is set to NULL.
    if constexpr (Nullable)
    {
        if (checkIsNullJsonProxy(fieldIndex, simdJsonRawBufferIndex, simdIndexer))
        {
            result.isNull = true;
            return &result;
        }
    }

    const auto& fieldName = simdIndexer->getFieldNameInJsonAt(fieldIndex);
    auto currentDoc = *simdJsonRawBufferIndex->getDocStreamIterator();
    std::string_view rawValue = accessSIMDJsonFieldOrThrow(currentDoc, fieldName).raw_json().value();
    /// raw_json() may include trailing JSON whitespace (space, tab, newline, carriage return). We need to truncate it, otherwise the size of
    /// the value is not correct, which will corrupt varsized values and chars
    if (const auto lastNonWs = rawValue.find_last_not_of(" \t\n\r"); lastNonWs != std::string_view::npos)
    {
        rawValue = rawValue.substr(0, lastNonWs + 1);
    }
    /// The actual parse of the value will be handeled by the InputParser
    result.ptrToRawJson = reinterpret_cast<const int8_t*>(rawValue.data());
    result.sizeOfRawJson = rawValue.size();
    return &result;
}

inline void parseJsonIntoVarVal(
    const DataType dataType,
    const std::string& fieldName,
    const std::string& parserType,
    const std::vector<std::string>& nullValues,
    Record& record,
    const nautilus::val<FieldIndex>& fieldIndex,
    const nautilus::val<RawBufferIndex*>& rawBufferIndex,
    const nautilus::val<const InputFormatIndexer*>& indexer)
{
    const std::unique_ptr<InputParser> nullValueProvider = provideInputParser(parserType);
    if (dataType.nullable)
    {
        const auto parseResult
            = nautilus::invoke({nautilus::ModRefInfo::Ref}, getRawValueFromIndex<true>, fieldIndex, rawBufferIndex, indexer);
        const nautilus::val<bool> isNull = *getMemberWithOffset<bool>(parseResult, offsetof(RawJsonAccessResult, isNull));
        if (isNull)
        {
            /// During the access of the raw value, it was already determined that it was NULL. In this case, we do not need to parse and write a NULL-value.
            record.write(fieldName, nullValueProvider->writeNull());
        }
        else
        {
            /// Initiate a parse of the value. During the parse, the value might still be determined as malformed and therefore NULL.
            const nautilus::val<int8_t*> address = *getMemberWithOffset<int8_t*>(parseResult, offsetof(RawJsonAccessResult, ptrToRawJson));
            const nautilus::val<uint64_t> size = *getMemberWithOffset<uint64_t>(parseResult, offsetof(RawJsonAccessResult, sizeOfRawJson));
            parseRawValueIntoRecord(dataType, parserType, record, address, size, fieldName, nullValues);
        }
    }
    else
    {
        const auto parseResult
            = nautilus::invoke({nautilus::ModRefInfo::Ref}, getRawValueFromIndex<false>, fieldIndex, rawBufferIndex, indexer);
        /// Initiate parse
        const nautilus::val<int8_t*> address = *getMemberWithOffset<int8_t*>(parseResult, offsetof(RawJsonAccessResult, ptrToRawJson));
        const nautilus::val<uint64_t> size = *getMemberWithOffset<uint64_t>(parseResult, offsetof(RawJsonAccessResult, sizeOfRawJson));
        parseRawValueIntoRecord(dataType, parserType, record, address, size, fieldName, nullValues);
    }
}
}
