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

#include <SIMDJSONRawBufferIndex.hpp>

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
#include <DataTypes/DataTypesUtil.hpp>
#include <DataTypes/VarVal.hpp>
#include <DataTypes/VariableSizedData.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <Interface/BufferRef/TupleBufferRef.hpp>
#include <Interface/Record.hpp>
#include <magic_enum/magic_enum.hpp>
#include <Arena.hpp>
#include <ErrorHandling.hpp>
#include <InputFormatIndexer.hpp>
#include <RawBufferIndex.hpp>
#include <RawTupleBuffer.hpp>
#include <function.hpp>
#include <static.hpp>
#include <val.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>
#include <val_ptr.hpp>
#include <val_std.hpp>

#include <SIMDJSONInputFormatIndexer.hpp>
#include <ValueDeserializer.hpp>
#include <ValueDeserializerUtil.hpp>

namespace NES
{

/// This is obtained after accessing the raw (unparsed) value of a specific field
/// If the access of the value fails, ptrToRawJSON is set to nullptr and sizeOfRawJson is set to 0
struct RawJsonAccessResult
{
    const int8_t* ptrToRawJson;
    uint64_t sizeOfRawJson;
};

/// Navigates to the field's value via simdjson's JSON Pointer API (the pointer is precomputed in
/// SIMDJSONInputFormatIndexer).
/// For nullable fields, a missing field, a missing parent object, or an explicit JSON null all map to
/// NULL (nullopt). For non-nullable fields, a missing field/parent throws FieldNotFound.
template <bool Nullable>
std::optional<simdjson::simdjson_result<simdjson::ondemand::value>>
navigateToField(simdjson::simdjson_result<simdjson::ondemand::document_reference>& doc, const std::string_view jsonPointer)
{
    auto simdJsonResult = doc.at_pointer(jsonPointer);
    if constexpr (Nullable)
    {
        if (not simdJsonResult.has_value() or simdJsonResult.is_null())
        {
            return std::nullopt;
        }
    }
    else if (not simdJsonResult.has_value())
    {
        throw FieldNotFound(
            "SimdJson has not found the fieldName {} with error: {}", jsonPointer, magic_enum::enum_name(simdJsonResult.error()));
    }
    return simdJsonResult;
}

/// (Proxy) functions being called via nautilus::invoke() can not be member functions. Thus, we need to implement them outside of the class
template <bool Nullable>
void getRawValueFromIndex(
    const FieldIndex fieldIndex, RawBufferIndex* rawBufferIndex, const InputFormatIndexer* indexer, RawJsonAccessResult* result)
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

    result->ptrToRawJson = nullptr;
    result->sizeOfRawJson = 0;

    const auto jsonPointer = simdIndexer->getJsonPointerAt(fieldIndex);
    auto currentDoc = *simdJsonRawBufferIndex->getDocStreamIterator();
    auto navigated = navigateToField<Nullable>(currentDoc, jsonPointer);

    if (navigated.has_value())
    {
        const std::string_view rawValue = (*navigated).raw_json().value();
        /// The actual parse of the value will be handeled by the ValueDeserializer
        result->ptrToRawJson = reinterpret_cast<const int8_t*>(rawValue.data());
        result->sizeOfRawJson = rawValue.size();
    }
}

SIMDJSONRawBufferIndex::SIMDJSONRawBufferIndex()
{
    INVARIANT(
        static_cast<void*>(this) == static_cast<void*>(static_cast<RawBufferIndex*>(this)),
        "RawBufferIndex base subobject must lay out at offset 0 in SIMDJSONRawBufferIndex");
}

[[nodiscard]] nautilus::val<bool>
SIMDJSONRawBufferIndex::hasNext(const nautilus::val<uint64_t>&, const nautilus::val<RawBufferIndex*>& rawBufferIndex) const
{
    const nautilus::val<bool> lastTuple = readValueFromMemRef<bool>(getMemberRef(rawBufferIndex, &SIMDJSONRawBufferIndex::isAtLastTuple));
    return not lastTuple;
}

Record SIMDJSONRawBufferIndex::readSpanningRecord(
    const std::vector<Record::RecordFieldIdentifier>& projections,
    const nautilus::val<int8_t*>&,
    const nautilus::val<uint64_t>&,
    const InputFormatIndexer& indexer,
    nautilus::val<RawBufferIndex*> rawBufferIndex,
    const TupleBufferRef& bufferRef,
    const ArenaRef& arena) const
{
    Record record;
    const auto numberOfFields = bufferRef.getAllDataTypes().size();
    for (nautilus::static_val<uint64_t> i = 0; i < numberOfFields; ++i)
    {
        const auto fieldName = bufferRef.getAllFieldNames().at(i);

        if (std::ranges::find(projections, fieldName) == projections.end())
        {
            continue;
        }

        auto fieldIndex = static_cast<nautilus::val<FieldIndex>>(i);
        const auto fieldDataType = bufferRef.getAllDataTypes().at(i);

        nautilus::val<RawJsonAccessResult> fieldAccessResult;
        /// Retrieve the address and size of the raw field value
        /// Workaround to pass nullable into the template. Will be resolved during tracetime as nullable will always be known before compiling the query.
        fieldDataType.nullable ? nautilus::invoke(
                                     getRawValueFromIndex<true>,
                                     fieldIndex,
                                     rawBufferIndex,
                                     nautilus::val<const InputFormatIndexer*>(&indexer),
                                     &fieldAccessResult)
                               : nautilus::invoke(
                                     getRawValueFromIndex<false>,
                                     fieldIndex,
                                     rawBufferIndex,
                                     nautilus::val<const InputFormatIndexer*>(&indexer),
                                     &fieldAccessResult);

        const nautilus::val<const int8_t*> address = fieldAccessResult.get(&RawJsonAccessResult::ptrToRawJson);
        const nautilus::val<uint64_t> size = fieldAccessResult.get(&RawJsonAccessResult::sizeOfRawJson);

        /// Create the deserializer for the field and deserialize the value.
        /// These are the temporary defaults for our JSON format. Later, these arguments will be set by the user in the source definition.
        const ValueDeserializerConfig deserializerConfig{.nullable = fieldDataType.nullable, .quoted = true, .hasTrailingSpaces = true};
        const std::unique_ptr<ValueDeserializer> valueDeserializer
            = provideValueDeserializer(indexer.getDeserializerType(fieldName, fieldDataType.type), deserializerConfig);
        const VarVal parsedVal = valueDeserializer->deserializeToVarVal(address, size, indexer.getNullValues(), arena);
        record.write(fieldName, parsedVal);
    }
    /// Increment iterator and return record
    nautilus::invoke(
        +[](RawBufferIndex* rawBufferIndexPtr)
        {
            PRECONDITION(
                dynamic_cast<SIMDJSONRawBufferIndex*>(rawBufferIndexPtr) != nullptr, "rawBufferIndex must be a SIMDJSONRawBufferIndex");
            /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast): type verified by PRECONDITION above.
            auto* simdJsonBufferIndex = static_cast<SIMDJSONRawBufferIndex*>(rawBufferIndexPtr);
            ++simdJsonBufferIndex->docStreamIterator;
            simdJsonBufferIndex->isAtLastTuple = simdJsonBufferIndex->docStreamIterator.at_end();
        },
        rawBufferIndex);
    return record;
}

/// Marks the buffer as containing no tuple delimiters by setting both offsets to `max()`.
void SIMDJSONRawBufferIndex::markNoTupleDelimiters()
{
    this->offsetOfFirstTuple = std::numeric_limits<FieldIndex>::max();
    this->offsetOfLastTuple = std::numeric_limits<FieldIndex>::max();
}

void SIMDJSONRawBufferIndex::markWithTupleDelimiters(const FieldIndex offsetToFirstTuple, const std::optional<FieldIndex> offsetToLastTuple)
{
    this->offsetOfFirstTuple = offsetToFirstTuple;
    this->offsetOfLastTuple = offsetToLastTuple.value_or(std::numeric_limits<FieldIndex>::max());
}

std::pair<bool, FieldIndex> SIMDJSONRawBufferIndex::indexJSON(const std::string_view jsonSV)
{
    return indexJSON(jsonSV, simdjson::ondemand::DEFAULT_BATCH_SIZE);
}

std::pair<bool, FieldIndex> SIMDJSONRawBufferIndex::indexJSON(const std::string_view jsonSV, size_t batchSize)
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
