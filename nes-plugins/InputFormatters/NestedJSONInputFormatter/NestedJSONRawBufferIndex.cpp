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

#include <NestedJSONRawBufferIndex.hpp>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <string_view>
#include <utility>
#include <vector>
#include <simdjson.h>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypesUtil.hpp>
#include <DataTypes/VarVal.hpp>
#include <Interface/Record.hpp>
#include <ErrorHandling.hpp>
#include <InputFormatIndexer.hpp>
#include <NestedJSONInputFormatIndexer.hpp>
#include <RawBufferIndex.hpp>
#include <RawTupleBuffer.hpp>
#include <ValueDeserializer.hpp>
#include <ValueDeserializerUtil.hpp>
#include <function.hpp>
#include <static.hpp>
#include <val.hpp>
#include <val_ptr.hpp>

namespace NES
{

namespace
{

/// This is obtained after accessing the raw (unparsed) value of a specific field
/// If the access of the value fails, ptrToRawJSON is set to nullptr and sizeOfRawJson is set to 0
struct RawJsonAccessResult
{
    const int8_t* ptrToRawJson;
    uint64_t sizeOfRawJson;
};

simdjson::simdjson_result<simdjson::ondemand::value> accessSIMDJsonFieldOrThrow(
    simdjson::simdjson_result<simdjson::ondemand::document_reference>& simdJsonReference, const std::string_view fieldName)
{
    auto slashPos = fieldName.find('/');
    if (slashPos == std::string_view::npos)
    {
        const auto result = simdJsonReference.find_field_unordered(fieldName);
        if (not result.has_value())
        {
            throw FieldNotFound("SimdJson has not found the fieldName {} with error: {}", fieldName, magic_enum::enum_name(result.error()));
        }
        return result;
    }
    auto result = simdJsonReference.find_field_unordered(fieldName.substr(0, slashPos));
    if (not result.has_value())
    {
        throw FieldNotFound(
            "SimdJson has not found the fieldName {} with error: {}", fieldName.substr(0, slashPos), magic_enum::enum_name(result.error()));
    }
    auto path = fieldName.substr(slashPos + 1);
    for (slashPos = path.find('/'); slashPos != std::string_view::npos; slashPos = path.find('/'))
    {
        result = result.find_field_unordered(path.substr(0, slashPos));
        if (not result.has_value())
        {
            throw FieldNotFound(
                "SimdJson has not found the fieldName {} with error: {}", path.substr(0, slashPos), magic_enum::enum_name(result.error()));
        }
        path = path.substr(slashPos + 1);
    }
    result = result.find_field_unordered(path);
    if (not result.has_value())
    {
        throw FieldNotFound("SimdJson has not found the fieldName {} with error: {}", path, magic_enum::enum_name(result.error()));
    }
    return result;
}

bool checkIsNullJsonProxy(
    const FieldIndex fieldIndex, const NestedJSONRawBufferIndex* simdJsonRawBufferIndex, const NestedJSONInputFormatIndexer* simdIndexer)
{
    const auto fieldNameStr = simdIndexer->getFieldNameInJsonAt(fieldIndex).asCanonicalString();
    const std::string_view fieldName = fieldNameStr;
    auto currentDoc = *simdJsonRawBufferIndex->getDocStreamIterator();

    /// Second, we need to check if the key is equal to one of the null values
    try
    {
        if (accessSIMDJsonFieldOrThrow(currentDoc, fieldName).is_null())
        {
            return true;
        }
    }
    catch (Exception& e)
    {
        return true;
    }
    return false;
}

/// (Proxy) functions being called via nautilus::invoke() can not be member functions. Thus, we need to implement them outside of the class
template <bool Nullable>
RawJsonAccessResult* getRawValueFromIndex(const FieldIndex fieldIndex, RawBufferIndex* rawBufferIndex, const InputFormatIndexer* indexer)
{
    PRECONDITION(dynamic_cast<NestedJSONRawBufferIndex*>(rawBufferIndex) != nullptr, "rawBufferIndex must be a NestedJSONRawBufferIndex");
    PRECONDITION(dynamic_cast<const NestedJSONInputFormatIndexer*>(indexer) != nullptr, "indexer must be a NestedJSONInputFormatIndexer");
    /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast): type verified by PRECONDITION above.
    auto* simdJsonRawBufferIndex = static_cast<NestedJSONRawBufferIndex*>(rawBufferIndex);
    /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast): type verified by PRECONDITION above.
    const auto* simdIndexer = static_cast<const NestedJSONInputFormatIndexer*>(indexer);
    PRECONDITION(
        fieldIndex < simdIndexer->getNumberOfFields(),
        "fieldIndex {} is out of bounds for schema keys of size: {}",
        fieldIndex,
        simdIndexer->getNumberOfFields());

    /// We use the thread local to return multiple values.
    /// C++ guarantees that the returned address is valid throughout the lifetime of this thread.
    thread_local static RawJsonAccessResult result;
    result.ptrToRawJson = nullptr;
    result.sizeOfRawJson = 0;

    /// Checking if the field is null but only if the field is nullable
    /// This null-check includes a check, if the key of the field exists in the object or if the value is set to NULL.
    if constexpr (Nullable)
    {
        if (checkIsNullJsonProxy(fieldIndex, simdJsonRawBufferIndex, simdIndexer))
        {
            return &result;
        }
    }

    const auto fieldNameStr = simdIndexer->getFieldNameInJsonAt(fieldIndex).asCanonicalString();
    const std::string_view fieldName = fieldNameStr;
    auto currentDoc = *simdJsonRawBufferIndex->getDocStreamIterator();
    std::string_view rawValue = accessSIMDJsonFieldOrThrow(currentDoc, fieldName).raw_json().value();
    /// The actual parse of the value will be handeled by the ValueDeserializer
    result.ptrToRawJson = reinterpret_cast<const int8_t*>(rawValue.data());
    result.sizeOfRawJson = rawValue.size();
    return &result;
}
}

NestedJSONRawBufferIndex::NestedJSONRawBufferIndex()
{
    INVARIANT(
        static_cast<void*>(this) == static_cast<void*>(static_cast<RawBufferIndex*>(this)),
        "RawBufferIndex base subobject must lay out at offset 0 in NestedJSONRawBufferIndex");
}

[[nodiscard]] nautilus::val<bool>
NestedJSONRawBufferIndex::hasNext(const nautilus::val<uint64_t>&, const nautilus::val<RawBufferIndex*>& rawBufferIndex) const
{
    const nautilus::val<bool> lastTuple = readValueFromMemRef<bool>(getMemberRef(rawBufferIndex, &NestedJSONRawBufferIndex::isAtLastTuple));
    return not lastTuple;
}

Record NestedJSONRawBufferIndex::readSpanningRecord(
    const std::vector<Record::RecordFieldIdentifier>& projections,
    const nautilus::val<int8_t*>&,
    const nautilus::val<uint64_t>&,
    const InputFormatIndexer& indexer,
    nautilus::val<RawBufferIndex*> rawBufferIndex,
    const TupleBufferRef& bufferRef,
    ArenaRef& arena) const
{
    Record record;
    const auto numberOfFields = nautilus::static_val{bufferRef.getAllDataTypes().size()};
    const nautilus::val<const InputFormatIndexer*> indexerVal{&indexer};
    /// The indexer of a NestedJSONRawBufferIndex is always a NestedJSONInputFormatIndexer. The cast happens at trace time on a
    /// plain C++ reference, so it never reaches the traced code.
    const auto& nestedIndexer = dynamic_cast<const NestedJSONInputFormatIndexer&>(indexer);

    for (nautilus::static_val<uint64_t> i = 0; i < numberOfFields; ++i)
    {
        const auto fieldName = bufferRef.getAllFieldNames().at(i);
        if (std::ranges::find(projections, fieldName) == projections.end())
        {
            continue;
        }
        auto fieldIndex = static_cast<nautilus::val<FieldIndex>>(i);
        /// Must be a reference into the indexer, which outlives compilation, and not a copy. JSONSTRUCTValueDeserializer bakes
        /// pointers to the sub-field names of this DataType into the traced code as constants.
        const DataType& fieldDataType = nestedIndexer.getFieldDataTypeAt(i);

        /// Retrieve the address and size of the raw field value
        /// Workaround to pass nullable into the template. Will be resolved during tracetime as nullable will always be known before compiling the query.
        const auto fieldAccessResult = fieldDataType.nullable ? nautilus::invoke(
                                                                    {.modRefInfo = nautilus::ModRefInfo::Ref},
                                                                    getRawValueFromIndex<true>,
                                                                    fieldIndex,
                                                                    rawBufferIndex,
                                                                    nautilus::val<const InputFormatIndexer*>(&indexer))
                                                              : nautilus::invoke(
                                                                    {.modRefInfo = nautilus::ModRefInfo::Ref},
                                                                    getRawValueFromIndex<false>,
                                                                    fieldIndex,
                                                                    rawBufferIndex,
                                                                    nautilus::val<const InputFormatIndexer*>(&indexer));

        const nautilus::val<int8_t*> address
            = *getMemberWithOffset<int8_t*>(fieldAccessResult, offsetof(RawJsonAccessResult, ptrToRawJson));
        const nautilus::val<uint64_t> size
            = *getMemberWithOffset<uint64_t>(fieldAccessResult, offsetof(RawJsonAccessResult, sizeOfRawJson));

        /// Create the deserializer for the field and deserialize the value.
        /// These are the temporary defaults for our JSON format. Later, these arguments will be set by the user in the source definition.
        const ValueDeserializerConfig deserializerConfig{.nullable = fieldDataType.nullable, .quoted = true, .hasTrailingSpaces = true};
        const std::unique_ptr<ValueDeserializer> valueDeserializer
            = provideValueDeserializer(indexer.getDeserializerType(fieldDataType.type), deserializerConfig);
        const VarVal parsedVal = valueDeserializer->deserializeToVarVal(
            address, size, indexer.getNullValues(), indexer.getDeserializerTypes(), fieldDataType, arena);
        record.write(fieldName, parsedVal);
    }

    nautilus::invoke(
        +[](RawBufferIndex* bi)
        {
            auto* nestedJsonBI = dynamic_cast<NestedJSONRawBufferIndex*>(bi);
            ++nestedJsonBI->docStreamIterator;
            nestedJsonBI->isAtLastTuple = nestedJsonBI->docStreamIterator.at_end();
        },
        rawBufferIndex);
    return record;
}

void NestedJSONRawBufferIndex::markNoTupleDelimiters()
{
    this->offsetOfFirstTuple = std::numeric_limits<FieldIndex>::max();
    this->offsetOfLastTuple = std::numeric_limits<FieldIndex>::max();
}

void NestedJSONRawBufferIndex::markWithTupleDelimiters(
    const FieldIndex offsetToFirstTuple, const std::optional<FieldIndex> offsetToLastTuple)
{
    this->offsetOfFirstTuple = offsetToFirstTuple;
    this->offsetOfLastTuple = offsetToLastTuple.value_or(std::numeric_limits<FieldIndex>::max());
}

std::pair<bool, FieldIndex> NestedJSONRawBufferIndex::indexJSON(const std::string_view jsonSV)
{
    return indexJSON(jsonSV, simdjson::ondemand::DEFAULT_BATCH_SIZE);
}

std::pair<bool, FieldIndex> NestedJSONRawBufferIndex::indexJSON(const std::string_view jsonSV, size_t batchSize)
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
