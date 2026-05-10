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
#include <limits>
#include <memory>
#include <optional>
#include <string_view>
#include <utility>
#include <vector>
#include <simdjson.h>

#include <Nautilus/Interface/Record.hpp>
#include <ErrorHandling.hpp>
#include <InputFormatIndexer.hpp>
#include <JsonValueParser.hpp>
#include <RawBufferIndex.hpp>
#include <RawTupleBuffer.hpp>
#include <SIMDJSONInputFormatIndexer.hpp>
#include <function.hpp>
#include <static.hpp>
#include <val.hpp>
#include <val_ptr.hpp>

namespace NES
{

namespace
{

/// Flat-JSON traits: a single `find_field_unordered` (via `operator[]`, which
/// internally calls the same routine — no `rewind()`) walks straight to the
/// field, and FIXEDSIZED arrays are not supported in the flat schema model.
struct FlatJsonTraits
{
    using BufferIndex = SIMDJSONRawBufferIndex;
    using Indexer = SIMDJSONInputFormatIndexer;

    static simdjson::simdjson_result<simdjson::ondemand::value>
    navigate(simdjson::simdjson_result<simdjson::ondemand::document_reference>& doc, const std::string_view fieldName)
    {
        return doc[fieldName];
    }

    static constexpr bool supportsFixedSized = false;
};

using FlatParser = JsonValueParser::JsonRecordParser<FlatJsonTraits>;

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
    ArenaRef& arena) const
{
    Record record;
    const auto numberOfFields = nautilus::static_val{bufferRef.getAllDataTypes().size()};
    const nautilus::val<const InputFormatIndexer*> indexerVal{&indexer};

    for (nautilus::static_val<uint64_t> i = 0; i < numberOfFields; ++i)
    {
        const auto fieldName = bufferRef.getAllFieldNames().at(i);
        if (std::ranges::find(projections, fieldName) == projections.end())
        {
            continue;
        }
        const auto fieldDataType = bufferRef.getAllDataTypes().at(i);
        auto fieldIndex = static_cast<nautilus::val<FieldIndex>>(i);
        FlatParser::writeValueToRecord(fieldDataType, record, fieldName, fieldIndex, rawBufferIndex, indexerVal, arena);
    }

    nautilus::invoke(
        +[](RawBufferIndex* bi)
        {
            auto* simdJsonBI = dynamic_cast<SIMDJSONRawBufferIndex*>(bi);
            ++simdJsonBI->docStreamIterator;
            simdJsonBI->isAtLastTuple = simdJsonBI->docStreamIterator.at_end();
        },
        rawBufferIndex);
    return record;
}

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
