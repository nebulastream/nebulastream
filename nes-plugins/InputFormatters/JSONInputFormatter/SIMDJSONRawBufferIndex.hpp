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
#include <ErrorHandling.hpp>
#include <InputFormatIndexer.hpp>
#include <RawBufferIndex.hpp>
#include <RawTupleBuffer.hpp>
#include <val.hpp>
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
        const TupleBufferRef& bufferRef,
        ArenaRef&) const override;

    [[nodiscard]] TupleDelimiterOffsets getTupleDelimiterOffsets() const override { return {offsetOfFirstTuple, offsetOfLastTuple}; }

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

}
