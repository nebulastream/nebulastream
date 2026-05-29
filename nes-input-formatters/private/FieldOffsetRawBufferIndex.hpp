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

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <vector>

#include <Interface/BufferRef/TupleBufferRef.hpp>
#include <Interface/Record.hpp>
#include <InputFormatter.hpp>
#include <RawBufferIndex.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>
#include <val_concepts.hpp>
#include <val_ptr.hpp>

namespace NES
{

inline bool
includesField(const std::vector<NES::Record::RecordFieldIdentifier>& projections, const NES::Record::RecordFieldIdentifier& fieldIndex)
{
    return std::ranges::find(projections, fieldIndex) != projections.end();
}

class FieldOffsetRawBufferIndex final : public RawBufferIndex
{
public:
    FieldOffsetRawBufferIndex();
    ~FieldOffsetRawBufferIndex() override = default;

    [[nodiscard]] nautilus::val<bool>
    hasNext(const nautilus::val<uint64_t>& tupleIdx, const nautilus::val<RawBufferIndex*>& rawBufferIndex) const override;

    static const FieldIndex* getIndexValuesProxy(const RawBufferIndex* rawBufferIndex);

    [[nodiscard]] Record readSpanningRecord(
        const std::vector<Record::RecordFieldIdentifier>& projections,
        const nautilus::val<int8_t*>& recordBufferPtr,
        const nautilus::val<uint64_t>& recordIndex,
        const InputFormatIndexer& indexer,
        nautilus::val<RawBufferIndex*> rawBufferIndex,
        const TupleBufferRef& bufferRef) const override;

    [[nodiscard]] TupleDelimiterOffsets getTupleDelimiterOffsets() const override
    {
        return {.first = offsetOfFirstTuple, .last = offsetOfLastTuple};
    }

    [[nodiscard]] size_t getNumberOfTuples() const override { return totalNumberOfTuples; }

    void startSetup(size_t numberOfFieldsInSchema);

    void emplaceFieldOffset(FieldIndex offset) { this->indexValues.emplace_back(offset); }

    /// Sets offsetOfFirstTuple and offsetOfLastTuple to 'max' values, indicating that none were found
    void markNoTupleDelimiters();

    /// Sets offsetOfFirstTuple and offsetOfLastTuple and the total number of tuples found (calculated from number of offsets)
    void markWithTupleDelimiters(FieldIndex offsetToFirstTuple, FieldIndex offsetToLastTuple);

private:
    size_t numberOfFieldsInSchema{};
    size_t numberOfOffsetsPerTuple{};
    size_t totalNumberOfTuples{};
    FieldIndex offsetOfFirstTuple{};
    FieldIndex offsetOfLastTuple{};
    std::vector<FieldIndex> indexValues;
};

}
