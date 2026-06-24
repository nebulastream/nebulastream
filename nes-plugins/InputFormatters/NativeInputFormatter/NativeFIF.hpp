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
#include <limits>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <Nautilus/DataTypes/DataTypesUtil.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <ErrorHandling.hpp>
#include <FieldIndexFunction.hpp>
#include <FieldOffsets.hpp>
#include <static.hpp>
#include <val.hpp>
#include <val_arith.hpp>
#include <val_ptr.hpp>

namespace NES
{

/// FieldIndexFunction for the native input format.
/// The native format assumes that raw buffer bytes already match the internal row tuple binary layout per the schema:
///   `tuple n` lives at byte `n * tupleSize`. There is no per-tuple parsing — fields are loaded directly from their
///   fixed schema offsets.
/// State is set during the indexer's `indexRawBuffer` call and consumed by the InputFormatter.
class NativeFIF final : public FieldIndexFunction<NativeFIF>
{
    friend FieldIndexFunction<NativeFIF>;

    [[nodiscard]] FieldIndex applyGetByteOffsetOfFirstTuple() const { return offsetOfFirstTuple; }

    [[nodiscard]] FieldIndex applyGetByteOffsetOfLastTuple() const { return offsetOfLastTuple; }

    [[nodiscard]] size_t applyGetTotalNumberOfTuples() const { return totalNumberOfTuples; }

    [[nodiscard]] nautilus::val<bool>
    applyHasNext(const nautilus::val<uint64_t>& recordIdx, nautilus::val<NativeFIF*> fieldIndexFunctionPtr) const
    {
        const nautilus::val<uint64_t> total = *getMemberWithOffset<size_t>(fieldIndexFunctionPtr, offsetof(NativeFIF, totalNumberOfTuples));
        return recordIdx < total;
    }

    template <typename IndexerMetaData>
    [[nodiscard]] Record applyReadSpanningRecord(
        const std::vector<Record::RecordFieldIdentifier>& projections,
        const nautilus::val<int8_t*>& recordBufferPtr,
        const nautilus::val<uint64_t>& recordIndex,
        const IndexerMetaData& metaData,
        const nautilus::val<NativeFIF*> fif,
        const std::unordered_map<DataType::Type, std::string>&,
        const std::unordered_map<DataType::Type, bool>&) const
    {
        nautilus::val<FieldIndex> offset = *getMemberWithOffset<FieldIndex>(fif, offsetof(NativeFIF, offsetOfFirstTuple));
        return metaData.readRecord(projections, recordBufferPtr, recordIndex, offset);
    }

public:
    NativeFIF() = default;
    ~NativeFIF() = default;

    void setIndexedBuffer(const FieldIndex offsetToFirstTuple, const FieldIndex offsetToLastTuple, const size_t numberOfTuples)
    {
        this->offsetOfFirstTuple = offsetToFirstTuple;
        this->offsetOfLastTuple = offsetToLastTuple;
        this->totalNumberOfTuples = numberOfTuples;
    }

    void markNoTupleDelimiters()
    {
        this->offsetOfFirstTuple = std::numeric_limits<FieldIndex>::max();
        this->offsetOfLastTuple = std::numeric_limits<FieldIndex>::max();
        this->totalNumberOfTuples = 0;
    }

private:
    FieldIndex offsetOfFirstTuple{};
    FieldIndex offsetOfLastTuple{};
    size_t totalNumberOfTuples{};
};

}
