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
#include <memory>


#include <JSONTupleBufferMemoryProvider.hpp>

#include <memory>
#include <utility>
#include <vector>
#include <MemoryLayout/ColumnLayout.hpp>
#include <MemoryLayout/MemoryLayout.hpp>
#include <MemoryLayout/RowLayout.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <nautilus/static.hpp>
#include <nautilus/val.hpp>
#include <nautilus/val_ptr.hpp>

#include <JSONInputFormatIndexer.hpp>

namespace NES::Nautilus::Interface::MemoryProvider
{

struct SpanningTuplePOD
{
    int8_t* leadingSpanningTuplePtr = nullptr;
    int8_t* trailingSpanningTuplePtr = nullptr;
    bool hasLeadingSpanningTupleBool = false;
    bool hasTrailingSpanningTupleBool = false;
    uint64_t totalNumberOfTuples = 0;
    size_t leadingSpanningTupleSizeInBytes = 0;
    size_t trailingSpanningTupleSizeInBytes = 0;
    bool isRepeat = false;

    FieldOffsets<JSON_NUM_OFFSETS_PER_FIELD> leadingSpanningTupleFIF;
    FieldOffsets<JSON_NUM_OFFSETS_PER_FIELD> trailingSpanningTupleFIF;
    FieldOffsets<JSON_NUM_OFFSETS_PER_FIELD> rawBufferFIF;
};

struct State
{
    nautilus::val<SpanningTuplePOD*> spanningTuple;
    ArenaRef& arena;
};

static thread_local std::unique_ptr<State> state{};

JSONTupleBufferMemoryProvider::JSONTupleBufferMemoryProvider(ParserConfig config, std::shared_ptr<MemoryLayout> layout)
    : shredder(std::make_unique<SequenceShredder>(config.tupleDelimiter.size()))
    , metadata(std::make_unique<JSONMetaData>(std::move(config), *layout))
    , layout(std::move(layout))
{
}

std::shared_ptr<MemoryLayout> JSONTupleBufferMemoryProvider::getMemoryLayout() const
{
    return layout;
}

/// accumulates all data produced during the indexing phase, which the parsing phase requires
/// on calling 'createSpanningTuplePOD()' it returns this data
class SpanningTupleData
{
public:
    explicit SpanningTupleData() = default;

    std::pair<FieldIndex, FieldIndex> indexRawBuffer(const JSONMetaData& metaData, const TupleBuffer& tupleBuffer)
    {
        JSONInputFormatIndexer().indexRawBuffer(spanningTuplePOD.rawBufferFIF, RawTupleBuffer{tupleBuffer}, metaData);
        this->spanningTuplePOD.totalNumberOfTuples = this->spanningTuplePOD.rawBufferFIF.getTotalNumberOfTuples();
        return {
            spanningTuplePOD.rawBufferFIF.getOffsetOfFirstTupleDelimiter(), spanningTuplePOD.rawBufferFIF.getOffsetOfLastTupleDelimiter()};
    }

    void setIsRepeat(bool isRepeat) { this->spanningTuplePOD.isRepeat = isRepeat; }

    SpanningTuplePOD* getThreadLocalSpanningTuplePODPtr() const
    {
        /// each thread sets up a static address for a POD struct that contains all relevent data that the indexing phase produces
        /// this allows this proxy function to return a, guaranteed to be valid, reference to the POD tho the compiled query pipeline
        thread_local SpanningTuplePOD spanningTuple{};
        spanningTuple = std::move(spanningTuplePOD);
        return &spanningTuple;
    }

    void handleWithDelimiter(
        const std::vector<StagedBuffer>& stagedBuffers,
        const size_t indexOfSequenceNumberInStagedBuffers,
        const JSONMetaData& metaData,
        Arena& arenaRef)
    {
        calculateSizeOfSpanningTuplesWithDelimiter(
            stagedBuffers, indexOfSequenceNumberInStagedBuffers, metaData.getTupleDelimitingBytes().size());

        if (spanningTuplePOD.leadingSpanningTupleSizeInBytes > 0)
        {
            allocateForLeadingSpanningTuple(arenaRef);
            const auto leadingSpanningTupleBuffers = std::span(stagedBuffers).subspan(0, indexOfSequenceNumberInStagedBuffers + 1);
            processSpanningTuple<JSONMetaData>(
                leadingSpanningTupleBuffers,
                std::span<int8_t>{spanningTuplePOD.leadingSpanningTuplePtr, spanningTuplePOD.leadingSpanningTupleSizeInBytes},
                metaData);
            JSONInputFormatIndexer().indexRawBuffer(
                spanningTuplePOD.leadingSpanningTupleFIF,
                RawTupleBuffer{
                    std::bit_cast<const char*>(spanningTuplePOD.leadingSpanningTuplePtr), spanningTuplePOD.leadingSpanningTupleSizeInBytes},
                metaData);
        }
        if (spanningTuplePOD.trailingSpanningTupleSizeInBytes > 0)
        {
            allocateForTrailingSpanningTuple(arenaRef);
            const auto trailingSpanningTupleBuffers
                = std::span(stagedBuffers)
                      .subspan(indexOfSequenceNumberInStagedBuffers, stagedBuffers.size() - indexOfSequenceNumberInStagedBuffers);
            processSpanningTuple<JSONMetaData>(
                trailingSpanningTupleBuffers,
                std::span<int8_t>{spanningTuplePOD.trailingSpanningTuplePtr, spanningTuplePOD.trailingSpanningTupleSizeInBytes},
                metaData);
            JSONInputFormatIndexer().indexRawBuffer(
                spanningTuplePOD.trailingSpanningTupleFIF,
                RawTupleBuffer{
                    std::bit_cast<const char*>(spanningTuplePOD.trailingSpanningTuplePtr),
                    spanningTuplePOD.trailingSpanningTupleSizeInBytes},
                metaData);
        }
    }

    void handleWithoutDelimiter(const std::vector<StagedBuffer>& spanningTupleBuffers, const JSONMetaData& metaData, Arena& arenaRef)
    {
        calculateSizeOfSpanningTuple(
            [this](const size_t bytes) { increaseLeadingSpanningTupleSize(bytes); },
            spanningTupleBuffers,
            metaData.getTupleDelimitingBytes().size());
        allocateForLeadingSpanningTuple(arenaRef);

        processSpanningTuple<JSONMetaData>(
            spanningTupleBuffers,
            std::span<int8_t>{spanningTuplePOD.leadingSpanningTuplePtr, spanningTuplePOD.leadingSpanningTupleSizeInBytes},
            metaData);
        JSONInputFormatIndexer().indexRawBuffer(
            spanningTuplePOD.leadingSpanningTupleFIF,
            RawTupleBuffer{
                std::bit_cast<const char*>(spanningTuplePOD.leadingSpanningTuplePtr), spanningTuplePOD.leadingSpanningTupleSizeInBytes},
            metaData);
    }

private:
    SpanningTuplePOD spanningTuplePOD;

    void allocateForLeadingSpanningTuple(Arena& arenaRef)
    {
        this->spanningTuplePOD.hasLeadingSpanningTupleBool = true;
        this->spanningTuplePOD.leadingSpanningTuplePtr = arenaRef.allocateMemory(spanningTuplePOD.leadingSpanningTupleSizeInBytes);
    }

    void allocateForTrailingSpanningTuple(Arena& arenaRef)
    {
        this->spanningTuplePOD.hasTrailingSpanningTupleBool = true;
        this->spanningTuplePOD.trailingSpanningTuplePtr = arenaRef.allocateMemory(spanningTuplePOD.trailingSpanningTupleSizeInBytes);
    }

    void increaseLeadingSpanningTupleSize(const size_t additionalBytes)
    {
        spanningTuplePOD.leadingSpanningTupleSizeInBytes += additionalBytes;
    }

    void increaseTrailingSpanningTupleSize(const size_t additionalBytes)
    {
        spanningTuplePOD.trailingSpanningTupleSizeInBytes += additionalBytes;
    }

    template <typename IncreaseFunc>
    static void calculateSizeOfSpanningTuple(
        const IncreaseFunc& increaseFunc,
        const std::span<const StagedBuffer> spanningTupleBuffers,
        const size_t sizeOfTupleDelimiterInBytes)
    {
        increaseFunc(2 * sizeOfTupleDelimiterInBytes);
        increaseFunc(spanningTupleBuffers.front().getTrailingBytes(sizeOfTupleDelimiterInBytes).size());
        for (size_t i = 1; i < spanningTupleBuffers.size() - 1; ++i)
        {
            increaseFunc(spanningTupleBuffers[i].getSizeOfBufferInBytes());
        }
        increaseFunc(spanningTupleBuffers.back().getLeadingBytes().size());
    }

    void calculateSizeOfSpanningTuplesWithDelimiter(
        const std::vector<StagedBuffer>& spanningTupleBuffers,
        const size_t indexOfSequenceNumberInStagedBuffers,
        const size_t sizeOfTupleDelimiterInBytes)
    {
        const bool hasLeadingST = indexOfSequenceNumberInStagedBuffers != 0;
        const bool hasTrailingST = indexOfSequenceNumberInStagedBuffers < spanningTupleBuffers.size() - 1;
        INVARIANT(hasLeadingST or hasTrailingST, "cannot calculate size of spanning tuple for buffers without spanning tuples");

        /// Has leading spanning tuple only
        if (hasLeadingST and not hasTrailingST)
        {
            calculateSizeOfSpanningTuple(
                [this](const size_t bytes) { increaseLeadingSpanningTupleSize(bytes); }, spanningTupleBuffers, sizeOfTupleDelimiterInBytes);
            return;
        }
        /// Has trailing spanning tuple only
        if (not hasLeadingST)
        {
            calculateSizeOfSpanningTuple(
                [this](const size_t bytes) { increaseTrailingSpanningTupleSize(bytes); },
                spanningTupleBuffers,
                sizeOfTupleDelimiterInBytes);
            return;
        }
        /// Has both leading and spanning tuple
        calculateSizeOfSpanningTuple(
            [this](const size_t bytes) { increaseLeadingSpanningTupleSize(bytes); },
            std::span(spanningTupleBuffers).subspan(0, indexOfSequenceNumberInStagedBuffers + 1),
            sizeOfTupleDelimiterInBytes);

        calculateSizeOfSpanningTuple(
            [this](const size_t bytes) { increaseTrailingSpanningTupleSize(bytes); },
            std::span(spanningTupleBuffers)
                .subspan(indexOfSequenceNumberInStagedBuffers, spanningTupleBuffers.size() - (indexOfSequenceNumberInStagedBuffers)),
            sizeOfTupleDelimiterInBytes);
    }
};

static SpanningTuplePOD* indexTuplesProxy(
    const TupleBuffer* tupleBuffer,
    SequenceShredder& sequenceShredder,
    const JSONMetaData metaData,
    const size_t configuredBufferSize,
    Arena* arenaRef)
{
    SpanningTupleData spanningTupleData{};
    if (not sequenceShredder.isInRange(tupleBuffer->getSequenceNumber().getRawValue()))
    {
        INVARIANT(false, "This is not handled");
    }

    const auto [offsetOfFirstTupleDelimiter, offsetOfSecondTupleDelimiter] = spanningTupleData.indexRawBuffer(metaData, *tupleBuffer);


    if (/* hasTupleDelimiter */ offsetOfFirstTupleDelimiter < configuredBufferSize)
    {
        const auto [indexOfSequenceNumberInStagedBuffers, stagedBuffers] = sequenceShredder.processSequenceNumber<true>(
            StagedBuffer{
                RawTupleBuffer{*tupleBuffer},
                tupleBuffer->getNumberOfTuples(), /// size in bytes
                offsetOfFirstTupleDelimiter,
                offsetOfSecondTupleDelimiter},
            tupleBuffer->getSequenceNumber().getRawValue());

        if (stagedBuffers.size() < 2)
        {
            return spanningTupleData.getThreadLocalSpanningTuplePODPtr();
        }
        spanningTupleData.handleWithDelimiter(stagedBuffers, indexOfSequenceNumberInStagedBuffers, metaData, *arenaRef);
    }
    else /* has no tuple delimiter */
    {
        const auto [indexOfSequenceNumberInStagedBuffers, stagedBuffers] = sequenceShredder.processSequenceNumber<false>(
            StagedBuffer{
                RawTupleBuffer{*tupleBuffer},
                tupleBuffer->getNumberOfTuples(), /// size in bytes
                offsetOfFirstTupleDelimiter,
                offsetOfSecondTupleDelimiter},
            tupleBuffer->getSequenceNumber().getRawValue());

        if (stagedBuffers.size() < 3)
        {
            return spanningTupleData.getThreadLocalSpanningTuplePODPtr();
        }

        /// The buffer has no delimiter, but connects two buffers with delimiters, forming one spanning tuple
        /// We arbitrarily treat it as a 'leading' spanning tuple (technically, it is both leading and trailing)
        spanningTupleData.handleWithoutDelimiter(stagedBuffers, metaData, *arenaRef);
    }
    return spanningTupleData.getThreadLocalSpanningTuplePODPtr();
}

Record JSONTupleBufferMemoryProvider::readRecord(
    const std::vector<Record::RecordFieldIdentifier>& projections,
    const RecordBuffer& recordBuffer,
    nautilus::val<uint64_t>& recordIndex) const
{
    auto spanningTuplePOD = state->spanningTuple;
    auto hasLeading = *Nautilus::Util::getMemberWithOffset<bool>(spanningTuplePOD, offsetof(SpanningTuplePOD, hasLeadingSpanningTupleBool));
    auto hasTrailing
        = *Nautilus::Util::getMemberWithOffset<bool>(spanningTuplePOD, offsetof(SpanningTuplePOD, hasTrailingSpanningTupleBool));

    auto leadingFIF = Nautilus::Util::getMemberWithOffset<FieldOffsets<JSON_NUM_OFFSETS_PER_FIELD>>(
        spanningTuplePOD, offsetof(SpanningTuplePOD, leadingSpanningTupleFIF));
    auto trailingFIF = Nautilus::Util::getMemberWithOffset<FieldOffsets<JSON_NUM_OFFSETS_PER_FIELD>>(
        spanningTuplePOD, offsetof(SpanningTuplePOD, trailingSpanningTupleFIF));
    auto rawFieldAccessFunction = Nautilus::Util::getMemberWithOffset<FieldOffsets<JSON_NUM_OFFSETS_PER_FIELD>>(
        spanningTuplePOD, offsetof(SpanningTuplePOD, rawBufferFIF));

    nautilus::val<uint64_t> offset = 0;
    if (hasLeading)
    {
        offset = 1;
    }

    if (recordIndex == 0)
    {
        /// parse leading spanning tuple if exists
        if (hasLeading)
        {
            /// Get leading field index function and a pointer to the spanning tuple 'record'
            auto spanningRecordPtr
                = *Nautilus::Util::getMemberPtrWithOffset<int8_t>(spanningTuplePOD, offsetof(SpanningTuplePOD, leadingSpanningTuplePtr));

            /// 'leadingFIF.value' is essentially the static function FormatterType::readsSpanningRecord
            auto recordIndex = nautilus::val<uint64_t>(0);
            return FieldOffsets<JSON_NUM_OFFSETS_PER_FIELD>{}.readSpanningRecord(
                projections, spanningRecordPtr, recordIndex, *metadata, leadingFIF, state->arena);
        }
    }
    else if (recordIndex + 1 == getNumberOfRecords(recordBuffer))
    {
        if (hasTrailing)
        {
            auto spanningRecordPtr
                = *Nautilus::Util::getMemberPtrWithOffset<int8_t>(spanningTuplePOD, offsetof(SpanningTuplePOD, trailingSpanningTuplePtr));

            auto recordIndex = nautilus::val<uint64_t>(0);
            return trailingFIF.value->readSpanningRecord(projections, spanningRecordPtr, recordIndex, *metadata, trailingFIF, state->arena);
        }
    }

    auto actualRecordIndex = recordIndex - offset;
    return FieldOffsets<JSON_NUM_OFFSETS_PER_FIELD>{}.readSpanningRecord(
        projections, recordBuffer.getMemArea(), actualRecordIndex, *metadata, rawFieldAccessFunction, state->arena);
}

void JSONTupleBufferMemoryProvider::writeRecord(
    nautilus::val<uint64_t>&, const RecordBuffer&, const Record&, const nautilus::val<AbstractBufferProvider*>&) const
{
    INVARIANT(false, "Yo this is read only");
}

void JSONTupleBufferMemoryProvider::open(const RecordBuffer& recordBuffer, ArenaRef& arenaRef)
{
    nautilus::val<SpanningTuplePOD*> spanningTuple = nautilus::invoke(
        +[](const JSONMetaData* metaData, SequenceShredder* shredder, TupleBuffer* buffer, Arena* arena)
        { return indexTuplesProxy(buffer, *shredder, *metaData, 4096, arena); },
        nautilus::val<const JSONMetaData*>(metadata.get()),
        nautilus::val<SequenceShredder*>(shredder.get()),
        recordBuffer.getReference(),
        arenaRef.getArena());
    state = std::make_unique<State>(spanningTuple, arenaRef);
}

nautilus::val<uint64_t> JSONTupleBufferMemoryProvider::getNumberOfRecords(const RecordBuffer&) const
{
    nautilus::val<uint64_t> totalNumberOfTuples
        = *Nautilus::Util::getMemberWithOffset<uint64_t>(state->spanningTuple, offsetof(SpanningTuplePOD, totalNumberOfTuples));

    if (*Nautilus::Util::getMemberWithOffset<bool>(state->spanningTuple, offsetof(SpanningTuplePOD, hasLeadingSpanningTupleBool)))
    {
        totalNumberOfTuples += 1;
    }

    if (*Nautilus::Util::getMemberWithOffset<bool>(state->spanningTuple, offsetof(SpanningTuplePOD, hasTrailingSpanningTupleBool)))
    {
        totalNumberOfTuples += 1;
    }

    return totalNumberOfTuples;
}

}
