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
#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <limits>
#include <memory>
#include <ostream>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include <Util/Strings.hpp>

#include <Nautilus/DataTypes/DataTypesUtil.hpp>
#include <Nautilus/Interface/BufferRef/TupleBufferRef.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <nautilus/function.hpp>
#include <Arena.hpp>
#include <Concepts.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <RawTupleBuffer.hpp>
#include <SequenceShredder.hpp>
#include <val.hpp>
#include <val_arith.hpp>
#include <val_concepts.hpp>
#include <val_std.hpp>

namespace NES
{
/// The type that all formatters use to represent indexes to fields.
using FieldIndex = uint32_t;

/// Constructs a spanning tuple (string) that spans over at least two buffers (stagedBuffersSpan).
template <IndexerMetaDataType IndexerMetaData>
void processSpanningTuple(
    const std::span<const StagedBuffer>& stagedBuffersSpan, std::span<char> spanningTupleBuffer, const IndexerMetaData& indexerMetaData)
{
    INVARIANT(stagedBuffersSpan.size() >= 2, "A spanning tuple must span across at least two buffers");

    size_t offset = 0;
    auto boundsCheckedCopy = [&spanningTupleBuffer, &offset](std::string_view source)
    {
        INVARIANT(offset + source.size() <= spanningTupleBuffer.size(), "Buffer overflow: insufficient space in spanning tuple buffer");
        auto destSubspan = spanningTupleBuffer.subspan(offset, source.size());
        std::ranges::copy(source, destSubspan.begin());
        offset += source.size();
    };

    boundsCheckedCopy(indexerMetaData.getTupleDelimitingBytes());
    boundsCheckedCopy(stagedBuffersSpan.front().getTrailingBytes(indexerMetaData.getTupleDelimitingBytes().size()));

    for (const auto& buffer : stagedBuffersSpan.subspan(1, stagedBuffersSpan.size() - 2))
    {
        boundsCheckedCopy(buffer.getBufferView());
    }

    boundsCheckedCopy(stagedBuffersSpan.back().getLeadingBytes());
    boundsCheckedCopy(indexerMetaData.getTupleDelimitingBytes());
}

template <InputFormatIndexerType FormatterType>
class InputFormatter
{
public:
    explicit InputFormatter(
        FormatterType inputFormatIndexer, std::shared_ptr<TupleBufferRef> memoryProvider, const ParserConfig& parserConfig)
        : inputFormatIndexer(std::move(inputFormatIndexer))
        , indexerMetaData(typename FormatterType::IndexerMetaData{parserConfig, *memoryProvider})
        , projections(memoryProvider->getAllFieldNames())
        , memoryProvider(std::move(memoryProvider))
        , sequenceShredder(std::make_unique<SequenceShredder>(parserConfig.tupleDelimiter.size()))
    {
    }

    ~InputFormatter() = default;

    InputFormatter(const InputFormatter&) = delete;
    InputFormatter& operator=(const InputFormatter&) = delete;
    InputFormatter(InputFormatter&&) = default;
    InputFormatter& operator=(InputFormatter&&) = delete;

    [[nodiscard]] nautilus::val<bool> isEmpty(RecordBuffer&) const
    {
        nautilus::val<IndexPhaseResultBuffer> ipr;
        nautilus::invoke(loadIndexPhaseResultProxy, &ipr);
        return ipr.get(&IndexPhaseResultBuffer::numberOfTuples) == nautilus::val<uint64_t>(0);
    }

    [[nodiscard]] nautilus::val<bool> indexBuffer(const RecordBuffer& recordBuffer, ExecutionContext& executionContext) const
    {
        nautilus::invoke(
            +[](TupleBuffer* tb)
            {
                NES_DEBUG(
                    "Indexing buffer with seq: {}: {}",
                    tb->getSequenceRange(),
                    escapeSpecialCharacters(std::string_view(tb->getAvailableMemoryArea<char>())));
            },
            recordBuffer.getReference());

        /// Two-phase indexing: leading must complete before trailing can be resolved by other threads.
        /// The make_unique wrapping ensures the return value is used, creating a data dependency.
        const auto leadingResultPtr = std::make_unique<nautilus::val<IndexPhaseResult*>>(invoke(
            indexLeadingAndBufferProxy,
            recordBuffer.getReference(),
            nautilus::val<const InputFormatter*>(this),
            executionContext.pipelineMemoryProvider.arena.getArena()));

        const nautilus::val<bool> hasTrailingSpanningTuple = invoke(
            indexTrailingSpanningTupleProxy,
            recordBuffer.getReference(),
            nautilus::val<const InputFormatter*>(this),
            executionContext.pipelineMemoryProvider.arena.getArena());

        /// Snapshot the completed indexing result into a bridge struct
        nautilus::val<IndexPhaseResultBuffer> ipr;
        nautilus::invoke(loadIndexPhaseResultProxy, &ipr);

        if (ipr.get(&IndexPhaseResultBuffer::isRepeat))
        {
            return {false};
        }

        nautilus::val<bool> hasLeading = ipr.get(&IndexPhaseResultBuffer::hasLeading);
        nautilus::val<bool> hasTrailing = ipr.get(&IndexPhaseResultBuffer::hasTrailing);
        nautilus::val<uint64_t> leadingStartSeq = ipr.get(&IndexPhaseResultBuffer::leadingStartSequence);
        nautilus::val<uint64_t> trailingEndSeq = ipr.get(&IndexPhaseResultBuffer::trailingEndSequence);
        nautilus::val<bool> leadingBufferSingleDelim = ipr.get(&IndexPhaseResultBuffer::leadingBufferHasSingleDelimiter);
        nautilus::val<bool> currentSingleDelim = ipr.get(&IndexPhaseResultBuffer::currentBufferHasSingleDelimiter);

        /// Set sequence range start
        if (!hasLeading)
        {
            executionContext.sequenceRange.setStart(0, recordBuffer.getSequenceRange().getStart(0));
            executionContext.sequenceRange.setStart(1, 1);
        }
        else
        {
            if (leadingStartSeq == 0)
            {
                executionContext.sequenceRange.setStart(0, 1);
                executionContext.sequenceRange.setStart(1, 0);
            }
            else if (leadingBufferSingleDelim)
            {
                executionContext.sequenceRange.setStart(0, leadingStartSeq);
                executionContext.sequenceRange.setStart(1, 1);
            }
            else
            {
                executionContext.sequenceRange.setStart(0, leadingStartSeq);
                executionContext.sequenceRange.setStart(1, 2);
            }
        }

        /// Set sequence range end
        if (!hasTrailing)
        {
            if (currentSingleDelim)
            {
                executionContext.sequenceRange.setEnd(0, recordBuffer.getSequenceRange().getStart(0));
                executionContext.sequenceRange.setEnd(1, 1);
            }
            else
            {
                executionContext.sequenceRange.setEnd(0, recordBuffer.getSequenceRange().getStart(0));
                executionContext.sequenceRange.setEnd(1, 2);
            }
        }
        else
        {
            executionContext.sequenceRange.setEnd(0, trailingEndSeq);
            executionContext.sequenceRange.setEnd(1, 1);
        }

        return {true};
    }

    void readBuffer(
        ExecutionContext& executionCtx,
        const RecordBuffer& recordBuffer,
        const std::function<void(ExecutionContext& executionCtx, Record& record)>& executeChild) const
    {
        nautilus::val<IndexPhaseResultBuffer> ipr;
        nautilus::invoke(loadIndexPhaseResultProxy, &ipr);

        parseLeadingRecord(executionCtx, executeChild, ipr);

        if (not(nautilus::val<bool>(ipr.get(&IndexPhaseResultBuffer::hasTupleDelimiter))))
        {
            return;
        }

        parseRecordsInRawBuffer(executionCtx, recordBuffer, executeChild, ipr);
        parseTrailingRecord(executionCtx, executeChild, ipr);
    }

    std::ostream& toString(std::ostream& os) const
    {
        os << "InputFormatter(" << ", inputFormatIndexer: " << inputFormatIndexer << ", sequenceShredder: " << *sequenceShredder << ")\n";
        return os;
    }

private:
    FormatterType inputFormatIndexer;
    typename FormatterType::IndexerMetaData indexerMetaData;
    std::vector<Record::RecordFieldIdentifier> projections;
    std::shared_ptr<TupleBufferRef> memoryProvider;
    std::unique_ptr<SequenceShredder> sequenceShredder;

    /// Internal struct for the thread-local indexing state (C++ side only, not exposed to Nautilus)
    struct IndexPhaseResult
    {
        std::span<char> leadingSpanningTuple;
        std::span<char> trailingSpanningTuple;
        uint64_t leadingSpanningTupleStartSequence = 0;
        uint64_t trailingSpanningTupleEndSequence = 0;
        bool startBufferHasSingleDelimiter = false;
        bool currentBufferHasSingleDelimiter = false;
        bool hasLeadingSpanningTupleBool = false;
        bool hasTrailingSpanningTupleBool = false;
        bool hasTupleDelimiter = false;
        bool isRepeat = false;
        bool hasValidOffsetOfTrailingSpanningTuple = false;
        uint64_t numberOfTuples = 0;

        typename FormatterType::FieldIndexFunctionType leadingSpanningTupleFIF;
        typename FormatterType::FieldIndexFunctionType trailingSpanningTupleFIF;
        typename FormatterType::FieldIndexFunctionType rawBufferFIF;
    };

    static_assert(std::is_standard_layout_v<IndexPhaseResult>, "IndexPhaseResult must have a standard layout for safe usage of offsetof");

    /// Bridge struct: POD-only, transferred to Nautilus via val<IndexPhaseResultBuffer>
    using FIFType = typename FormatterType::FieldIndexFunctionType;
    struct IndexPhaseResultBuffer
    {
        /// Group pointers together (8 bytes each, naturally aligned)
        int8_t* leadingDataPtr;
        FIFType* leadingFIFPtr;
        int8_t* trailingDataPtr;
        FIFType* trailingFIFPtr;
        FIFType* rawBufferFIFPtr;
        /// Group uint64_t together (8 bytes each)
        uint64_t leadingStartSequence;
        uint64_t trailingEndSequence;
        uint64_t numberOfTuples;
        /// Group bools together (1 byte each, packed at end)
        bool hasLeading;
        bool hasTrailing;
        bool leadingBufferHasSingleDelimiter;
        bool hasTupleDelimiter;
        bool currentBufferHasSingleDelimiter;
        bool isRepeat;
    };

    static thread_local IndexPhaseResult tlIndexPhaseResult;

    /// Populates the bridge struct from the thread-local IndexPhaseResult
    static void loadIndexPhaseResultProxy(IndexPhaseResultBuffer* dst)
    {
        dst->hasLeading = tlIndexPhaseResult.hasLeadingSpanningTupleBool;
        dst->leadingStartSequence = tlIndexPhaseResult.leadingSpanningTupleStartSequence;
        dst->leadingBufferHasSingleDelimiter = tlIndexPhaseResult.startBufferHasSingleDelimiter;
        dst->leadingDataPtr = std::bit_cast<int8_t*>(tlIndexPhaseResult.leadingSpanningTuple.data());
        dst->leadingFIFPtr = &tlIndexPhaseResult.leadingSpanningTupleFIF;

        dst->hasTrailing = tlIndexPhaseResult.hasTrailingSpanningTupleBool;
        dst->trailingEndSequence = tlIndexPhaseResult.trailingSpanningTupleEndSequence;
        dst->trailingDataPtr = std::bit_cast<int8_t*>(tlIndexPhaseResult.trailingSpanningTuple.data());
        dst->trailingFIFPtr = &tlIndexPhaseResult.trailingSpanningTupleFIF;

        dst->hasTupleDelimiter = tlIndexPhaseResult.hasTupleDelimiter;
        dst->currentBufferHasSingleDelimiter = tlIndexPhaseResult.currentBufferHasSingleDelimiter;
        dst->numberOfTuples = tlIndexPhaseResult.numberOfTuples;
        dst->rawBufferFIFPtr = &tlIndexPhaseResult.rawBufferFIF;

        dst->isRepeat = tlIndexPhaseResult.isRepeat;
    }

    /// Phase 1: Index raw buffer and resolve leading spanning tuple.
    /// Must complete before trailing resolution so other threads can see this buffer's state.
    static IndexPhaseResult* indexLeadingAndBufferProxy(const TupleBuffer* tupleBuffer, InputFormatter* inputFormatter, Arena* arenaRef)
    {
        NES_DEBUG("indexLeadingAndBufferProxy enter seq={}", tupleBuffer->getSequenceRange());
        tlIndexPhaseResult = IndexPhaseResult{};
        NES_DEBUG("indexLeadingAndBufferProxy indexing raw buffer");
        inputFormatter->inputFormatIndexer.indexRawBuffer(
            tlIndexPhaseResult.rawBufferFIF, RawTupleBuffer{*tupleBuffer}, inputFormatter->indexerMetaData);

        const auto offsetOfFirstTupleDelimiter = tlIndexPhaseResult.rawBufferFIF.getByteOffsetOfFirstTuple();
        const auto offsetOfLastTupleDelimiter = tlIndexPhaseResult.rawBufferFIF.getByteOffsetOfLastTuple();
        tlIndexPhaseResult.hasValidOffsetOfTrailingSpanningTuple = offsetOfLastTupleDelimiter != std::numeric_limits<FieldIndex>::max();
        tlIndexPhaseResult.currentBufferHasSingleDelimiter = offsetOfFirstTupleDelimiter == offsetOfLastTupleDelimiter;
        tlIndexPhaseResult.hasTupleDelimiter = offsetOfFirstTupleDelimiter < tupleBuffer->getBufferSize();

        NES_DEBUG("indexLeadingAndBufferProxy hasTupleDelimiter={} firstOff={} lastOff={}",
            tlIndexPhaseResult.hasTupleDelimiter, offsetOfFirstTupleDelimiter, offsetOfLastTupleDelimiter);
        if (tlIndexPhaseResult.hasTupleDelimiter)
        {
            NES_DEBUG("indexLeadingAndBufferProxy calling findLeadingSpanningTupleWithDelimiter");
            const auto [isInRange, stagedBuffers] = inputFormatter->sequenceShredder->findLeadingSpanningTupleWithDelimiter(
                StagedBuffer{RawTupleBuffer{*tupleBuffer}, offsetOfFirstTupleDelimiter, offsetOfLastTupleDelimiter});
            NES_DEBUG("indexLeadingAndBufferProxy findLeading returned isInRange={} size={}", isInRange, stagedBuffers.getSize());
            if (not isInRange)
            {
                tlIndexPhaseResult.isRepeat = true;
                return &tlIndexPhaseResult;
            }
            if (stagedBuffers.getSize() >= 2)
            {
                constructAndIndexLeadingSpanningTuple(stagedBuffers.getSpanningBuffers(), *inputFormatter, *arenaRef);
            }
        }
        else
        {
            const auto [isInRange, stagedBuffers] = inputFormatter->sequenceShredder->findSpanningTupleWithoutDelimiter(
                StagedBuffer{RawTupleBuffer{*tupleBuffer}, offsetOfFirstTupleDelimiter, offsetOfFirstTupleDelimiter});
            if (not isInRange)
            {
                tlIndexPhaseResult.isRepeat = true;
                return &tlIndexPhaseResult;
            }
            if (stagedBuffers.getSize() >= 3)
            {
                constructAndIndexLeadingAndTrailingSpanningTuple(stagedBuffers.getSpanningBuffers(), *inputFormatter, *arenaRef);
            }
        }
        return &tlIndexPhaseResult;
    }

    /// Phase 2: Resolve trailing spanning tuple and finalize tuple count.
    static bool indexTrailingSpanningTupleProxy(const TupleBuffer* tupleBuffer, const InputFormatter* inputFormatter, Arena* arenaRef)
    {
        if (tlIndexPhaseResult.isRepeat)
        {
            return false;
        }

        if (tlIndexPhaseResult.hasTupleDelimiter)
        {
            const auto offsetOfLastTupleDelimiter = tlIndexPhaseResult.rawBufferFIF.getByteOffsetOfLastTuple();
            if (offsetOfLastTupleDelimiter != std::numeric_limits<uint32_t>::max())
            {
                auto stagedBuffers = (not tlIndexPhaseResult.hasValidOffsetOfTrailingSpanningTuple)
                    ? inputFormatter->sequenceShredder->findTrailingSpanningTupleWithDelimiter(
                          SequenceNumber::fromRange(tupleBuffer->getSequenceRange()), offsetOfLastTupleDelimiter)
                    : inputFormatter->sequenceShredder->findTrailingSpanningTupleWithDelimiter(
                          SequenceNumber::fromRange(tupleBuffer->getSequenceRange()));
                if (stagedBuffers.getSize() >= 2)
                {
                    constructAndIndexTrailingSpanningTuple(
                        stagedBuffers.getSpanningBuffers(), *inputFormatter, *arenaRef);
                }
            }
        }

        /// Compute total number of emittable tuples
        const uint64_t owningTuples = tlIndexPhaseResult.currentBufferHasSingleDelimiter
            ? 0
            : tlIndexPhaseResult.rawBufferFIF.getTotalNumberOfTuples();
        tlIndexPhaseResult.numberOfTuples = owningTuples
            + static_cast<uint64_t>(tlIndexPhaseResult.hasLeadingSpanningTupleBool)
            + static_cast<uint64_t>(tlIndexPhaseResult.hasTrailingSpanningTupleBool);
        return true;
    }

    /// --- Spanning tuple construction helpers ---

    static void constructAndIndexLeadingSpanningTuple(
        const std::vector<StagedBuffer>& leadingSpanningTupleBuffers, InputFormatter& inputFormatter, Arena& arenaRef)
    {
        const auto size = calculateSizeOfSpanningTuple(
            leadingSpanningTupleBuffers, inputFormatter.indexerMetaData.getTupleDelimitingBytes().size());
        tlIndexPhaseResult.leadingSpanningTupleStartSequence
            = SequenceNumber::fromRange(leadingSpanningTupleBuffers.front().getRawTupleBuffer().getSequenceRange()).rawValue;
        auto& leadingBuffer = leadingSpanningTupleBuffers.front();
        tlIndexPhaseResult.startBufferHasSingleDelimiter
            = leadingBuffer.getOffsetOfLastTuple() == leadingBuffer.getByteOffsetOfLastTuple();

        tlIndexPhaseResult.hasLeadingSpanningTupleBool = true;
        auto byteSpan = arenaRef.allocateMemory(size);
        tlIndexPhaseResult.leadingSpanningTuple = std::span<char>(std::bit_cast<char*>(byteSpan.data()), byteSpan.size());
        processSpanningTuple<typename FormatterType::IndexerMetaData>(
            leadingSpanningTupleBuffers, tlIndexPhaseResult.leadingSpanningTuple, inputFormatter.indexerMetaData);
        inputFormatter.inputFormatIndexer.indexRawBuffer(
            tlIndexPhaseResult.leadingSpanningTupleFIF,
            RawTupleBuffer{
                std::bit_cast<const char*>(tlIndexPhaseResult.leadingSpanningTuple.data()),
                tlIndexPhaseResult.leadingSpanningTuple.size()},
            inputFormatter.indexerMetaData);
    }

    static void constructAndIndexTrailingSpanningTuple(
        const std::vector<StagedBuffer>& stagedBuffers, const InputFormatter& inputFormatter, Arena& arenaRef)
    {
        const auto size = calculateSizeOfSpanningTuple(stagedBuffers, inputFormatter.indexerMetaData.getTupleDelimitingBytes().size());
        tlIndexPhaseResult.trailingSpanningTupleEndSequence
            = SequenceNumber::fromRange(stagedBuffers.back().getRawTupleBuffer().getSequenceRange()).rawValue;

        tlIndexPhaseResult.hasTrailingSpanningTupleBool = true;
        auto byteSpan = arenaRef.allocateMemory(size);
        tlIndexPhaseResult.trailingSpanningTuple = std::span<char>(std::bit_cast<char*>(byteSpan.data()), byteSpan.size());
        const auto trailingSpanningTupleBuffers = std::span(stagedBuffers).subspan(0, stagedBuffers.size());
        processSpanningTuple<typename FormatterType::IndexerMetaData>(
            trailingSpanningTupleBuffers, tlIndexPhaseResult.trailingSpanningTuple, inputFormatter.indexerMetaData);
        inputFormatter.inputFormatIndexer.indexRawBuffer(
            tlIndexPhaseResult.trailingSpanningTupleFIF,
            RawTupleBuffer{
                std::bit_cast<const char*>(tlIndexPhaseResult.trailingSpanningTuple.data()),
                tlIndexPhaseResult.trailingSpanningTuple.size()},
            inputFormatter.indexerMetaData);
    }

    static void constructAndIndexLeadingAndTrailingSpanningTuple(
        const std::vector<StagedBuffer>& leadingSpanningTupleBuffers, InputFormatter& inputFormatter, Arena& arenaRef)
    {
        const auto size = calculateSizeOfSpanningTuple(
            leadingSpanningTupleBuffers, inputFormatter.indexerMetaData.getTupleDelimitingBytes().size());
        tlIndexPhaseResult.leadingSpanningTupleStartSequence
            = SequenceNumber::fromRange(leadingSpanningTupleBuffers.front().getRawTupleBuffer().getSequenceRange()).rawValue;
        tlIndexPhaseResult.trailingSpanningTupleEndSequence
            = SequenceNumber::fromRange(leadingSpanningTupleBuffers.back().getRawTupleBuffer().getSequenceRange()).rawValue;
        auto& leadingBuffer = leadingSpanningTupleBuffers.front();
        tlIndexPhaseResult.startBufferHasSingleDelimiter
            = leadingBuffer.getOffsetOfLastTuple() == leadingBuffer.getByteOffsetOfLastTuple();

        tlIndexPhaseResult.hasLeadingSpanningTupleBool = true;
        tlIndexPhaseResult.hasTrailingSpanningTupleBool = true;
        auto byteSpan = arenaRef.allocateMemory(size);
        tlIndexPhaseResult.leadingSpanningTuple = std::span<char>(std::bit_cast<char*>(byteSpan.data()), byteSpan.size());
        tlIndexPhaseResult.trailingSpanningTuple = std::span<char>(std::bit_cast<char*>(byteSpan.data()), byteSpan.size());
        processSpanningTuple<typename FormatterType::IndexerMetaData>(
            leadingSpanningTupleBuffers, tlIndexPhaseResult.leadingSpanningTuple, inputFormatter.indexerMetaData);
        inputFormatter.inputFormatIndexer.indexRawBuffer(
            tlIndexPhaseResult.leadingSpanningTupleFIF,
            RawTupleBuffer{
                std::bit_cast<const char*>(tlIndexPhaseResult.leadingSpanningTuple.data()),
                tlIndexPhaseResult.leadingSpanningTuple.size()},
            inputFormatter.indexerMetaData);
    }

    static size_t calculateSizeOfSpanningTuple(const std::span<const StagedBuffer> spanningTupleBuffers, const size_t sizeOfTupleDelimitingBytes)
    {
        size_t sizeOfSpanningTuple = 0;
        sizeOfSpanningTuple += (2 * sizeOfTupleDelimitingBytes);
        sizeOfSpanningTuple += spanningTupleBuffers.front().getTrailingBytes(sizeOfTupleDelimitingBytes).size();
        for (size_t i = 1; i < spanningTupleBuffers.size() - 1; ++i)
        {
            sizeOfSpanningTuple += spanningTupleBuffers[i].getSizeOfBufferInBytes();
        }
        sizeOfSpanningTuple += spanningTupleBuffers.back().getLeadingBytes().size();
        return sizeOfSpanningTuple;
    }

    /// --- Parse methods (Phase 2) ---

    void parseLeadingRecord(
        ExecutionContext& executionCtx,
        const std::function<void(ExecutionContext& executionCtx, Record& record)>& executeChild,
        nautilus::val<IndexPhaseResultBuffer>& ipr) const
    {
        if (ipr.get(&IndexPhaseResultBuffer::hasLeading))
        {
            nautilus::val<FIFType*> leadingFIF = ipr.get(&IndexPhaseResultBuffer::leadingFIFPtr);
            nautilus::val<int8_t*> spanningRecordPtr = ipr.get(&IndexPhaseResultBuffer::leadingDataPtr);

            auto record = typename FormatterType::FieldIndexFunctionType{}.readSpanningRecord(
                projections, spanningRecordPtr, nautilus::val<uint64_t>(0), indexerMetaData, leadingFIF);
            executeChild(executionCtx, record);
        }
    }

    void parseRecordsInRawBuffer(
        ExecutionContext& executionCtx,
        const RecordBuffer& recordBuffer,
        const std::function<void(ExecutionContext& executionCtx, Record& record)>& executeChild,
        nautilus::val<IndexPhaseResultBuffer>& ipr) const
    {
        nautilus::val<uint64_t> bufferRecordIdx = 0;
        nautilus::val<FIFType*> rawFieldAccessFunction = ipr.get(&IndexPhaseResultBuffer::rawBufferFIFPtr);
        while (typename FormatterType::FieldIndexFunctionType{}.hasNext(bufferRecordIdx, rawFieldAccessFunction))
        {
            auto record = typename FormatterType::FieldIndexFunctionType{}.readSpanningRecord(
                projections, recordBuffer.getMemArea(), bufferRecordIdx, indexerMetaData, rawFieldAccessFunction);
            executeChild(executionCtx, record);
            bufferRecordIdx += 1;
        }
    }

    void parseTrailingRecord(
        ExecutionContext& executionCtx,
        const std::function<void(ExecutionContext& executionCtx, Record& record)>& executeChild,
        nautilus::val<IndexPhaseResultBuffer>& ipr) const
    {
        if (ipr.get(&IndexPhaseResultBuffer::hasTrailing))
        {
            nautilus::val<FIFType*> trailingFIF = ipr.get(&IndexPhaseResultBuffer::trailingFIFPtr);
            nautilus::val<int8_t*> spanningRecordPtr = ipr.get(&IndexPhaseResultBuffer::trailingDataPtr);

            auto record = typename FormatterType::FieldIndexFunctionType{}.readSpanningRecord(
                projections, spanningRecordPtr, nautilus::val<uint64_t>(0), indexerMetaData, trailingFIF);
            executeChild(executionCtx, record);
        }
    }
};

template <InputFormatIndexerType T>
thread_local typename InputFormatter<T>::IndexPhaseResult InputFormatter<T>::tlIndexPhaseResult{};

}
