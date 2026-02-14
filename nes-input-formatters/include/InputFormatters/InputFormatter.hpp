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

#include <Nautilus/DataTypes/DataTypesUtil.hpp>
#include <Nautilus/Interface/BufferRef/TupleBufferRef.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Arena.hpp>
#include <Concepts.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <RawTupleBuffer.hpp>
#include <SequenceShredder.hpp>
#include <val.hpp>
#include <val_concepts.hpp>

namespace NES
{
/// The type that all formatters use to represent indexes to fields.
using FieldIndex = uint32_t;

/// Constructs a spanning tuple (string) that spans over at least two buffers (stagedBuffersSpan).
/// First, determines the start of the spanning tuple in the first buffer to format. Constructs a spanning tuple from the required bytes.
/// Second, appends all bytes of all raw buffers that are not the last buffer to the spanning tuple.
/// Third, determines the end of the spanning tuple in the last buffer to format. Appends the required bytes to the spanning tuple.
/// Lastly, formats the full spanning tuple.
template <IndexerMetaDataType IndexerMetaData>
void processSpanningTuple(
    const std::span<const StagedBuffer>& stagedBuffersSpan, std::span<char> spanningTupleBuffer, const IndexerMetaData& indexerMetaData)
{
    /// If the buffers are not empty, there are at least three buffers
    INVARIANT(stagedBuffersSpan.size() >= 2, "A spanning tuple must span across at least two buffers");

    size_t offset = 0;
    auto boundsCheckedCopy = [&spanningTupleBuffer, &offset](std::string_view source)
    {
        INVARIANT(offset + source.size() <= spanningTupleBuffer.size(), "Buffer overflow: insufficient space in spanning tuple buffer");

        auto destSubspan = spanningTupleBuffer.subspan(offset, source.size());
        std::ranges::copy(source, destSubspan.begin());
        offset += source.size();
    };

    /// Wrap the spanning tuple in delimiter bytes (if exist) and copy the trailing bytes of the first buffer into the spanning tuple
    boundsCheckedCopy(indexerMetaData.getTupleDelimitingBytes());
    boundsCheckedCopy(stagedBuffersSpan.front().getTrailingBytes(indexerMetaData.getTupleDelimitingBytes().size()));

    /// Copy all complete buffers between the first and the last into the spanning tuple
    for (const auto& buffer : stagedBuffersSpan.subspan(1, stagedBuffersSpan.size() - 2))
    {
        boundsCheckedCopy(buffer.getBufferView());
    }

    /// Copy the leading bytes of the last buffer into the spanning tuple and wrap the spanning tuple with delimiting bytes (if exist)
    boundsCheckedCopy(stagedBuffersSpan.back().getLeadingBytes());
    boundsCheckedCopy(indexerMetaData.getTupleDelimitingBytes());
}

/// InputFormatters concurrently take (potentially) raw input buffers and format all full tuples in these raw input buffers that the
/// individual InputFormatters see during execution.
/// The only point of synchronization is a call to the SequenceShredder data structure, which determines which buffers the InputFormatter
/// needs to process (resolving tuples that span multiple raw buffers).
/// An InputFormatter belongs to exactly one source. The source reads raw data into buffers, constructs a Task from the
/// raw buffer and its successor (the InputFormatter) and writes it to the task queue of the QueryEngine.
/// The QueryEngine concurrently executes InputFormatters. Thus, even if the source writes the InputFormatters to the task queue sequentially,
/// the QueryEngine may still execute them in any order.
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

    /// Executes the first phase, which indexes a (raw) buffer enabling the second phase, which calls 'readBuffer()' to index specific
    /// records/fields within the (raw) buffer. Relies on static thread_local member variables to 'bridge' the result of the indexing phase
    /// to the second phase, which uses the index to access specific records/fields
    [[nodiscard]] nautilus::val<bool> indexBuffer(const RecordBuffer& recordBuffer, const ArenaRef& arenaRef) const
    {
        /// index raw tuple buffer, resolve and index spanning tuples(SequenceShredder) and return pointers to resolved spanning tuples, if exist
        const auto tlIndexPhaseResultNautilusVal = std::make_unique<nautilus::val<IndexPhaseResult*>>(invoke(
            indexLeadingSpanningTupleAndBufferProxy,
            recordBuffer.getReference(),
            nautilus::val<const InputFormatter*>(this),
            arenaRef.getArena()));
        nautilus::val<bool> isRepeat = *getMemberWithOffset<bool>(*tlIndexPhaseResultNautilusVal, offsetof(IndexPhaseResult, isRepeat));
        return !isRepeat;
    }

    /// Executes the second phase, which iterates over a (raw) buffer, reading specific records and fields from a (raw) buffer
    /// Relies on the index created in the first phase (indexBuffer), which it accesses through the static_thread local member
    void readBuffer(
        ExecutionContext& executionCtx,
        const RecordBuffer& recordBuffer,
        const std::function<void(ExecutionContext& executionCtx, Record& record)>& executeChild) const
    {
        /// @Note: the order below is important
        const nautilus::val<IndexPhaseResult*> indexPhaseResult = nautilus::invoke(getIndexPhaseResultProxy);

        /// a buffer that only contains data from a single tuple may connect two buffers that delimit tuples
        /// we count such a spanning tuple as a leading spanning tuple
        /// a buffer that delimits tuples may form a leading (and a trailing) spanning tuple
        parseLeadingRecord(executionCtx, executeChild, indexPhaseResult);

        /// check if the buffer only contains data from a single tuple (does not delimit two tuples)
        /// such a buffer can only form one (leading) spanning tuple, so returning is safe
        if (nautilus::val<bool>(*getMemberWithOffset<bool>(indexPhaseResult, offsetof(IndexPhaseResult, hasTupleDelimiter))))
        {
            /// a buffer that delimits tuples may contain multiple complete tuples
            /// determining the offset of a tuple may require parsing the prior tuple
            parseRecordsInRawBuffer(executionCtx, recordBuffer, executeChild, indexPhaseResult);

            /// a buffer that delimits tuples usually forms a spanning tuple that continues in the next buffer
            /// determining the offset of the start of that tuple may require parsing all prior records in the raw buffer
            parseTrailingRecord(executionCtx, recordBuffer, executeChild, indexPhaseResult);
        }
    }

    std::ostream& toString(std::ostream& os) const
    {
        /// Not using fmt::format, because it fails during build, trying to pass sequenceShredder as a const value
        os << "InputFormatter(" << ", inputFormatIndexer: " << inputFormatIndexer << ", sequenceShredder: " << *sequenceShredder << ")\n";
        return os;
    }

private:
    FormatterType inputFormatIndexer;
    typename FormatterType::IndexerMetaData indexerMetaData;
    std::vector<Record::RecordFieldIdentifier> projections;
    std::shared_ptr<TupleBufferRef> memoryProvider;
    std::unique_ptr<SequenceShredder> sequenceShredder;

    struct IndexPhaseResult
    {
        std::span<char> leadingSpanningTuple;
        std::span<char> trailingSpanningTuple;
        bool hasLeadingSpanningTupleBool = false;
        bool hasTupleDelimiter = false;
        bool isRepeat = false;
        bool hasValidOffsetOfTrailingSpanningTuple = false;
        uint64_t numTuplesWithoutTrailing = 0;

        typename FormatterType::FieldIndexFunctionType leadingSpanningTupleFIF;
        typename FormatterType::FieldIndexFunctionType trailingSpanningTupleFIF;
        typename FormatterType::FieldIndexFunctionType rawBufferFIF;
    };

    static_assert(std::is_standard_layout_v<IndexPhaseResult>, "IndexPhaseResult must have a standard layout for safe usage of offsetof");

    /// We need a thread_local variable to 'bridge' state between the indexing (open) and the parsing (readBuffer) phase.
    /// They must be thread_local, since multiple threads use them at the same time.
    /// 'thread_local' means that every thread creates its own 'local' static version.
    static thread_local IndexPhaseResult tlIndexPhaseResult;

    static nautilus::val<IndexPhaseResult*> getIndexPhaseResult() { return nautilus::val<IndexPhaseResult*>(&tlIndexPhaseResult); }

    /// Builds an index during the indexing phase (open()), which the parsing phase (readBuffer()) requires for accessing specific fields.
    /// Does not have state itself, but wraps the static thread_local 'tlIndexPhaseResult', which it builds during its lifetime.
    class IndexPhaseResultBuilder
    {
    public:
        IndexPhaseResultBuilder() = delete;

        static void startBuildingIndex() { tlIndexPhaseResult = IndexPhaseResult{}; }

        struct IndexResult
        {
            FieldIndex offsetOfFirstTupleDelimiter;
            FieldIndex offsetOfLastTupleDelimiter;
            bool hasTupleDelimiter;
        };

        static IndexResult indexRawBuffer(InputFormatter& inputFormatter, const TupleBuffer& tupleBuffer)
        {
            inputFormatter.inputFormatIndexer.indexRawBuffer(
                tlIndexPhaseResult.rawBufferFIF, RawTupleBuffer{tupleBuffer}, inputFormatter.indexerMetaData);
            tlIndexPhaseResult.numTuplesWithoutTrailing = tlIndexPhaseResult.rawBufferFIF.getTotalNumberOfTuples();

            const auto offsetOfFirstTupleDelimiter = tlIndexPhaseResult.rawBufferFIF.getByteOffsetOfFirstTuple();
            const auto offsetOfLastTupleDelimiter = tlIndexPhaseResult.rawBufferFIF.getByteOffsetOfLastTuple();
            tlIndexPhaseResult.hasValidOffsetOfTrailingSpanningTuple = offsetOfLastTupleDelimiter != std::numeric_limits<FieldIndex>::max();

            tlIndexPhaseResult.hasTupleDelimiter = offsetOfFirstTupleDelimiter < tupleBuffer.getBufferSize();
            return {
                .offsetOfFirstTupleDelimiter = offsetOfFirstTupleDelimiter,
                .offsetOfLastTupleDelimiter = offsetOfLastTupleDelimiter,
                .hasTupleDelimiter = tlIndexPhaseResult.hasTupleDelimiter};
        }

        static void setIsRepeat(bool isRepeat) { tlIndexPhaseResult.isRepeat = isRepeat; }

        static IndexPhaseResult* finalizeLeadingIndexPhase()
        {
            tlIndexPhaseResult.numTuplesWithoutTrailing += static_cast<uint64_t>(tlIndexPhaseResult.hasLeadingSpanningTupleBool);
            return &tlIndexPhaseResult;
        }

        static void finalizeTrailingIndexPhase() { tlIndexPhaseResult.numTuplesWithoutTrailing += 1; }

        static void constructAndIndexTrailingSpanningTuple(
            const std::vector<StagedBuffer>& stagedBuffers, const InputFormatter& inputFormatter, Arena& arenaRef)
        {
            const auto sizeOfTrailingSpanningTuple
                = calculateSizeOfSpanningTuple(stagedBuffers, inputFormatter.indexerMetaData.getTupleDelimitingBytes().size());
            allocateForTrailingSpanningTuple(arenaRef, sizeOfTrailingSpanningTuple);
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

        static void constructAndIndexLeadingSpanningTuple(
            const std::vector<StagedBuffer>& leadingSpanningTupleBuffers, InputFormatter& inputFormatter, Arena& arenaRef)
        {
            const auto sizeOfLeadingSpanningTuple = calculateSizeOfSpanningTuple(
                leadingSpanningTupleBuffers, inputFormatter.indexerMetaData.getTupleDelimitingBytes().size());

            allocateForLeadingSpanningTuple(arenaRef, sizeOfLeadingSpanningTuple);
            processSpanningTuple<typename FormatterType::IndexerMetaData>(
                leadingSpanningTupleBuffers, tlIndexPhaseResult.leadingSpanningTuple, inputFormatter.indexerMetaData);
            inputFormatter.inputFormatIndexer.indexRawBuffer(
                tlIndexPhaseResult.leadingSpanningTupleFIF,
                RawTupleBuffer{
                    std::bit_cast<const char*>(tlIndexPhaseResult.leadingSpanningTuple.data()),
                    tlIndexPhaseResult.leadingSpanningTuple.size()},
                inputFormatter.indexerMetaData);
        }

    private:
        static void allocateForLeadingSpanningTuple(Arena& arenaRef, const size_t sizeOfLeadingSpanningTuple)
        {
            tlIndexPhaseResult.hasLeadingSpanningTupleBool = true;
            auto byteSpan = arenaRef.allocateMemory(sizeOfLeadingSpanningTuple);
            tlIndexPhaseResult.leadingSpanningTuple = std::span<char>(std::bit_cast<char*>(byteSpan.data()), byteSpan.size());
        }

        static void allocateForTrailingSpanningTuple(Arena& arenaRef, const size_t sizeOfTrailingSpanningTuple)
        {
            auto byteSpan = arenaRef.allocateMemory(sizeOfTrailingSpanningTuple);
            tlIndexPhaseResult.trailingSpanningTuple = std::span<char>(std::bit_cast<char*>(byteSpan.data()), byteSpan.size());
        }

        static size_t
        calculateSizeOfSpanningTuple(const std::span<const StagedBuffer> spanningTupleBuffers, const size_t sizeOfTupleDelimitingBytes)
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
    };

    static IndexPhaseResult* getIndexPhaseResultProxy() { return &tlIndexPhaseResult; }

    static bool indexTrailingSpanningTupleProxy(const TupleBuffer* tupleBuffer, const InputFormatter* inputFormatter, Arena* arenaRef)
    {
        /// the buffer does not have a trailing SpanningTuple, if after iterating over the entire buffer, getByteOffsetOfLastTuple is invalid
        const auto offsetOfLastTupleDelimiter = tlIndexPhaseResult.rawBufferFIF.getByteOffsetOfLastTuple();
        if (offsetOfLastTupleDelimiter == std::numeric_limits<uint64_t>::max())
        {
            return false;
        }

        /// if the offset to trailing SpanningTuple was not known after indexing we need to set it in the SequenceShredder, allowing threads to find it
        auto stagedBuffers = (not tlIndexPhaseResult.hasValidOffsetOfTrailingSpanningTuple)
            ? inputFormatter->sequenceShredder->findTrailingSpanningTupleWithDelimiter(
                  tupleBuffer->getSequenceNumber(), offsetOfLastTupleDelimiter)
            : inputFormatter->sequenceShredder->findTrailingSpanningTupleWithDelimiter(tupleBuffer->getSequenceNumber());
        /// a trailing spanning tuple must span at least 2 buffers
        if (stagedBuffers.getSize() < 2)
        {
            return false;
        }
        IndexPhaseResultBuilder::constructAndIndexTrailingSpanningTuple(stagedBuffers.getSpanningBuffers(), *inputFormatter, *arenaRef);
        IndexPhaseResultBuilder::finalizeTrailingIndexPhase();
        return true;
    }

    static IndexPhaseResult*
    indexLeadingSpanningTupleAndBufferProxy(const TupleBuffer* tupleBuffer, InputFormatter* inputFormatter, Arena* arenaRef)
    {
        IndexPhaseResultBuilder::startBuildingIndex();
        const auto [offsetOfFirstTupleDelimiter, offsetOfLastTupleDelimiter, hasTupleDelimiter]
            = IndexPhaseResultBuilder::indexRawBuffer(*inputFormatter, *tupleBuffer);

        if (hasTupleDelimiter)
        {
            const auto [isInRange, stagedBuffers] = inputFormatter->sequenceShredder->findLeadingSpanningTupleWithDelimiter(
                StagedBuffer{RawTupleBuffer{*tupleBuffer}, offsetOfFirstTupleDelimiter, offsetOfLastTupleDelimiter});
            if (not isInRange)
            {
                IndexPhaseResultBuilder::setIsRepeat(true);
                return IndexPhaseResultBuilder::finalizeLeadingIndexPhase();
            }
            if (stagedBuffers.getSize() < 2)
            {
                return IndexPhaseResultBuilder::finalizeLeadingIndexPhase();
            }
            IndexPhaseResultBuilder::constructAndIndexLeadingSpanningTuple(stagedBuffers.getSpanningBuffers(), *inputFormatter, *arenaRef);
        }
        else /* has no tuple delimiter */
        {
            const auto [isInRange, stagedBuffers] = inputFormatter->sequenceShredder->findSpanningTupleWithoutDelimiter(
                StagedBuffer{RawTupleBuffer{*tupleBuffer}, offsetOfFirstTupleDelimiter, offsetOfFirstTupleDelimiter});

            if (not isInRange)
            {
                IndexPhaseResultBuilder::setIsRepeat(true);
                return IndexPhaseResultBuilder::finalizeLeadingIndexPhase();
            }

            if (stagedBuffers.getSize() < 3)
            {
                return IndexPhaseResultBuilder::finalizeLeadingIndexPhase();
            }

            /// The buffer has no delimiter, but connects two buffers with delimiters, forming one spanning tuple
            /// We arbitrarily treat it as a 'leading' spanning tuple (technically, it is both leading and trailing)
            IndexPhaseResultBuilder::constructAndIndexLeadingSpanningTuple(stagedBuffers.getSpanningBuffers(), *inputFormatter, *arenaRef);
        }
        return IndexPhaseResultBuilder::finalizeLeadingIndexPhase();
    }

    template <typename IndexPhaseResult>
    void parseLeadingRecord(
        ExecutionContext& executionCtx,
        const std::function<void(ExecutionContext& executionCtx, Record& record)>& executeChild,
        const nautilus::val<IndexPhaseResult*>& indexPhaseResult) const
    {
        if (*getMemberWithOffset<bool>(indexPhaseResult, offsetof(IndexPhaseResult, hasLeadingSpanningTupleBool)))
        {
            auto leadingFIF = getMemberWithOffset<typename FormatterType::FieldIndexFunctionType>(
                indexPhaseResult, offsetof(IndexPhaseResult, leadingSpanningTupleFIF));

            /// Get leading field index function and a pointer to the spanning tuple 'record'
            auto spanningRecordPtr = *getMemberPtrWithOffset<int8_t>(indexPhaseResult, offsetof(IndexPhaseResult, leadingSpanningTuple));

            auto record = typename FormatterType::FieldIndexFunctionType{}.readSpanningRecord(
                projections,
                spanningRecordPtr,
                nautilus::val<uint64_t>(0),
                indexerMetaData,
                leadingFIF,
                executionCtx.pipelineMemoryProvider.arena);
            executeChild(executionCtx, record);
        }
    }

    template <typename IndexPhaseResult>
    void parseRecordsInRawBuffer(
        ExecutionContext& executionCtx,
        const RecordBuffer& recordBuffer,
        const std::function<void(ExecutionContext& executionCtx, Record& record)>& executeChild,
        const nautilus::val<IndexPhaseResult*>& indexPhaseResult) const
    {
        nautilus::val<uint64_t> bufferRecordIdx = 0;
        auto rawFieldAccessFunction = getMemberWithOffset<typename FormatterType::FieldIndexFunctionType>(
            indexPhaseResult, offsetof(IndexPhaseResult, rawBufferFIF));
        while (typename FormatterType::FieldIndexFunctionType{}.hasNext(bufferRecordIdx, rawFieldAccessFunction))
        {
            auto record = typename FormatterType::FieldIndexFunctionType{}.readSpanningRecord(
                projections,
                recordBuffer.getMemArea(),
                bufferRecordIdx,
                indexerMetaData,
                rawFieldAccessFunction,
                executionCtx.pipelineMemoryProvider.arena);
            executeChild(executionCtx, record);
            bufferRecordIdx += 1;
        }
    }

    template <typename IndexPhaseResult>
    void parseTrailingRecord(
        ExecutionContext& executionCtx,
        const RecordBuffer& recordBuffer,
        const std::function<void(ExecutionContext& executionCtx, Record& record)>& executeChild,
        const nautilus::val<IndexPhaseResult*>& indexPhaseResult) const
    {
        const nautilus::val<bool> hasTrailingSpanningTuple = invoke(
            indexTrailingSpanningTupleProxy,
            recordBuffer.getReference(),
            nautilus::val<const InputFormatter*>(this),
            executionCtx.pipelineMemoryProvider.arena.getArena());

        if (hasTrailingSpanningTuple)
        {
            auto trailingFIF = getMemberWithOffset<typename FormatterType::FieldIndexFunctionType>(
                indexPhaseResult, offsetof(IndexPhaseResult, trailingSpanningTupleFIF));

            auto spanningRecordPtr = *getMemberPtrWithOffset<int8_t>(indexPhaseResult, offsetof(IndexPhaseResult, trailingSpanningTuple));

            auto record = typename FormatterType::FieldIndexFunctionType{}.readSpanningRecord(
                projections,
                spanningRecordPtr,
                nautilus::val<uint64_t>(0),
                indexerMetaData,
                trailingFIF,
                executionCtx.pipelineMemoryProvider.arena);
            executeChild(executionCtx, record);
        }
    }
};

/// Necessary thread_local variable definitions to make static thread_local variables usable in the InputFormatter class
template <InputFormatIndexerType T>
thread_local typename InputFormatter<T>::IndexPhaseResult InputFormatter<T>::tlIndexPhaseResult{};

}
