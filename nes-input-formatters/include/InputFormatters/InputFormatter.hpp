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
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
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
#include <LockingSequenceShredder.hpp>
#include <RawTupleBuffer.hpp>
#include <SequenceShredder.hpp>
#include <val.hpp>
#include <val_arith.hpp>
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
template <InputFormatIndexerType FormatterType, typename ShredderType = SequenceShredder>
class InputFormatter
{
public:
    static constexpr bool isSequential() { return FormatterType::IsSequential; }

    explicit InputFormatter(
        FormatterType inputFormatIndexer, std::shared_ptr<TupleBufferRef> memoryProvider, const InputFormatterDescriptor& config)
        : inputFormatIndexer(std::move(inputFormatIndexer))
        , indexerMetaData(typename FormatterType::IndexerMetaData{config, *memoryProvider})
        , projections(memoryProvider->getAllFieldNames())
        , memoryProvider(std::move(memoryProvider))
    {
        if constexpr (not isSequential())
        {
            sequenceShredder = std::make_unique<ShredderType>(indexerMetaData.getTupleDelimitingBytes().size());
        }
        parserTypes[DataType::Type::BOOLEAN] = config.getFromConfig(InputFormatterDescriptor::BOOL_PARSER);
        parserTypes[DataType::Type::CHAR] = config.getFromConfig(InputFormatterDescriptor::CHAR_PARSER);
        parserTypes[DataType::Type::FLOAT32] = config.getFromConfig(InputFormatterDescriptor::F32_PARSER);
        parserTypes[DataType::Type::FLOAT64] = config.getFromConfig(InputFormatterDescriptor::F64_PARSER);
        parserTypes[DataType::Type::INT8] = config.getFromConfig(InputFormatterDescriptor::INT8_PARSER);
        parserTypes[DataType::Type::INT16] = config.getFromConfig(InputFormatterDescriptor::INT16_PARSER);
        parserTypes[DataType::Type::INT32] = config.getFromConfig(InputFormatterDescriptor::INT32_PARSER);
        parserTypes[DataType::Type::INT64] = config.getFromConfig(InputFormatterDescriptor::INT64_PARSER);
        parserTypes[DataType::Type::UINT8] = config.getFromConfig(InputFormatterDescriptor::UINT8_PARSER);
        parserTypes[DataType::Type::UINT16] = config.getFromConfig(InputFormatterDescriptor::UINT16_PARSER);
        parserTypes[DataType::Type::UINT32] = config.getFromConfig(InputFormatterDescriptor::UINT32_PARSER);
        parserTypes[DataType::Type::UINT64] = config.getFromConfig(InputFormatterDescriptor::UINT64_PARSER);
        /// Setting these even though they will probably not be used
        parserTypes[DataType::Type::VARSIZED] = "";
        parserTypes[DataType::Type::UNDEFINED] = "";
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
    requires FormatterType::IsSequential
    {
        invoke(
            indexSequentialBufferProxy,
            recordBuffer.getReference(),
            nautilus::val<InputFormatter*>(const_cast<InputFormatter*>(this)),
            arenaRef.getArena());
        return {true}; /// sequential mode never repeats
    }

    [[nodiscard]] nautilus::val<bool> indexBuffer(const RecordBuffer& recordBuffer, const ArenaRef& arenaRef) const
    requires(not FormatterType::IsSequential)
    {
        /// index raw tuple buffer, resolve and index spanning tuples(SequenceShredder) and return pointers to resolved spanning tuples, if exist
        const auto tlIndexPhaseResultNautilusVal = std::make_unique<nautilus::val<IndexPhaseResult*>>(invoke(
            indexLeadingSpanningTupleAndBufferProxy,
            recordBuffer.getReference(),
            nautilus::val<const InputFormatter*>(this),
            arenaRef.getArena()));

        if (/* isRepeat */ *getMemberWithOffset<bool>(*tlIndexPhaseResultNautilusVal, offsetof(IndexPhaseResult, isRepeat)))
        {
            return {false};
        }

        return {true};
    }

    /// Executes the second phase, which iterates over a (raw) buffer, reading specific records and fields from a (raw) buffer
    /// Relies on the index created in the first phase (indexBuffer), which it accesses through the static_thread local member
    void readBuffer(
        ExecutionContext& executionCtx,
        const RecordBuffer& recordBuffer,
        const std::function<void(ExecutionContext& executionCtx, Record& record)>& executeChild) const
    requires(not FormatterType::IsSequential)
    {
        /// @Note: the order below is important
        const nautilus::val<IndexPhaseResult*> indexPhaseResult = nautilus::invoke(getIndexPhaseResultProxy);

        parseLeadingRecord(executionCtx, executeChild, indexPhaseResult);

        if (not(nautilus::val<bool>(*getMemberWithOffset<bool>(indexPhaseResult, offsetof(IndexPhaseResult, hasTupleDelimiter)))))
        {
            return;
        }

        parseRecordsInRawBuffer(executionCtx, recordBuffer, executeChild, indexPhaseResult);

        parseTrailingRecord(executionCtx, recordBuffer, executeChild, indexPhaseResult);
    }

    /// Sequential readBuffer: no trailing spanning tuple resolution — truncated bytes already reported in indexSequentialBufferProxy.
    void readBuffer(
        ExecutionContext& executionCtx,
        const RecordBuffer& recordBuffer,
        const std::function<void(ExecutionContext& executionCtx, Record& record)>& executeChild) const
    requires FormatterType::IsSequential
    {
        const nautilus::val<IndexPhaseResult*> indexPhaseResult = nautilus::invoke(getIndexPhaseResultProxy);

        parseLeadingRecord(executionCtx, executeChild, indexPhaseResult);

        if (not(nautilus::val<bool>(*getMemberWithOffset<bool>(indexPhaseResult, offsetof(IndexPhaseResult, hasTupleDelimiter)))))
        {
            return;
        }

        parseRecordsInRawBuffer(executionCtx, recordBuffer, executeChild, indexPhaseResult);
    }

    std::ostream& toString(std::ostream& os) const
    {
        os << "InputFormatter(inputFormatIndexer: " << inputFormatIndexer;
        if constexpr (not isSequential())
        {
            os << ", sequenceShredder: " << *sequenceShredder;
        }
        os << ")\n";
        return os;
    }

private:
    FormatterType inputFormatIndexer;
    typename FormatterType::IndexerMetaData indexerMetaData;
    std::vector<Record::RecordFieldIdentifier> projections;
    std::shared_ptr<TupleBufferRef> memoryProvider;

    struct Empty
    {
    };

    /// Concurrent mode only: spanning tuple resolution across threads (SequenceShredder or LockingSequenceShredder)
    [[no_unique_address]] std::conditional_t<not isSequential(), std::unique_ptr<ShredderType>, Empty> sequenceShredder;

    /// Sequential mode only: accumulates delimiter-less buffers that are part of a multi-buffer spanning tuple.
    /// Stores (TupleBuffer, byteCount) pairs — byteCount is saved before setNumberOfTuples(0) since TupleBuffer is ref-counted.
    [[no_unique_address]] std::conditional_t<isSequential(), std::vector<std::pair<TupleBuffer, size_t>>, Empty>
        accumulatedSpanningTupleBuffers;
    std::unordered_map<DataType::Type, std::string> parserTypes;

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
            const std::vector<StagedBuffer>& stagedBuffers,
            const InputFormatter& inputFormatter,
            Arena& arenaRef,
            const size_t sizeOfTrailingSpanningTuple)
        {
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
            if (sizeOfLeadingSpanningTuple <= inputFormatter.indexerMetaData.getTupleDelimitingBytes().size() * 2)
            {
                return;
            }
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

        /// Sequential mode: assembles a leading spanning tuple from accumulated buffers + leading bytes of the current buffer.
        static void constructAndIndexSequentialLeadingSpanningTuple(
            std::vector<std::pair<TupleBuffer, size_t>>& accumulatedBuffers,
            const std::string_view leadingBytes,
            InputFormatter& inputFormatter,
            Arena& arenaRef)
        {
            const auto delimiterBytes = inputFormatter.indexerMetaData.getTupleDelimitingBytes();

            /// Compute total size: delimiter + accumulated_data + leading_bytes + delimiter
            size_t totalSize = 2 * delimiterBytes.size() + leadingBytes.size();
            for (const auto& [buffer, byteCount] : accumulatedBuffers)
            {
                totalSize += byteCount;
            }

            /// Arena-allocate and copy
            allocateForLeadingSpanningTuple(arenaRef, totalSize);
            auto dest = tlIndexPhaseResult.leadingSpanningTuple;
            size_t offset = 0;

            auto copyTo = [&dest, &offset](const std::string_view src)
            {
                std::ranges::copy(src, dest.subspan(offset, src.size()).begin());
                offset += src.size();
            };

            copyTo(delimiterBytes);
            for (const auto& [buffer, byteCount] : accumulatedBuffers)
            {
                copyTo(std::string_view(buffer.getAvailableMemoryArea<char>().data(), byteCount));
            }
            copyTo(leadingBytes);
            copyTo(delimiterBytes);

            /// Release accumulated buffers
            accumulatedBuffers.clear();

            /// Index the assembled spanning tuple
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
    };

    static IndexPhaseResult* getIndexPhaseResultProxy() { return &tlIndexPhaseResult; }

    /// Sequential mode proxy: indexes the buffer and handles accumulated spanning tuples without SequenceShredder.
    static IndexPhaseResult* indexSequentialBufferProxy(const TupleBuffer* tupleBuffer, InputFormatter* inputFormatter, Arena* arenaRef)
    {
        IndexPhaseResultBuilder::startBuildingIndex();
        const auto [offsetOfFirstTupleDelimiter, offsetOfLastTupleDelimiter, hasTupleDelimiter]
            = IndexPhaseResultBuilder::indexRawBuffer(*inputFormatter, *tupleBuffer);

        if (hasTupleDelimiter)
        {
            if (not inputFormatter->accumulatedSpanningTupleBuffers.empty())
            {
                /// Bytes before the first delimiter are the tail of the spanning tuple
                const auto leadingBytes = std::string_view(tupleBuffer->getAvailableMemoryArea<char>().data(), offsetOfFirstTupleDelimiter);
                IndexPhaseResultBuilder::constructAndIndexSequentialLeadingSpanningTuple(
                    inputFormatter->accumulatedSpanningTupleBuffers, leadingBytes, *inputFormatter, *arenaRef);
            }

            /// Report truncated trailing bytes so the outside can prepend them to the next buffer
            const auto delimiterSize = inputFormatter->indexerMetaData.getTupleDelimitingBytes().size();
            const auto truncatedByteCount = tupleBuffer->getNumberOfTuples() - (offsetOfLastTupleDelimiter + delimiterSize);
            tupleBuffer->setNumberOfTuples(truncatedByteCount);
        }
        else
        {
            /// No delimiter: accumulate entire buffer
            const auto byteCount = tupleBuffer->getNumberOfTuples();
            inputFormatter->accumulatedSpanningTupleBuffers.emplace_back(*tupleBuffer, byteCount);
            tupleBuffer->setNumberOfTuples(0);
        }
        return IndexPhaseResultBuilder::finalizeLeadingIndexPhase();
    }

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
        const auto sizeOfTrailingSpanningTuple = IndexPhaseResultBuilder::calculateSizeOfSpanningTuple(
            stagedBuffers.getSpanningBuffers(), inputFormatter->indexerMetaData.getTupleDelimitingBytes().size());
        /// If the spanning tuple consists only of delimiters, it is empty and we can skip processing it
        if (sizeOfTrailingSpanningTuple <= inputFormatter->indexerMetaData.getTupleDelimitingBytes().size() * 2)
        {
            return false;
        }
        IndexPhaseResultBuilder::constructAndIndexTrailingSpanningTuple(
            stagedBuffers.getSpanningBuffers(), *inputFormatter, *arenaRef, sizeOfTrailingSpanningTuple);
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
            auto spanningRecordPtr = *getMemberPtrWithOffset<int8_t>(indexPhaseResult, offsetof(IndexPhaseResult, leadingSpanningTuple));
            auto rawFieldAccessFunction = getMemberWithOffset<typename FormatterType::FieldIndexFunctionType>(
                indexPhaseResult, offsetof(IndexPhaseResult, leadingSpanningTupleFIF));
            /// Formats whose tuple delimiter cannot reunite into extra record boundaries inside a
            /// spanning tuple (CSV/JSON/Native with single-byte delims) opt out of the loop and
            /// use the fast path: exactly one record per spanning tuple, no hasNext branch in the
            /// traced pipeline.
            if constexpr (FormatterType::SpanningTupleMayContainMultipleRecords)
            {
                nautilus::val<uint64_t> bufferRecordIdx = 0;
                while (typename FormatterType::FieldIndexFunctionType{}.hasNext(bufferRecordIdx, rawFieldAccessFunction))
                {
                    auto record = typename FormatterType::FieldIndexFunctionType{}.readSpanningRecord(
                        projections, spanningRecordPtr, bufferRecordIdx, indexerMetaData, rawFieldAccessFunction, parserTypes);
                    executeChild(executionCtx, record);
                    ++bufferRecordIdx;
                }
            }
            else
            {
                auto record = typename FormatterType::FieldIndexFunctionType{}.readSpanningRecord(
                    projections, spanningRecordPtr, nautilus::val<uint64_t>(0), indexerMetaData, rawFieldAccessFunction, parserTypes);
                executeChild(executionCtx, record);
            }
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
                projections, recordBuffer.getMemArea(), bufferRecordIdx, indexerMetaData, rawFieldAccessFunction, parserTypes);
            executeChild(executionCtx, record);
            ++bufferRecordIdx;
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
            auto spanningRecordPtr = *getMemberPtrWithOffset<int8_t>(indexPhaseResult, offsetof(IndexPhaseResult, trailingSpanningTuple));
            auto rawFieldAccessFunction = getMemberWithOffset<typename FormatterType::FieldIndexFunctionType>(
                indexPhaseResult, offsetof(IndexPhaseResult, trailingSpanningTupleFIF));
            /// See parseLeadingRecord for the rationale behind this compile-time split.
            if constexpr (FormatterType::SpanningTupleMayContainMultipleRecords)
            {
                nautilus::val<uint64_t> bufferRecordIdx = 0;
                while (typename FormatterType::FieldIndexFunctionType{}.hasNext(bufferRecordIdx, rawFieldAccessFunction))
                {
                    auto record = typename FormatterType::FieldIndexFunctionType{}.readSpanningRecord(
                        projections, spanningRecordPtr, bufferRecordIdx, indexerMetaData, rawFieldAccessFunction, parserTypes);
                    executeChild(executionCtx, record);
                    ++bufferRecordIdx;
                }
            }
            else
            {
                auto record = typename FormatterType::FieldIndexFunctionType{}.readSpanningRecord(
                    projections, spanningRecordPtr, nautilus::val<uint64_t>(0), indexerMetaData, rawFieldAccessFunction, parserTypes);
                executeChild(executionCtx, record);
            }
        }
    }
};

/// Necessary thread_local variable definitions to make static thread_local variables usable in the InputFormatter class
template <InputFormatIndexerType T, typename S>
thread_local typename InputFormatter<T, S>::IndexPhaseResult InputFormatter<T, S>::tlIndexPhaseResult{};

}
