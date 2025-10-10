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
#include <cstring>
#include <memory>
#include <ostream>
#include <ranges>
#include <span>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <DataTypes/Schema.hpp>
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
#include <FieldIndexFunction.hpp>
#include <SequenceShredder.hpp>

namespace NES
{
/// The type that all formatters use to represent indexes to fields.
using FieldIndex = uint32_t;

/// Constructs a spanning tuple (string) that spans over at least two buffers (buffersToFormat).
/// First, determines the start of the spanning tuple in the first buffer to format. Constructs a spanning tuple from the required bytes.
/// Second, appends all bytes of all raw buffers that are not the last buffer to the spanning tuple.
/// Third, determines the end of the spanning tuple in the last buffer to format. Appends the required bytes to the spanning tuple.
/// Lastly, formats the full spanning tuple.
template <IndexerMetaDataType IndexerMetaData>
void processSpanningTuple(
    const std::span<const StagedBuffer>& stagedBuffersSpan, int8_t* spanningTuplePointer, const IndexerMetaData& indexerMetaData)
{
    /// If the buffers are not empty, there are at least three buffers
    INVARIANT(stagedBuffersSpan.size() >= 2, "A spanning tuple must span across at least two buffers");
    size_t spanningTuplePtrOffset = 0;

    /// Wrap the spanning tuple in delimiter bytes (if exist) to allow the InputFormatIndexer to detect the spanning tuple
    std::memcpy(spanningTuplePointer, indexerMetaData.getTupleDelimitingBytes().data(), indexerMetaData.getTupleDelimitingBytes().size());
    spanningTuplePtrOffset += indexerMetaData.getTupleDelimitingBytes().size();

    /// The trailing bytes of the first staged buffer are the first bytes of the spanning tuple
    const auto firstSpanningTuple = stagedBuffersSpan.front().getTrailingBytes(indexerMetaData.getTupleDelimitingBytes().size());
    std::memcpy(spanningTuplePointer + spanningTuplePtrOffset, firstSpanningTuple.data(), firstSpanningTuple.size());
    spanningTuplePtrOffset += firstSpanningTuple.size();

    /// Process all buffers in-between the first and the last
    for (const auto middleBuffers = stagedBuffersSpan | std::views::drop(1) | std::views::take(stagedBuffersSpan.size() - 2);
         const auto& buffer : middleBuffers)
    {
        std::memcpy(spanningTuplePointer + spanningTuplePtrOffset, buffer.getBufferView().data(), buffer.getBufferView().size());
        spanningTuplePtrOffset += buffer.getBufferView().size();
    }

    /// The leading bytes of the final staged buffer are the last bytes fo the spanning tuple
    const auto trailingSpanningTuple = stagedBuffersSpan.back().getLeadingBytes();
    std::memcpy(spanningTuplePointer + spanningTuplePtrOffset, trailingSpanningTuple.data(), trailingSpanningTuple.size());
    spanningTuplePtrOffset += trailingSpanningTuple.size();

    /// Wrap the end of the spanning tuple in a newline to allow the InputFormatIndexer to detect the tuple
    std::memcpy(
        spanningTuplePointer + spanningTuplePtrOffset,
        indexerMetaData.getTupleDelimitingBytes().data(),
        indexerMetaData.getTupleDelimitingBytes().size());
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
        FormatterType inputFormatIndexer,
        std::shared_ptr<NES::Nautilus::Interface::BufferRef::TupleBufferRef> memoryProvider,
        const ParserConfig& parserConfig)
        : inputFormatIndexer(std::move(inputFormatIndexer))
        , indexerMetaData(typename FormatterType::IndexerMetaData{parserConfig, *memoryProvider->getMemoryLayout()})
        , projections(memoryProvider->getMemoryLayout()->getSchema().getFieldNames())
        , memoryProvider(std::move(memoryProvider))
        , sequenceShredder(std::make_unique<SequenceShredder>(parserConfig.tupleDelimiter.size()))
    {
    }

    ~InputFormatter() = default;

    InputFormatter(const InputFormatter&) = delete;
    InputFormatter& operator=(const InputFormatter&) = delete;
    InputFormatter(InputFormatter&&) = default;
    InputFormatter& operator=(InputFormatter&&) = delete;

    std::shared_ptr<MemoryLayout> getMemoryLayout() const { return memoryProvider->getMemoryLayout(); }

    /// Executes the first phase, which indexes a (raw) buffer enabling the second phase, which calls 'readRecord()' to index specific
    /// records/fields within the (raw) buffer. Relies on static thread_local member variables to 'bridge' the result of the indexing phase
    /// to the second phase, which uses the index to access specific records/fields
    Interface::BufferRef::IndexBufferResult indexBuffer(RecordBuffer& recordBuffer, ArenaRef& arenaRef)
    {
        /// index raw tuple buffer, resolve and index spanning tuples(SequenceShredder) and return pointers to resolved spanning tuples, if exist
        tlIndexPhaseResultNautilusVal = std::make_unique<nautilus::val<IndexPhaseResult*>>(nautilus::invoke(
            indexBufferProxy,
            recordBuffer.getReference(),
            nautilus::val<InputFormatter*>(this),
            nautilus::val<size_t>(memoryProvider->getMemoryLayout()->getBufferSize()),
            arenaRef.getArena()));

        tlArenaPtr = &arenaRef;
        const nautilus::val<uint64_t> totalNumberOfTuples = *Nautilus::Util::getMemberWithOffset<uint64_t>(
            *tlIndexPhaseResultNautilusVal, offsetof(IndexPhaseResult, totalNumberOfTuples));
        recordBuffer.setNumRecords(totalNumberOfTuples);

        if (/* isRepeat */ *Nautilus::Util::getMemberWithOffset<bool>(*tlIndexPhaseResultNautilusVal, offsetof(IndexPhaseResult, isRepeat)))
        {
            return Interface::BufferRef::IndexBufferResult::REQUIRES_REPEAT;
        }

        return Interface::BufferRef::IndexBufferResult::INITIALIZED;
    }

    /// Executes the second phase, which iterates over a (raw) buffer, reading specific records and fields from a (raw) buffer
    /// Relies on the index created in the first phase (indexBuffer), which it accesses through static_thread local members
    Record readRecord(
        const std::vector<Record::RecordFieldIdentifier>& projections,
        const RecordBuffer& recordBuffer,
        nautilus::val<uint64_t>& recordIndex) const
    {
        const nautilus::val<bool> bufferHasLeadingSpanningTuple = *Nautilus::Util::getMemberWithOffset<bool>(
            *tlIndexPhaseResultNautilusVal, offsetof(IndexPhaseResult, hasLeadingSpanningTupleBool));
        const nautilus::val<bool> bufferHasTrailingSpanningTuple = *Nautilus::Util::getMemberWithOffset<bool>(
            *tlIndexPhaseResultNautilusVal, offsetof(IndexPhaseResult, hasTrailingSpanningTupleBool));

        if (/*isFirstRecord*/ (recordIndex == 0) and bufferHasLeadingSpanningTuple)
        {
            auto leadingFIF = Nautilus::Util::getMemberWithOffset<typename FormatterType::FieldIndexFunctionType>(
                *tlIndexPhaseResultNautilusVal, offsetof(IndexPhaseResult, leadingSpanningTupleFIF));

            /// Get leading field index function and a pointer to the spanning tuple 'record'
            auto spanningRecordPtr = *Nautilus::Util::getMemberPtrWithOffset<int8_t>(
                *tlIndexPhaseResultNautilusVal, offsetof(IndexPhaseResult, leadingSpanningTuplePtr));

            return typename FormatterType::FieldIndexFunctionType{}.readSpanningRecord(
                projections, spanningRecordPtr, nautilus::val<uint64_t>(0), indexerMetaData, leadingFIF, *tlArenaPtr);
        }
        if (/*isLastRecord*/ (recordIndex + 1 == recordBuffer.getNumRecords()) and bufferHasTrailingSpanningTuple)
        {
            auto trailingFIF = Nautilus::Util::getMemberWithOffset<typename FormatterType::FieldIndexFunctionType>(
                *tlIndexPhaseResultNautilusVal, offsetof(IndexPhaseResult, trailingSpanningTupleFIF));

            auto spanningRecordPtr = *Nautilus::Util::getMemberPtrWithOffset<int8_t>(
                *tlIndexPhaseResultNautilusVal, offsetof(IndexPhaseResult, trailingSpanningTuplePtr));

            return trailingFIF.value->readSpanningRecord(
                projections, spanningRecordPtr, nautilus::val<uint64_t>(0), indexerMetaData, trailingFIF, *tlArenaPtr);
        }

        /// Proces all non-spanning tuples in raw (record) buffer, adjusting the index by 1, if there is a leading spanning tuple
        auto rawBufferRecordIndex = recordIndex - static_cast<nautilus::val<uint64_t>>(bufferHasLeadingSpanningTuple);
        auto rawFieldAccessFunction = Nautilus::Util::getMemberWithOffset<typename FormatterType::FieldIndexFunctionType>(
            *tlIndexPhaseResultNautilusVal, offsetof(IndexPhaseResult, rawBufferFIF));
        return typename FormatterType::FieldIndexFunctionType{}.readSpanningRecord(
            projections, recordBuffer.getMemArea(), rawBufferRecordIndex, indexerMetaData, rawFieldAccessFunction, *tlArenaPtr);
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
    std::shared_ptr<NES::Nautilus::Interface::BufferRef::TupleBufferRef> memoryProvider;
    std::unique_ptr<SequenceShredder> sequenceShredder; /// unique_ptr, because mutex is not copiable

    struct IndexPhaseResult
    {
        int8_t* leadingSpanningTuplePtr = nullptr;
        int8_t* trailingSpanningTuplePtr = nullptr;
        bool hasLeadingSpanningTupleBool = false;
        bool hasTrailingSpanningTupleBool = false;
        uint64_t totalNumberOfTuples = 0;
        size_t leadingSpanningTupleSizeInBytes = 0;
        size_t trailingSpanningTupleSizeInBytes = 0;
        bool isRepeat = false;

        typename FormatterType::FieldIndexFunctionType leadingSpanningTupleFIF;
        typename FormatterType::FieldIndexFunctionType trailingSpanningTupleFIF;
        typename FormatterType::FieldIndexFunctionType rawBufferFIF;
    };

    static_assert(std::is_standard_layout_v<IndexPhaseResult>, "IndexPhaseResult must have a standard layout for safe usage of offsetof");

    /// We need thread_local variables to 'bridge' state between the indexing (open) and the parsing (readRecord) phase.
    /// They must be thread_local, since multiple threads use them at the same time, especially in the interpreted mode.
    /// 'thread_local' means that every thread creates its own 'local' static version.
    /// TODO #1185: we only need the arenaRef, to parse raw values of type 'VarSized' (RawValueParser uses allocateVariableSizedData)
    ///  and because we use 'readRecord', which does not take arguments that give access to the arena. We can get rid of this thread_local
    ///  if we lazily parse VarSized and enable execution on the raw input data directly)
    static thread_local ArenaRef* tlArenaPtr;
    /// 'Bridges' the result of the indexing phase (open() call) to the parsing phase (readRecord() call, uses index to access fields).
    /// Must be stateful, because it must exist between open() and readRecord(). Must be thread_local, because otherwise there would be a
    /// race conditions between different threads executing the same pipeline (with different data).
    static thread_local IndexPhaseResult tlIndexPhaseResult;
    /// The 'tlIndexPhaseResultNautilusVal' allows the indexing phase to pass the result of the indexing proxy call to the parsing phase.
    /// We can't use 'tlIndexPhaseResult' directly, because we would hardcode the thread_local address of the tracing thread into usages of
    /// the 'tlIndexPhaseResult', which causes race conditions during execution.
    /// It must be thread_local, because otherwise there is a race condition between threads when executing in INTERPRETER mode.
    /// It must be a pointer, because copy assigning into a static thread_local causes a crash during tracing, which a pointer prevents.
    static thread_local std::unique_ptr<nautilus::val<IndexPhaseResult*>> tlIndexPhaseResultNautilusVal;

    /// Builds an index during the indexing phase (open()), which the parsing phase (readRecord()) requires for accessing specific fields.
    /// Does not have state itself, but wraps the static thread_local 'tlIndexPhaseResult', which it builds during its lifetime.
    class IndexPhaseResultBuilder
    {
    public:
        IndexPhaseResultBuilder() = delete;

        static void startBuildingIndex() { tlIndexPhaseResult = IndexPhaseResult{}; }

        static std::pair<FieldIndex, FieldIndex> indexRawBuffer(InputFormatter& InputFormatter, const TupleBuffer& tupleBuffer)
        {
            InputFormatter.inputFormatIndexer.indexRawBuffer(
                tlIndexPhaseResult.rawBufferFIF, RawTupleBuffer{tupleBuffer}, InputFormatter.indexerMetaData);
            tlIndexPhaseResult.totalNumberOfTuples = tlIndexPhaseResult.rawBufferFIF.getTotalNumberOfTuples();
            return {
                tlIndexPhaseResult.rawBufferFIF.getOffsetOfFirstTupleDelimiter(),
                tlIndexPhaseResult.rawBufferFIF.getOffsetOfLastTupleDelimiter()};
        }

        static void setIsRepeat(bool isRepeat) { tlIndexPhaseResult.isRepeat = isRepeat; }

        static IndexPhaseResult* finalizeIndexPhase()
        {
            tlIndexPhaseResult.totalNumberOfTuples += static_cast<uint64_t>(tlIndexPhaseResult.hasLeadingSpanningTupleBool)
                + static_cast<uint64_t>(tlIndexPhaseResult.hasTrailingSpanningTupleBool);
            return &tlIndexPhaseResult;
        }

        static void handleWithDelimiter(
            const std::vector<StagedBuffer>& stagedBuffers,
            const size_t indexOfSequenceNumberInStagedBuffers,
            InputFormatter& InputFormatter,
            Arena& arenaRef)
        {
            calculateSizeOfSpanningTuplesWithDelimiter(
                stagedBuffers, indexOfSequenceNumberInStagedBuffers, InputFormatter.indexerMetaData.getTupleDelimitingBytes().size());

            if (tlIndexPhaseResult.leadingSpanningTupleSizeInBytes > 0)
            {
                allocateForLeadingSpanningTuple(arenaRef);
                const auto leadingSpanningTupleBuffers = std::span(stagedBuffers).subspan(0, indexOfSequenceNumberInStagedBuffers + 1);
                processSpanningTuple<typename FormatterType::IndexerMetaData>(
                    leadingSpanningTupleBuffers, tlIndexPhaseResult.leadingSpanningTuplePtr, InputFormatter.indexerMetaData);
                InputFormatter.inputFormatIndexer.indexRawBuffer(
                    tlIndexPhaseResult.leadingSpanningTupleFIF,
                    RawTupleBuffer{
                        std::bit_cast<const char*>(tlIndexPhaseResult.leadingSpanningTuplePtr),
                        tlIndexPhaseResult.leadingSpanningTupleSizeInBytes},
                    InputFormatter.indexerMetaData);
            }
            if (tlIndexPhaseResult.trailingSpanningTupleSizeInBytes > 0)
            {
                allocateForTrailingSpanningTuple(arenaRef);
                const auto trailingSpanningTupleBuffers
                    = std::span(stagedBuffers)
                          .subspan(indexOfSequenceNumberInStagedBuffers, stagedBuffers.size() - indexOfSequenceNumberInStagedBuffers);
                processSpanningTuple<typename FormatterType::IndexerMetaData>(
                    trailingSpanningTupleBuffers, tlIndexPhaseResult.trailingSpanningTuplePtr, InputFormatter.indexerMetaData);
                InputFormatter.inputFormatIndexer.indexRawBuffer(
                    tlIndexPhaseResult.trailingSpanningTupleFIF,
                    RawTupleBuffer{
                        std::bit_cast<const char*>(tlIndexPhaseResult.trailingSpanningTuplePtr),
                        tlIndexPhaseResult.trailingSpanningTupleSizeInBytes},
                    InputFormatter.indexerMetaData);
            }
        }

        static void
        handleWithoutDelimiter(const std::vector<StagedBuffer>& spanningTupleBuffers, InputFormatter& inputFormatter, Arena& arenaRef)
        {
            calculateSizeOfSpanningTuple(
                [](const size_t bytes) { increaseLeadingSpanningTupleSize(bytes); },
                spanningTupleBuffers,
                inputFormatter.indexerMetaData.getTupleDelimitingBytes().size());
            allocateForLeadingSpanningTuple(arenaRef);

            processSpanningTuple<typename FormatterType::IndexerMetaData>(
                spanningTupleBuffers, tlIndexPhaseResult.leadingSpanningTuplePtr, inputFormatter.indexerMetaData);
            inputFormatter.inputFormatIndexer.indexRawBuffer(
                tlIndexPhaseResult.leadingSpanningTupleFIF,
                RawTupleBuffer{
                    std::bit_cast<const char*>(tlIndexPhaseResult.leadingSpanningTuplePtr),
                    tlIndexPhaseResult.leadingSpanningTupleSizeInBytes},
                inputFormatter.indexerMetaData);
        }

    private:
        static void allocateForLeadingSpanningTuple(Arena& arenaRef)
        {
            tlIndexPhaseResult.hasLeadingSpanningTupleBool = true;
            tlIndexPhaseResult.leadingSpanningTuplePtr = arenaRef.allocateMemory(tlIndexPhaseResult.leadingSpanningTupleSizeInBytes);
        }

        static void allocateForTrailingSpanningTuple(Arena& arenaRef)
        {
            tlIndexPhaseResult.hasTrailingSpanningTupleBool = true;
            tlIndexPhaseResult.trailingSpanningTuplePtr = arenaRef.allocateMemory(tlIndexPhaseResult.trailingSpanningTupleSizeInBytes);
        }

        static void increaseLeadingSpanningTupleSize(const size_t additionalBytes)
        {
            tlIndexPhaseResult.leadingSpanningTupleSizeInBytes += additionalBytes;
        }

        static void increaseTrailingSpanningTupleSize(const size_t additionalBytes)
        {
            tlIndexPhaseResult.trailingSpanningTupleSizeInBytes += additionalBytes;
        }

        template <typename IncreaseFunc>
        static void calculateSizeOfSpanningTuple(
            IncreaseFunc&& increaseFunc, const std::span<const StagedBuffer> spanningTupleBuffers, const size_t sizeOfTupleDelimiterInBytes)
        {
            increaseFunc(2 * sizeOfTupleDelimiterInBytes);
            increaseFunc(spanningTupleBuffers.front().getTrailingBytes(sizeOfTupleDelimiterInBytes).size());
            for (size_t i = 1; i < spanningTupleBuffers.size() - 1; ++i)
            {
                increaseFunc(spanningTupleBuffers[i].getSizeOfBufferInBytes());
            }
            increaseFunc(spanningTupleBuffers.back().getLeadingBytes().size());
        }

        static void calculateSizeOfSpanningTuplesWithDelimiter(
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
                    [](const size_t bytes) { increaseLeadingSpanningTupleSize(bytes); }, spanningTupleBuffers, sizeOfTupleDelimiterInBytes);
                return;
            }
            /// Has trailing spanning tuple only
            if (not hasLeadingST)
            {
                calculateSizeOfSpanningTuple(
                    [](const size_t bytes) { increaseTrailingSpanningTupleSize(bytes); },
                    spanningTupleBuffers,
                    sizeOfTupleDelimiterInBytes);
                return;
            }
            /// Has both leading and spanning tuple
            calculateSizeOfSpanningTuple(
                [](const size_t bytes) { increaseLeadingSpanningTupleSize(bytes); },
                std::span(spanningTupleBuffers).subspan(0, indexOfSequenceNumberInStagedBuffers + 1),
                sizeOfTupleDelimiterInBytes);

            calculateSizeOfSpanningTuple(
                [](const size_t bytes) { increaseTrailingSpanningTupleSize(bytes); },
                std::span(spanningTupleBuffers)
                    .subspan(indexOfSequenceNumberInStagedBuffers, spanningTupleBuffers.size() - (indexOfSequenceNumberInStagedBuffers)),
                sizeOfTupleDelimiterInBytes);
        }
    };

    static IndexPhaseResult*
    indexBufferProxy(const TupleBuffer* tupleBuffer, InputFormatter* InputFormatter, const size_t configuredBufferSize, Arena* arenaRef)
    {
        if (not InputFormatter->sequenceShredder->isInRange(tupleBuffer->getSequenceNumber().getRawValue()))
        {
            IndexPhaseResultBuilder::setIsRepeat(true);
            return IndexPhaseResultBuilder::finalizeIndexPhase();
        }

        IndexPhaseResultBuilder::startBuildingIndex();
        const auto [offsetOfFirstTupleDelimiter, offsetOfSecondTupleDelimiter]
            = IndexPhaseResultBuilder::indexRawBuffer(*InputFormatter, *tupleBuffer);

        if (/* hasTupleDelimiter */ offsetOfFirstTupleDelimiter < configuredBufferSize)
        {
            const auto [indexOfSequenceNumberInStagedBuffers, stagedBuffers]
                = InputFormatter->sequenceShredder->processSequenceNumber<true>(
                    StagedBuffer{
                        RawTupleBuffer{*tupleBuffer},
                        tupleBuffer->getNumberOfTuples(), /// size in bytes
                        offsetOfFirstTupleDelimiter,
                        offsetOfSecondTupleDelimiter},
                    tupleBuffer->getSequenceNumber().getRawValue());

            if (stagedBuffers.size() < 2)
            {
                return IndexPhaseResultBuilder::finalizeIndexPhase();
            }
            IndexPhaseResultBuilder::handleWithDelimiter(stagedBuffers, indexOfSequenceNumberInStagedBuffers, *InputFormatter, *arenaRef);
        }
        else /* has no tuple delimiter */
        {
            const auto [indexOfSequenceNumberInStagedBuffers, stagedBuffers]
                = InputFormatter->sequenceShredder->processSequenceNumber<false>(
                    StagedBuffer{
                        RawTupleBuffer{*tupleBuffer},
                        tupleBuffer->getNumberOfTuples(), /// size in bytes
                        offsetOfFirstTupleDelimiter,
                        offsetOfSecondTupleDelimiter},
                    tupleBuffer->getSequenceNumber().getRawValue());

            if (stagedBuffers.size() < 3)
            {
                return IndexPhaseResultBuilder::finalizeIndexPhase();
            }

            /// The buffer has no delimiter, but connects two buffers with delimiters, forming one spanning tuple
            /// We arbitrarily treat it as a 'leading' spanning tuple (technically, it is both leading and trailing)
            IndexPhaseResultBuilder::handleWithoutDelimiter(stagedBuffers, *InputFormatter, *arenaRef);
        }
        return IndexPhaseResultBuilder::finalizeIndexPhase();
    }
};

/// Necessary thread_local variable definitions to make static thread_local variables usable in the InputFormatter class
template <InputFormatIndexerType T>
thread_local std::unique_ptr<nautilus::val<typename InputFormatter<T>::IndexPhaseResult*>>
    InputFormatter<T>::tlIndexPhaseResultNautilusVal{};
template <InputFormatIndexerType T>
thread_local typename InputFormatter<T>::IndexPhaseResult InputFormatter<T>::tlIndexPhaseResult{};
template <InputFormatIndexerType T>
thread_local ArenaRef* InputFormatter<T>::tlArenaPtr{};

}
