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

#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Nautilus/DataTypes/DataTypesUtil.hpp>
#include <Nautilus/Interface/MemoryProvider/TupleBufferMemoryProvider.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Concepts.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <FieldIndexFunction.hpp>
#include <PipelineExecutionContext.hpp>
#include <RawValueParser.hpp>
#include <SequenceShredder.hpp>

namespace NES
{
/// The type that all formatters use to represent indexes to fields.
using FieldIndex = uint32_t;

inline void setMetadataOfFormattedBuffer(
    const TupleBuffer& rawBuffer, TupleBuffer& formattedBuffer, ChunkNumber::Underlying& runningChunkNumber, const bool isLastChunk)
{
    formattedBuffer.setLastChunk(isLastChunk);
    formattedBuffer.setSequenceNumber(rawBuffer.getSequenceNumber());
    formattedBuffer.setChunkNumber(ChunkNumber(runningChunkNumber++));
    formattedBuffer.setOriginId(rawBuffer.getOriginId());
}

/// Given that we know the number of tuples in a raw buffer, the number of (spanning) tuples that we already wrote into our current formatted
/// buffer and the max number of tuples that fit into a formatted buffer (given a schema), we can precisely calculate how many formatted buffers
/// we need to store all tuples from the raw buffer.
inline size_t calculateNumberOfRequiredFormattedBuffers(
    const size_t totalNumberOfTuplesInRawBuffer, const size_t numberOfTuplesInFirstFormattedBuffer, const size_t numberOfTuplesPerBuffer)
{
    const auto capacityOfFirstBuffer
        = std::min(numberOfTuplesPerBuffer - numberOfTuplesInFirstFormattedBuffer, totalNumberOfTuplesInRawBuffer);
    const auto leftOverTuplesInFirstBuffer = totalNumberOfTuplesInRawBuffer - capacityOfFirstBuffer;
    /// We need at least one (first) formatted buffer. We need additional buffers if we have leftover raw tuples.
    /// Overflow-safe calculation of ceil taken from (https://stackoverflow.com/questions/2745074/fast-ceiling-of-an-integer-division-in-c-c)
    /// 1 + ceil(leftOverTuplesInFirstBuffer / numberOfTuplesPerBuffer)
    const auto numberOfBuffersToFill = 1
        + ((leftOverTuplesInFirstBuffer % numberOfTuplesPerBuffer != 0U) ? (leftOverTuplesInFirstBuffer / numberOfTuplesPerBuffer) + 1
                                                                         : leftOverTuplesInFirstBuffer / numberOfTuplesPerBuffer);
    return numberOfBuffersToFill;
}

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

/// InputFormatterTasks concurrently take (potentially) raw input buffers and format all full tuples in these raw input buffers that the
/// individual InputFormatterTasks see during execution.
/// The only point of synchronization is a call to the SequenceShredder data structure, which determines which buffers the InputFormatterTask
/// needs to process (resolving tuples that span multiple raw buffers).
/// An InputFormatterTask belongs to exactly one source. The source reads raw data into buffers, constructs a Task from the
/// raw buffer and its successor (the InputFormatterTask) and writes it to the task queue of the QueryEngine.
/// The QueryEngine concurrently executes InputFormatterTasks. Thus, even if the source writes the InputFormatterTasks to the task queue sequentially,
/// the QueryEngine may still execute them in any order.
template <InputFormatIndexerType FormatterType>
class InputFormatterTask
{
public:
    explicit InputFormatterTask(
        FormatterType inputFormatIndexer,
        std::shared_ptr<Interface::MemoryProvider::TupleBufferMemoryProvider> memoryProvider,
        const ParserConfig& parserConfig)
        : inputFormatIndexer(std::move(inputFormatIndexer))
        , indexerMetaData(typename FormatterType::IndexerMetaData{parserConfig, *memoryProvider->getMemoryLayout()})
        , projections(memoryProvider->getMemoryLayout()->getSchema().getFieldNames())
        , memoryProvider(std::move(memoryProvider))
        /// Only if we need to resolve spanning tuples, we need the SequenceShredder
        , sequenceShredder(std::make_unique<SequenceShredder>(parserConfig.tupleDelimiter.size()))
    {
    }

    ~InputFormatterTask() = default;

    InputFormatterTask(const InputFormatterTask&) = delete;
    InputFormatterTask& operator=(const InputFormatterTask&) = delete;
    InputFormatterTask(InputFormatterTask&&) = default;
    InputFormatterTask& operator=(InputFormatterTask&&) = delete;

    void startTask() { /* noop */ }

    void stopTask() const
    {
        /// If the InputFormatterTask needs to handle spanning tuples, it uses the SequenceShredder.
        /// The logs of 'validateState()' allow us to detect whether something went wrong during formatting and specifically, whether the
        /// SequenceShredder still owns TupleBuffers, that it should have given back to its corresponding source.
        INVARIANT(sequenceShredder != nullptr, "The SequenceShredder handles spanning tuples, thus it must not be null.");
        if (not sequenceShredder->validateState())
        {
            throw FormattingError("Failed to validate SequenceShredder.");
        }
    }

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

        typename FormatterType::FieldIndexFunctionType leadingSpanningTupleFIF;
        typename FormatterType::FieldIndexFunctionType trailingSpanningTupleFIF;
        typename FormatterType::FieldIndexFunctionType rawBufferFIF;
    };

    /// accumulates all data produced during the indexing phase, which the parsing phase requires
    /// on calling 'createSpanningTuplePOD()' it returns this data
    class SpanningTupleData
    {
    public:
        explicit SpanningTupleData() = default;

        std::pair<FieldIndex, FieldIndex> indexRawBuffer(InputFormatterTask& inputFormatterTask, const TupleBuffer& tupleBuffer)
        {
            inputFormatterTask.inputFormatIndexer.indexRawBuffer(
                spanningTuplePOD.rawBufferFIF, RawTupleBuffer{tupleBuffer}, inputFormatterTask.indexerMetaData);
            this->spanningTuplePOD.totalNumberOfTuples = this->spanningTuplePOD.rawBufferFIF.getTotalNumberOfTuples();
            return {
                spanningTuplePOD.rawBufferFIF.getOffsetOfFirstTupleDelimiter(),
                spanningTuplePOD.rawBufferFIF.getOffsetOfLastTupleDelimiter()};
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
            InputFormatterTask& inputFormatterTask,
            Arena& arenaRef)
        {
            calculateSizeOfSpanningTuplesWithDelimiter(
                stagedBuffers, indexOfSequenceNumberInStagedBuffers, inputFormatterTask.indexerMetaData.getTupleDelimitingBytes().size());

            if (spanningTuplePOD.leadingSpanningTupleSizeInBytes > 0)
            {
                allocateForLeadingSpanningTuple(arenaRef);
                const auto leadingSpanningTupleBuffers = std::span(stagedBuffers).subspan(0, indexOfSequenceNumberInStagedBuffers + 1);
                processSpanningTuple<typename FormatterType::IndexerMetaData>(
                    leadingSpanningTupleBuffers, spanningTuplePOD.leadingSpanningTuplePtr, inputFormatterTask.indexerMetaData);
                inputFormatterTask.inputFormatIndexer.indexRawBuffer(
                    spanningTuplePOD.leadingSpanningTupleFIF,
                    RawTupleBuffer{
                        std::bit_cast<const char*>(spanningTuplePOD.leadingSpanningTuplePtr),
                        spanningTuplePOD.leadingSpanningTupleSizeInBytes},
                    inputFormatterTask.indexerMetaData);
            }
            if (spanningTuplePOD.trailingSpanningTupleSizeInBytes > 0)
            {
                allocateForTrailingSpanningTuple(arenaRef);
                const auto trailingSpanningTupleBuffers
                    = std::span(stagedBuffers)
                          .subspan(indexOfSequenceNumberInStagedBuffers, stagedBuffers.size() - indexOfSequenceNumberInStagedBuffers);
                processSpanningTuple<typename FormatterType::IndexerMetaData>(
                    trailingSpanningTupleBuffers, spanningTuplePOD.trailingSpanningTuplePtr, inputFormatterTask.indexerMetaData);
                inputFormatterTask.inputFormatIndexer.indexRawBuffer(
                    spanningTuplePOD.trailingSpanningTupleFIF,
                    RawTupleBuffer{
                        std::bit_cast<const char*>(spanningTuplePOD.trailingSpanningTuplePtr),
                        spanningTuplePOD.trailingSpanningTupleSizeInBytes},
                    inputFormatterTask.indexerMetaData);
            }
        }

        void handleWithoutDelimiter(
            const std::vector<StagedBuffer>& spanningTupleBuffers, InputFormatterTask& inputFormatterTask, Arena& arenaRef)
        {
            calculateSizeOfSpanningTuple(
                [this](const size_t bytes) { increaseLeadingSpanningTupleSize(bytes); },
                spanningTupleBuffers,
                inputFormatterTask.indexerMetaData.getTupleDelimitingBytes().size());
            allocateForLeadingSpanningTuple(arenaRef);

            processSpanningTuple<typename FormatterType::IndexerMetaData>(
                spanningTupleBuffers, spanningTuplePOD.leadingSpanningTuplePtr, inputFormatterTask.indexerMetaData);
            inputFormatterTask.inputFormatIndexer.indexRawBuffer(
                spanningTuplePOD.leadingSpanningTupleFIF,
                RawTupleBuffer{
                    std::bit_cast<const char*>(spanningTuplePOD.leadingSpanningTuplePtr), spanningTuplePOD.leadingSpanningTupleSizeInBytes},
                inputFormatterTask.indexerMetaData);
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
        void calculateSizeOfSpanningTuple(
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
                    [this](const size_t bytes) { increaseLeadingSpanningTupleSize(bytes); },
                    spanningTupleBuffers,
                    sizeOfTupleDelimiterInBytes);
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
        PipelineExecutionContext* pec,
        InputFormatterTask* inputFormatterTask,
        const size_t configuredBufferSize,
        Arena* arenaRef)
    {
        SpanningTupleData spanningTupleData{};
        if (not inputFormatterTask->sequenceShredder->isInRange(tupleBuffer->getSequenceNumber().getRawValue()))
        {
            pec->emitBuffer(*tupleBuffer, PipelineExecutionContext::ContinuationPolicy::REPEAT);
            spanningTupleData.setIsRepeat(true);
            return spanningTupleData.getThreadLocalSpanningTuplePODPtr();
        }

        const auto [offsetOfFirstTupleDelimiter, offsetOfSecondTupleDelimiter]
            = spanningTupleData.indexRawBuffer(*inputFormatterTask, *tupleBuffer);


        if (/* hasTupleDelimiter */ offsetOfFirstTupleDelimiter < configuredBufferSize)
        {
            const auto [indexOfSequenceNumberInStagedBuffers, stagedBuffers]
                = inputFormatterTask->sequenceShredder->processSequenceNumber<true>(
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
            spanningTupleData.handleWithDelimiter(stagedBuffers, indexOfSequenceNumberInStagedBuffers, *inputFormatterTask, *arenaRef);
        }
        else /* has no tuple delimiter */
        {
            const auto [indexOfSequenceNumberInStagedBuffers, stagedBuffers]
                = inputFormatterTask->sequenceShredder->processSequenceNumber<false>(
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
            spanningTupleData.handleWithoutDelimiter(stagedBuffers, *inputFormatterTask, *arenaRef);
        }
        return spanningTupleData.getThreadLocalSpanningTuplePODPtr();
    }

    OpenReturnState scanTask(ExecutionContext& executionCtx, Nautilus::RecordBuffer& recordBuffer, const PhysicalOperator& child)
    {
        /// initialize global state variables to keep track of the watermark ts and the origin id
        executionCtx.watermarkTs = recordBuffer.getWatermarkTs();
        executionCtx.originId = recordBuffer.getOriginId();
        executionCtx.currentTs = recordBuffer.getCreatingTs();
        executionCtx.sequenceNumber = recordBuffer.getSequenceNumber();
        executionCtx.chunkNumber = recordBuffer.getChunkNumber();
        executionCtx.lastChunk = recordBuffer.isLastChunk();


        /// index raw tuple buffer, resolve and index spanning tuples(SequenceShredder) and return pointers to resolved spanning tuples, if exist;
        auto spanningTuplePOD = nautilus::invoke(
            indexTuplesProxy,
            recordBuffer.getReference(),
            executionCtx.pipelineContext,
            nautilus::val<InputFormatterTask*>(this),
            nautilus::val<size_t>(memoryProvider->getMemoryLayout()->getBufferSize()),
            executionCtx.pipelineMemoryProvider.arena.getArena());

        if (/* isRepeat */ *Nautilus::Util::getMemberWithOffset<bool>(spanningTuplePOD, offsetof(SpanningTuplePOD, isRepeat)))
        {
            return OpenReturnState::NOT_FINISHED;
        }

        /// call open on all child operators
        child.open(executionCtx, recordBuffer);

        /// parse leading spanning tuple if exists
        if (const nautilus::val<bool> hasLeadingPtr
            = *Nautilus::Util::getMemberWithOffset<bool>(spanningTuplePOD, offsetof(SpanningTuplePOD, hasLeadingSpanningTupleBool));
            hasLeadingPtr)
        {
            /// Get leading field index function and a pointer to the spanning tuple 'record'
            auto leadingFIF = Nautilus::Util::getMemberWithOffset<typename FormatterType::FieldIndexFunctionType>(
                spanningTuplePOD, offsetof(SpanningTuplePOD, leadingSpanningTupleFIF));
            auto spanningRecordPtr
                = *Nautilus::Util::getMemberPtrWithOffset<int8_t>(spanningTuplePOD, offsetof(SpanningTuplePOD, leadingSpanningTuplePtr));

            /// 'leadingFIF.value' is essentially the static function FormatterType::readsSpanningRecord
            auto recordIndex = nautilus::val<uint64_t>(0);
            auto record = leadingFIF.value->readSpanningRecord(
                projections, spanningRecordPtr, recordIndex, indexerMetaData, leadingFIF, executionCtx.pipelineMemoryProvider.arena);
            child.execute(executionCtx, record);
        }

        /// parse raw tuple buffer (if there are complete tuples in it)
        const nautilus::val<uint64_t> totalNumberOfTuples
            = *Nautilus::Util::getMemberWithOffset<uint64_t>(spanningTuplePOD, offsetof(SpanningTuplePOD, totalNumberOfTuples));
        auto rawFieldAccessFunction = Nautilus::Util::getMemberWithOffset<typename FormatterType::FieldIndexFunctionType>(
            spanningTuplePOD, offsetof(SpanningTuplePOD, rawBufferFIF));

        for (nautilus::val<uint64_t> i = static_cast<uint64_t>(0); i < totalNumberOfTuples; i = i + static_cast<uint64_t>(1))
        {
            auto record = rawFieldAccessFunction.value->readSpanningRecord(
                projections,
                recordBuffer.getMemArea(),
                i,
                indexerMetaData,
                rawFieldAccessFunction,
                executionCtx.pipelineMemoryProvider.arena);
            child.execute(executionCtx, record);
        }

        /// parse trailing spanning tuple if exists
        if (const nautilus::val<bool> hasTrailingPtr
            = *Nautilus::Util::getMemberWithOffset<bool>(spanningTuplePOD, offsetof(SpanningTuplePOD, hasTrailingSpanningTupleBool));
            hasTrailingPtr)
        {
            auto trailingFIF = Nautilus::Util::getMemberWithOffset<typename FormatterType::FieldIndexFunctionType>(
                spanningTuplePOD, offsetof(SpanningTuplePOD, trailingSpanningTupleFIF));
            auto spanningRecordPtr
                = *Nautilus::Util::getMemberPtrWithOffset<int8_t>(spanningTuplePOD, offsetof(SpanningTuplePOD, trailingSpanningTuplePtr));

            auto recordIndex = nautilus::val<uint64_t>(0);
            auto record = trailingFIF.value->readSpanningRecord(
                projections, spanningRecordPtr, recordIndex, indexerMetaData, trailingFIF, executionCtx.pipelineMemoryProvider.arena);
            child.execute(executionCtx, record);
        }
        return OpenReturnState::FINISHED;
    }

    std::ostream& taskToString(std::ostream& os) const
    {
        /// Not using fmt::format, because it fails during build, trying to pass sequenceShredder as a const value
        os << "InputFormatterTask(" << ", inputFormatIndexer: " << inputFormatIndexer << ", sequenceShredder: " << *sequenceShredder
           << ")\n";
        return os;
    }

private:
    FormatterType inputFormatIndexer;
    typename FormatterType::IndexerMetaData indexerMetaData;
    std::vector<Record::RecordFieldIdentifier> projections;
    std::shared_ptr<Interface::MemoryProvider::TupleBufferMemoryProvider> memoryProvider;
    std::unique_ptr<SequenceShredder> sequenceShredder; /// unique_ptr, because mutex is not copiable
};

}
