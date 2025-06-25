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
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <InputFormatters/InputFormatIndexer.hpp>
#include <InputFormatters/InputFormatterTaskPipeline.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Common.hpp>
#include <ErrorHandling.hpp>
#include <FieldIndexFunction.hpp>
#include <PipelineExecutionContext.hpp>
#include <RawValueParser.hpp>
#include <SequenceShredder.hpp>

#include <Nautilus/Interface/MemoryProvider/TupleBufferMemoryProvider.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Util/StdInt.hpp>
#include <NativeFieldIndexFunction.hpp>


namespace NES::InputFormatters
{
/// The type that all formatters use to represent indexes to fields.
using FieldOffsetsType = uint32_t;

/// Constructs a spanning tuple (string) that spans over at least two buffers (buffersToFormat).
/// First, determines the start of the spanning tuple in the first buffer to format. Constructs a spanning tuple from the required bytes.
/// Second, appends all bytes of all raw buffers that are not the last buffer to the spanning tuple.
/// Third, determines the end of the spanning tuple in the last buffer to format. Appends the required bytes to the spanning tuple.
/// Lastly, formats the full spanning tuple.
template <IndexerMetaDataType IndexerMetaData>
void processSpanningTuple(
    const std::vector<StagedBuffer> stagedBuffersSpan,
    int8_t* spanningTuplePointer, /// is of the right size
    const IndexerMetaData& indexerMetaData)
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
template <typename FormatterType, typename FieldIndexFunctionType, IndexerMetaDataType IndexerMetaData, bool HasSpanningTuple>
requires(HasSpanningTuple or not FormatterType::IsFormattingRequired)
class InputFormatterTask
{
public:
    static constexpr bool hasSpanningTuple() { return HasSpanningTuple; }
    explicit InputFormatterTask(
        const OriginId originId,
        std::unique_ptr<InputFormatIndexer<FieldIndexFunctionType, IndexerMetaData, FormatterType::IsFormattingRequired>>
            inputFormatIndexer,
        const Schema& schema,
        const RawValueParser::QuotationType quotationType,
        const ParserConfig& parserConfig)
        : originId(originId)
        , inputFormatIndexer(std::move(inputFormatIndexer))
        , schemaInfo(schema)
        , indexerMetaData(IndexerMetaData{parserConfig, schema})
        /// Only if we need to resolve spanning tuples, we need the SequenceShredder
        , sequenceShredder((HasSpanningTuple) ? std::make_unique<SequenceShredder>(parserConfig.tupleDelimiter.size()) : nullptr)
        /// Since we know the schema, we can create a vector that contains a function that converts the string representation of a field value
        /// to our internal representation in the correct order. During parsing, we iterate over the fields in each tuple, and, using the current
        /// field number, load the correct function for parsing from the vector.
        , parseFunctions(
              schema.getFields()
              | std::views::transform([quotationType](const auto& field)
                                      { return RawValueParser::getParseFunction(field.dataType.type, quotationType); })
              | std::ranges::to<std::vector>())
    {
    }
    ~InputFormatterTask() = default;

    InputFormatterTask(const InputFormatterTask&) = delete;
    InputFormatterTask& operator=(const InputFormatterTask&) = delete;
    InputFormatterTask(InputFormatterTask&&) = default;
    InputFormatterTask& operator=(InputFormatterTask&&) = delete;

    void stopTask() const
    {
        /// If the InputFormatterTask needs to handle spanning tuples, it uses the SequenceShredder.
        /// The logs of 'validateState()' allow us to detect whether something went wrong during formatting and specifically, whether the
        /// SequenceShredder still owns TupleBuffers, that it should have given back to its corresponding source.
        if (HasSpanningTuple)
        {
            INVARIANT(sequenceShredder != nullptr, "The SequenceShredder handles spanning tuples, thus it must not be null.");
            if (not sequenceShredder->validateState())
            {
                throw FormattingError("Failed to validate SequenceShredder.");
            }
        }
    }

    class SpanningTupleData
    {
    public:
        // Todo: allocate fixSizeBufferPool with 1 buffer and buffer size that matches exactly required number of indexes for spanning tuples?
        explicit SpanningTupleData(Memory::AbstractBufferProvider& bufferProvider)
            : leadingSpanningTupleFIF(FieldIndexFunctionType(bufferProvider))
            , trailingSpanningTupleFIF(FieldIndexFunctionType(bufferProvider))
            , rawBufferFIF(FieldIndexFunctionType(bufferProvider)) { };

        void setAllocatedMemory(int8_t* spanningTuplePtr) { this->spanningTuplePtr = spanningTuplePtr; }
        void increaseLeadingSpanningTupleSize(const size_t additionalBytes) { leadingSpanningTupleSizeInBytes += additionalBytes; }
        void increaseTrailingSpanningTupleSize(const size_t additionalBytes) { trailingSpanningTupleSizeInBytes += additionalBytes; }

        size_t getSizeOfLeadingSpanningTuple() const { return leadingSpanningTupleSizeInBytes; }
        size_t getSizeOfTrailingSpanningTuple() const { return trailingSpanningTupleSizeInBytes; }
        size_t getTotalSize() const { return leadingSpanningTupleSizeInBytes + trailingSpanningTupleSizeInBytes; }

        int8_t* getLeadingSpanningTuplePointer() const { return spanningTuplePtr; }
        int8_t* getTrailingTuplePointer() const { return spanningTuplePtr + trailingSpanningTupleSizeInBytes; }

        bool hasLeadingSpanningTuple() const { return leadingSpanningTupleSizeInBytes > 0; }
        bool hasTrailingSpanningTuple() const { return trailingSpanningTupleSizeInBytes > 0; }

        FieldIndexFunctionType& getLeadingSpanningTupleFIF() { return leadingSpanningTupleFIF; }
        FieldIndexFunctionType& getTrailingSpanningTupleFIF() { return trailingSpanningTupleFIF; }
        FieldIndexFunctionType& getRawBufferFIF() { return rawBufferFIF; }

    private:
        int8_t* spanningTuplePtr = nullptr;
        size_t leadingSpanningTupleSizeInBytes = 0;
        size_t trailingSpanningTupleSizeInBytes = 0;

        FieldIndexFunctionType leadingSpanningTupleFIF;
        FieldIndexFunctionType trailingSpanningTupleFIF;
        FieldIndexFunctionType rawBufferFIF;
    };

    static void calculateSizeOfSpanningTuples(
        SpanningTupleData& spanningTupleData,
        const bool hasTupleDelimiter,
        const std::vector<StagedBuffer>& spanningTupleBuffers,
        const size_t indexOfSequenceNumberInStagedBuffers,
        const size_t sizeOfTupleDelimiterInBytes)
    {
        // size_t sizeOfSpanningTuple = 2 * sizeOfTupleDelimiterInBytes;
        if (hasTupleDelimiter)
        {
            const bool hasLeadingST = indexOfSequenceNumberInStagedBuffers != 0;
            const bool hasTrailingST = indexOfSequenceNumberInStagedBuffers < spanningTupleBuffers.size() - 1;
            INVARIANT(hasLeadingST or hasTrailingST, "cannot calculate size of spanning tuple for buffers without spanning tuples");
            if (hasLeadingST and not hasTrailingST)
            {
                spanningTupleData.increaseLeadingSpanningTupleSize(2 * sizeOfTupleDelimiterInBytes);
                spanningTupleData.increaseLeadingSpanningTupleSize(
                    spanningTupleBuffers.front().getTrailingBytes(sizeOfTupleDelimiterInBytes).size());
                for (size_t i = 1; i < indexOfSequenceNumberInStagedBuffers - 1; ++i)
                {
                    spanningTupleData.increaseLeadingSpanningTupleSize(spanningTupleBuffers[i].getSizeOfBufferInBytes());
                }
                spanningTupleData.increaseLeadingSpanningTupleSize(spanningTupleBuffers.back().getLeadingBytes().size());
                return;
            }
            if (not hasLeadingST and hasTrailingST)
            {
                spanningTupleData.increaseTrailingSpanningTupleSize(2 * sizeOfTupleDelimiterInBytes);
                spanningTupleData.increaseTrailingSpanningTupleSize(
                    spanningTupleBuffers.front().getTrailingBytes(sizeOfTupleDelimiterInBytes).size());
                for (size_t i = indexOfSequenceNumberInStagedBuffers; i < spanningTupleBuffers.size() - 1; ++i)
                {
                    spanningTupleData.increaseTrailingSpanningTupleSize(spanningTupleBuffers[i].getSizeOfBufferInBytes());
                }
                spanningTupleData.increaseTrailingSpanningTupleSize(spanningTupleBuffers.back().getLeadingBytes().size());
                return;
            }
            if (hasLeadingST and hasTrailingST)
            {
                spanningTupleData.increaseLeadingSpanningTupleSize(2 * sizeOfTupleDelimiterInBytes);
                spanningTupleData.increaseTrailingSpanningTupleSize(2 * sizeOfTupleDelimiterInBytes);
                /// Size of leading spanning tuple
                spanningTupleData.increaseLeadingSpanningTupleSize(
                    spanningTupleBuffers.front().getTrailingBytes(sizeOfTupleDelimiterInBytes).size());
                for (size_t i = 1; i < indexOfSequenceNumberInStagedBuffers - 1; ++i)
                {
                    spanningTupleData.increaseLeadingSpanningTupleSize(spanningTupleBuffers[i].getSizeOfBufferInBytes());
                }
                spanningTupleData.increaseTrailingSpanningTupleSize(
                    spanningTupleBuffers[indexOfSequenceNumberInStagedBuffers].getLeadingBytes().size());

                /// Size of trailing spanning tuple
                spanningTupleData.increaseTrailingSpanningTupleSize(
                    spanningTupleBuffers[indexOfSequenceNumberInStagedBuffers].getTrailingBytes(sizeOfTupleDelimiterInBytes).size());
                for (size_t i = indexOfSequenceNumberInStagedBuffers; i < spanningTupleBuffers.size() - 1; ++i)
                {
                    spanningTupleData.increaseTrailingSpanningTupleSize(spanningTupleBuffers[i].getSizeOfBufferInBytes());
                }
                spanningTupleData.increaseLeadingSpanningTupleSize(spanningTupleBuffers.back().getLeadingBytes().size());
                return;
            }
        }
        else
        {
            spanningTupleData.increaseLeadingSpanningTupleSize(
                spanningTupleBuffers.front().getTrailingBytes(sizeOfTupleDelimiterInBytes).size());
            spanningTupleData.increaseLeadingSpanningTupleSize(spanningTupleBuffers.back().getLeadingBytes().size());
            for (size_t i = 1; i < spanningTupleBuffers.size() - 1; ++i)
            {
                spanningTupleData.increaseLeadingSpanningTupleSize(spanningTupleBuffers[i].getSizeOfBufferInBytes());
            }
        }
    }

    static SpanningTupleData* indexTuplesProxy(
        const Memory::TupleBuffer* tupleBuffer,
        PipelineExecutionContext* pec,
        InputFormatterTask* inputFormatterTask,
        const size_t configuredBufferSize,
        ExecutionContext* executionCtx)
    {
        thread_local SpanningTupleData spanningTupleData{*pec->getBufferManager()};
        if (not inputFormatterTask->sequenceShredder->isInRange(tupleBuffer->getSequenceNumber().getRawValue()))
        {
            pec->emitBuffer(*tupleBuffer, PipelineExecutionContext::ContinuationPolicy::REPEAT);
            return &spanningTupleData;
        }

        inputFormatterTask->inputFormatIndexer->indexRawBuffer(
            spanningTupleData.getRawBufferFIF(), RawTupleBuffer{*tupleBuffer}, inputFormatterTask->indexerMetaData);

        if (/*hasTupleDelimiter*/ spanningTupleData.getRawBufferFIF().getOffsetOfFirstTupleDelimiter() < configuredBufferSize)
        {
            const auto [indexOfSequenceNumberInStagedBuffers, stagedBuffers]
                = inputFormatterTask->sequenceShredder->processSequenceNumber<true>(
                    StagedBuffer{
                        RawTupleBuffer{*tupleBuffer},
                        tupleBuffer->getNumberOfTuples(), /// size in bytes
                        spanningTupleData.getRawBufferFIF().getOffsetOfFirstTupleDelimiter(),
                        spanningTupleData.getRawBufferFIF().getOffsetOfLastTupleDelimiter()},
                    tupleBuffer->getSequenceNumber().getRawValue());

            if (stagedBuffers.size() <= 1)
            {
                return &spanningTupleData;
            }

            calculateSizeOfSpanningTuples(
                spanningTupleData,
                true,
                stagedBuffers,
                indexOfSequenceNumberInStagedBuffers,
                inputFormatterTask->indexerMetaData.getTupleDelimitingBytes().size());
            spanningTupleData.setAllocatedMemory(
                executionCtx->allocateMemory(nautilus::val<size_t>(spanningTupleData.getTotalSize())).value);

            if (spanningTupleData.hasLeadingSpanningTuple())
            {
                processSpanningTuple<IndexerMetaData>(
                    stagedBuffers, spanningTupleData.getLeadingSpanningTuplePointer(), inputFormatterTask->indexerMetaData);
                inputFormatterTask->inputFormatIndexer->indexRawBuffer(
                    spanningTupleData.getLeadingSpanningTupleFIF(),
                    RawTupleBuffer{
                        std::bit_cast<const char*>(spanningTupleData.getLeadingSpanningTuplePointer()),
                        spanningTupleData.getSizeOfLeadingSpanningTuple()},
                    inputFormatterTask->indexerMetaData);
            }
            if (spanningTupleData.hasTrailingSpanningTuple())
            {
                processSpanningTuple<IndexerMetaData>(
                    stagedBuffers, spanningTupleData.getTrailingTuplePointer(), inputFormatterTask->indexerMetaData);
                inputFormatterTask->inputFormatIndexer->indexRawBuffer(
                    spanningTupleData.getTrailingSpanningTupleFIF(),
                    RawTupleBuffer{
                        std::bit_cast<const char*>(spanningTupleData.getTrailingTuplePointer()),
                        spanningTupleData.getSizeOfTrailingSpanningTuple()},
                    inputFormatterTask->indexerMetaData);
            }
        }
        else
        {
            const auto [indexOfSequenceNumberInStagedBuffers, stagedBuffers]
                = inputFormatterTask->sequenceShredder->processSequenceNumber<false>(
                    StagedBuffer{
                        RawTupleBuffer{*tupleBuffer},
                        tupleBuffer->getNumberOfTuples(), /// size in bytes
                        spanningTupleData.getRawBufferFIF().getOffsetOfFirstTupleDelimiter(),
                        spanningTupleData.getRawBufferFIF().getOffsetOfLastTupleDelimiter()},
                    tupleBuffer->getSequenceNumber().getRawValue());

            if (stagedBuffers.size() < 3)
            {
                return &spanningTupleData;
            }

            calculateSizeOfSpanningTuples(
                spanningTupleData,
                true,
                stagedBuffers,
                indexOfSequenceNumberInStagedBuffers,
                inputFormatterTask->indexerMetaData.getTupleDelimitingBytes().size());
            spanningTupleData.setAllocatedMemory(
                executionCtx->allocateMemory(nautilus::val<size_t>(spanningTupleData.getTotalSize())).value);
            if (spanningTupleData.hasLeadingSpanningTuple())
            {
                processSpanningTuple<IndexerMetaData>(
                    stagedBuffers, spanningTupleData.getLeadingSpanningTuplePointer(), inputFormatterTask->indexerMetaData);
                inputFormatterTask->inputFormatIndexer->indexRawBuffer(
                    spanningTupleData.getLeadingSpanningTupleFIF(),
                    RawTupleBuffer{
                        std::bit_cast<const char*>(spanningTupleData.getLeadingSpanningTuplePointer()),
                        spanningTupleData.getSizeOfLeadingSpanningTuple()},
                    inputFormatterTask->indexerMetaData);
            }
        }
        return &spanningTupleData;
    }

    void scanTask(
        ExecutionContext& executionCtx,
        Nautilus::RecordBuffer& recordBuffer,
        const PhysicalOperator& child,
        const std::vector<Record::RecordFieldIdentifier>& projections,
        const size_t configuredBufferSize,
        const bool)
    requires(FormatterType::IsFormattingRequired and HasSpanningTuple)
    {
        /// initialize global state variables to keep track of the watermark ts and the origin id
        executionCtx.watermarkTs = recordBuffer.getWatermarkTs();
        executionCtx.originId = recordBuffer.getOriginId();
        executionCtx.currentTs = recordBuffer.getCreatingTs();
        executionCtx.sequenceNumber = recordBuffer.getSequenceNumber();
        executionCtx.chunkNumber = recordBuffer.getChunkNumber();
        executionCtx.lastChunk = recordBuffer.isLastChunk();

        /// call open on all child operators
        child.open(executionCtx, recordBuffer);

        /// index raw tuple buffer, resolve and index spanning tuples(SequenceShredder) and return pointers to resolved spanning tuples, if exist
        auto spanningTupleData = nautilus::invoke(
            indexTuplesProxy,
            recordBuffer.getReference(),
            executionCtx.pipelineContext,
            nautilus::val<InputFormatterTask*>(this), //Todo: figure out if works (might though, because the address is tied to the (pipeline) operator and thus l'stable'
            nautilus::val<size_t>(configuredBufferSize),
            nautilus::val<ExecutionContext*>(&executionCtx));

        // Todo: replace invokes with smarter approach? <-- can we somehow trace writing the below there values?
        // .. can't 'hardcode' thread_local address into traced code, since other threads will have other thread_local state
        /// parse leading spanning tuple if exists
        if (const auto hasLeadingST = nautilus::invoke(
                +[](SpanningTupleData* finalSpanningTupleData) { return finalSpanningTupleData->hasLeadingSpanningTuple(); },
                spanningTupleData))
        {
            auto recordIndex = nautilus::val<uint64_t>(0);
            // Todo: get rid of the invoke call (analog for trailing)
            auto recordPtr = nautilus::invoke(
                +[](SpanningTupleData* finalSpanningTupleData) { return finalSpanningTupleData->getLeadingSpanningTuplePointer(); },
                spanningTupleData);

            auto leadingFieldAccessFunction = nautilus::invoke(
                +[](SpanningTupleData* finalSpanningTupleData) { return &finalSpanningTupleData->getLeadingSpanningTupleFIF(); },
                spanningTupleData);

            // Todo: we need to access 'readSpanningRecord' using 'value' here, since it is a wrapped nautilus val pointer
            auto record = leadingFieldAccessFunction.value->readSpanningRecord(projections, recordPtr, recordIndex, indexerMetaData, parseFunctions, leadingFieldAccessFunction);
            child.execute(executionCtx, record);
        }

        /// parse raw tuple buffer (if there are complete tuples in it)
        const nautilus::val<uint64_t> totalNumberOfTuples = nautilus::invoke(
            +[](SpanningTupleData* spanningTupleData) { return spanningTupleData->getRawBufferFIF().getTotalNumberOfTuples(); },
            spanningTupleData);

        auto rawFieldAccessFunction = nautilus::invoke(
            +[](SpanningTupleData* finalSpanningTupleData) { return &finalSpanningTupleData->getRawBufferFIF(); },
            spanningTupleData);

        for (nautilus::val<uint64_t> i = 0_u64; i < totalNumberOfTuples; i = i + 1_u64)
        {
            auto record = rawFieldAccessFunction.value->readSpanningRecord(
                projections, recordBuffer.getBuffer(), i, indexerMetaData, parseFunctions, rawFieldAccessFunction);
            child.execute(executionCtx, record);
        }

        /// parse trailing spanning tuple if exists
        if (const auto hasTrailingST = nautilus::invoke(
                +[](SpanningTupleData* finalSpanningTupleData) { return finalSpanningTupleData->hasTrailingSpanningTuple(); },
                spanningTupleData))
        {
            auto recordIndex = nautilus::val<uint64_t>(0);
            auto recordPtr = nautilus::invoke(
                +[](SpanningTupleData* finalSpanningTupleData) { return finalSpanningTupleData->getTrailingTuplePointer(); },
                spanningTupleData);

            auto trailingFieldAccessFunction = nautilus::invoke(
                +[](SpanningTupleData* finalSpanningTupleData) { return &finalSpanningTupleData->getTrailingSpanningTupleFIF(); },
                spanningTupleData);

            auto record = trailingFieldAccessFunction.value->readSpanningRecord(
                projections, recordPtr, recordIndex, indexerMetaData, parseFunctions, trailingFieldAccessFunction);
            child.execute(executionCtx, record);
        }
    }

    void scanTask(
        ExecutionContext& executionCtx,
        Nautilus::RecordBuffer& recordBuffer,
        const PhysicalOperator& child,
        const std::vector<Record::RecordFieldIdentifier>& projections,
        const size_t configuredBufferSize,
        const bool isFirstOperatorAfterSource)
    requires(not(FormatterType::IsFormattingRequired) and not(HasSpanningTuple))
    {
        /// initialize global state variables to keep track of the watermark ts and the origin id
        executionCtx.watermarkTs = recordBuffer.getWatermarkTs();
        executionCtx.originId = recordBuffer.getOriginId();
        executionCtx.currentTs = recordBuffer.getCreatingTs();
        executionCtx.sequenceNumber = recordBuffer.getSequenceNumber();
        executionCtx.chunkNumber = recordBuffer.getChunkNumber();
        executionCtx.lastChunk = recordBuffer.isLastChunk();
        /// call open on all child operators
        child.open(executionCtx, recordBuffer);

        /// The source sets the number of read bytes, instead of the number of records.
        // Todo: make sure numRecords() returns a multiple of sizeOfTupleInBytes
        auto numberOfRecords = (isFirstOperatorAfterSource)
            ? recordBuffer.getNumRecords() / nautilus::static_val<uint64_t>(this->schemaInfo.getSizeOfTupleInBytes())
            : recordBuffer.getNumRecords();

        auto fieldIndexFunction = FieldIndexFunctionType();
        for (nautilus::val<uint64_t> i = 0_u64; i < numberOfRecords; i = i + 1_u64)
        {
            auto record
                = fieldIndexFunction.readNextRecord(projections, recordBuffer, i, indexerMetaData, configuredBufferSize, parseFunctions);
            child.execute(executionCtx, record);
        }
    }

    std::ostream& taskToString(std::ostream& os) const
    {
        /// Not using fmt::format, because it fails during build, trying to pass sequenceShredder as a const value
        os << "InputFormatterTask(originId: " << originId << ", inputFormatIndexer: " << *inputFormatIndexer
           << ", sequenceShredder: " << *sequenceShredder << ")\n";
        return os;
    }

private:
    OriginId originId;
    std::unique_ptr<InputFormatIndexer<FieldIndexFunctionType, IndexerMetaData, FormatterType::IsFormattingRequired>>
        inputFormatIndexer; /// unique_ptr, because InputFormatIndexer is abstract class
    SchemaInfo schemaInfo;
    IndexerMetaData indexerMetaData;
    std::unique_ptr<SequenceShredder> sequenceShredder; /// unique_ptr, because mutex is not copiable
    std::vector<RawValueParser::ParseFunctionSignature> parseFunctions;
};

}
