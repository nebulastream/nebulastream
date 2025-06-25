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

inline void setMetadataOfFormattedBuffer(
    const Memory::TupleBuffer& rawBuffer,
    Memory::TupleBuffer& formattedBuffer,
    ChunkNumber::Underlying& runningChunkNumber,
    const bool isLastChunk)
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
        + ((leftOverTuplesInFirstBuffer % numberOfTuplesPerBuffer != 0u) ? (leftOverTuplesInFirstBuffer / numberOfTuplesPerBuffer) + 1
                                                                         : leftOverTuplesInFirstBuffer / numberOfTuplesPerBuffer);
    return numberOfBuffersToFill;
}

/// Takes a view over the raw bytes of a tuple, and a fieldIndexFunction that knows the field offsets in the raw bytes of the tuple.
/// Iterates over all fields of the tuple and parses each field using the corresponding 'parse function'.
template <typename FieldIndexFunctionType>
void processTuple(
    const std::string_view tupleView,
    const FieldIndexFunction<FieldIndexFunctionType>& fieldIndexFunction,
    const size_t numTuplesReadFromRawBuffer,
    Memory::TupleBuffer& formattedBuffer,
    const SchemaInfo& schemaInfo,
    const std::vector<RawValueParser::ParseFunctionSignature>& parseFunctions,
    Memory::AbstractBufferProvider& bufferProvider /// for getting unpooled buffers for varsized data
)
{
    const size_t currentTupleIdx = formattedBuffer.getNumberOfTuples();
    const size_t offsetOfCurrentTupleInBytes = currentTupleIdx * schemaInfo.getSizeOfTupleInBytes();
    size_t offsetOfCurrentFieldInBytes = 0;

    /// Currently, we still allow only row-wise writing to the formatted buffer
    /// This will will change with #496, which implements the InputFormatterTask in Nautilus
    /// The InputFormatterTask then becomes part of a pipeline with a scan/emit phase and has access to the MemoryProvider
    for (size_t fieldIndex = 0; fieldIndex < schemaInfo.getFieldSizesInBytes().size(); ++fieldIndex)
    {
        /// Get the current field, parse it, and write it to the correct position in the formatted buffer
        const auto currentFieldSV = fieldIndexFunction.readFieldAt(tupleView, numTuplesReadFromRawBuffer, fieldIndex);
        const auto writeOffsetInBytes = offsetOfCurrentTupleInBytes + offsetOfCurrentFieldInBytes;
        parseFunctions[fieldIndex](currentFieldSV, writeOffsetInBytes, bufferProvider, formattedBuffer);

        /// Add the size of the current field to the running offset to get the offset of the next field
        const auto sizeOfCurrentFieldInBytes = schemaInfo.getFieldSizesInBytes().at(fieldIndex);
        offsetOfCurrentFieldInBytes += sizeOfCurrentFieldInBytes;
    }
}

/// Constructs a spanning tuple (string) that spans over at least two buffers (buffersToFormat).
/// First, determines the start of the spanning tuple in the first buffer to format. Constructs a spanning tuple from the required bytes.
/// Second, appends all bytes of all raw buffers that are not the last buffer to the spanning tuple.
/// Third, determines the end of the spanning tuple in the last buffer to format. Appends the required bytes to the spanning tuple.
/// Lastly, formats the full spanning tuple.
template <IndexerMetaDataType IndexerMetaData>
bool processSpanningTuple(
    const std::vector<StagedBuffer> stagedBuffersSpan,
    int8_t* spanningTuplePointer, /// is of the right size
    // Memory::AbstractBufferProvider& bufferProvider,
    // Memory::TupleBuffer& formattedBuffer,
    // const SchemaInfo& schemaInfo,
    const IndexerMetaData& indexerMetaData
    // const InputFormatIndexer<FieldIndexFunctionType, IndexerMetaData, IsFormattingRequired>& inputFormatIndexer,
    // const std::vector<RawValueParser::ParseFunctionSignature>& parseFunctions
)
{
    INVARIANT(stagedBuffersSpan.size() >= 2, "A spanning tuple must span across at least two buffers");
    /// If the buffers are not empty, there are at least three buffers
    std::stringstream spanningTupleStringStream;
    spanningTupleStringStream << indexerMetaData.getTupleDelimitingBytes();

    auto firstBuffer = stagedBuffersSpan.front();
    const auto firstSpanningTuple
        = (firstBuffer.isValidRawBuffer()) ? firstBuffer.getTrailingBytes(indexerMetaData.getTupleDelimitingBytes().size()) : "";
    spanningTupleStringStream << firstSpanningTuple;

    /// Process all buffers in-between the first and the last
    for (const auto middleBuffers = stagedBuffersSpan | std::views::drop(1) | std::views::take(stagedBuffersSpan.size() - 2);
         const auto& buffer : middleBuffers)
    {
        spanningTupleStringStream << buffer.getBufferView();
    }

    auto lastBuffer = stagedBuffersSpan.back();
    spanningTupleStringStream << lastBuffer.getLeadingBytes();
    spanningTupleStringStream << indexerMetaData.getTupleDelimitingBytes();

    const std::string completeSpanningTuple(spanningTupleStringStream.str());

    const auto sizeOfLeadingAndTrailingTupleDelimiter = 2 * indexerMetaData.getTupleDelimitingBytes().size();
    if (completeSpanningTuple.size() > sizeOfLeadingAndTrailingTupleDelimiter)
    {
        memcpy(spanningTuplePointer, completeSpanningTuple.data(), completeSpanningTuple.size());
        return true;
        // auto fieldIndexFunction = FieldIndexFunctionType(bufferProvider);
        // lastBuffer.setSpanningTuple(completeSpanningTuple);
        // inputFormatIndexer.indexRawBuffer(fieldIndexFunction, lastBuffer.getRawTupleBuffer(), indexerMetaData);
        // processTuple<FieldIndexFunctionType>(
        //     completeSpanningTuple, fieldIndexFunction, 0, formattedBuffer, schemaInfo, parseFunctions, bufferProvider);
        // formattedBuffer.setNumberOfTuples(formattedBuffer.getNumberOfTuples() + 1);
    }
    return false;
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
        SpanningTupleData() = default;

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

    private:
        int8_t* spanningTuplePtr = nullptr;
        size_t leadingSpanningTupleSizeInBytes = 0;
        size_t trailingSpanningTupleSizeInBytes = 0;
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

    struct FieldIndexFunctions
    {
        FieldIndexFunctionType leadingSpanningTupleFIF;
        FieldIndexFunctionType trailingSpanningTupleFIF;
        FieldIndexFunctionType rawBufferFIF;
    };

    static SpanningTupleData* indexTuplesProxy(
        const Memory::TupleBuffer* tupleBuffer,
        PipelineExecutionContext* pec,
        InputFormatterTask* inputFormatterTask,
        FieldIndexFunctions* fieldIndexFunctions,
        const size_t configuredBufferSize,
        ExecutionContext* executionCtx)
    {

        thread_local SpanningTupleData spanningTupleData{};
        if (not inputFormatterTask->sequenceShredder->isInRange(tupleBuffer->getSequenceNumber().getRawValue()))
        {
            pec->emitBuffer(*tupleBuffer, PipelineExecutionContext::ContinuationPolicy::REPEAT);
            return &spanningTupleData;
        }
        inputFormatterTask->inputFormatIndexer->indexRawBuffer(fieldIndexFunctions->rawBufferFIF, RawTupleBuffer{*tupleBuffer}, inputFormatterTask->indexerMetaData);

        if (/*hasTupleDelimiter*/ fieldIndexFunctions->rawBufferFIF.getOffsetOfFirstTupleDelimiter() < configuredBufferSize)
        {
            const auto [indexOfSequenceNumberInStagedBuffers, stagedBuffers]
                = inputFormatterTask->sequenceShredder->processSequenceNumber<true>(
                    StagedBuffer{
                        RawTupleBuffer{*tupleBuffer},
                        tupleBuffer->getNumberOfTuples(), /// size in bytes
                        fieldIndexFunctions->rawBufferFIF.getOffsetOfFirstTupleDelimiter(),
                        fieldIndexFunctions->rawBufferFIF.getOffsetOfLastTupleDelimiter()},
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
                // Todo: don't return bool from 'processSpanningTuple'? <-- should be guaranteed at this point
                if (processSpanningTuple<IndexerMetaData>(
                        stagedBuffers, spanningTupleData.getLeadingSpanningTuplePointer(), inputFormatterTask->indexerMetaData))
                {
                    // Todo: create RawTupleBuffer from spanningTupleData pointer and pass to inputFormatIndexer
                    inputFormatterTask->inputFormatIndexer->indexRawBuffer(
                        fieldIndexFunctions->leadingSpanningTupleFIF,
                        RawTupleBuffer{
                            std::bit_cast<const char*>(spanningTupleData.getLeadingSpanningTuplePointer()),
                            spanningTupleData.getSizeOfLeadingSpanningTuple()},
                        inputFormatterTask->indexerMetaData);
                }
            }
            if (spanningTupleData.hasTrailingSpanningTuple())
            {
                if (processSpanningTuple<IndexerMetaData>(
                        stagedBuffers, spanningTupleData.getTrailingTuplePointer(), inputFormatterTask->indexerMetaData))
                {
                    inputFormatterTask->inputFormatIndexer->indexRawBuffer(
                        fieldIndexFunctions->trailingSpanningTupleFIF,
                        RawTupleBuffer{
                            std::bit_cast<const char*>(spanningTupleData.getTrailingTuplePointer()),
                            spanningTupleData.getSizeOfTrailingSpanningTuple()},
                        inputFormatterTask->indexerMetaData);
                }
            }
        }
        else
        {
            const auto [indexOfSequenceNumberInStagedBuffers, stagedBuffers]
                = inputFormatterTask->sequenceShredder->processSequenceNumber<false>(
                    StagedBuffer{
                        RawTupleBuffer{*tupleBuffer},
                        tupleBuffer->getNumberOfTuples(), /// size in bytes
                        fieldIndexFunctions->rawBufferFIF.getOffsetOfFirstTupleDelimiter(),
                        fieldIndexFunctions->rawBufferFIF.getOffsetOfLastTupleDelimiter()},
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
                if (processSpanningTuple<IndexerMetaData>(
                        stagedBuffers, spanningTupleData.getLeadingSpanningTuplePointer(), inputFormatterTask->indexerMetaData))
                {
                    inputFormatterTask->inputFormatIndexer->indexRawBuffer(
                        fieldIndexFunctions->leadingSpanningTupleFIF,
                        RawTupleBuffer{
                            std::bit_cast<const char*>(spanningTupleData.getLeadingSpanningTuplePointer()),
                            spanningTupleData.getSizeOfLeadingSpanningTuple()},
                        inputFormatterTask->indexerMetaData);
                }
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
        // Todo: allocate fixSizeBufferPool with 1 buffer and buffer size that matches exactly required number of indexes for spanning tuples?
        // Todo: Wrap spanning tuples in struct and return pointer to struct and decompose that struct
        auto fieldIndexFunctions = nautilus::invoke(
            +[](const PipelineExecutionContext* pec)
            {
                thread_local FieldIndexFunctions threadLocalFieldIndexFunctions = FieldIndexFunctions{
                    .leadingSpanningTupleFIF = FieldIndexFunctionType(*pec->getBufferManager()),
                    .trailingSpanningTupleFIF = FieldIndexFunctionType(*pec->getBufferManager()),
                    .rawBufferFIF = FieldIndexFunctionType(*pec->getBufferManager())};
                return &threadLocalFieldIndexFunctions;
            },
            executionCtx.pipelineContext);

        auto spanningTupleData = nautilus::invoke(
            indexTuplesProxy,
            recordBuffer.getReference(),
            executionCtx.pipelineContext,
            nautilus::val<InputFormatterTask*>(this),
            fieldIndexFunctions,
            nautilus::val<size_t>(configuredBufferSize),
            nautilus::val<ExecutionContext*>(&executionCtx));

        // Todo: replace invokes with smarter approach? <-- can we somehow trace writing the below there values?
        if (const auto hasLeadingST = nautilus::invoke(
                +[](SpanningTupleData* finalSpanningTupleData) { return finalSpanningTupleData->hasLeadingSpanningTuple(); },
                spanningTupleData))
        {
            auto recordIndex = nautilus::val<uint64_t>(0);
            auto recordPtr = nautilus::val<int8_t*>(spanningTupleData.value->getLeadingSpanningTuplePointer());
            auto record = fieldIndexFunctions.value->leadingSpanningTupleFIF.readSpanningRecord(
                projections, recordPtr, recordIndex, indexerMetaData, parseFunctions);
            child.execute(executionCtx, record);
        }
        const nautilus::val<uint64_t> totalNumberOfTuples = nautilus::invoke(
            +[](FieldIndexFunctions* fieldIndexFunctions) { return fieldIndexFunctions->rawBufferFIF.getTotalNumberOfTuples(); },
            fieldIndexFunctions);
        for (nautilus::val<uint64_t> i = 0_u64; i < totalNumberOfTuples; i = i + 1_u64)
        {
            auto record = fieldIndexFunctions.value->rawBufferFIF.readNextRecord(
                projections, recordBuffer, i, indexerMetaData, configuredBufferSize, parseFunctions);
            child.execute(executionCtx, record);
        }
        if (const auto hasTrailingST = nautilus::invoke(
                +[](SpanningTupleData* finalSpanningTupleData) { return finalSpanningTupleData->hasTrailingSpanningTuple(); },
                spanningTupleData))
        {
            auto recordIndex = nautilus::val<uint64_t>(0);
            auto recordPtr = nautilus::val<int8_t*>(spanningTupleData.value->getTrailingTuplePointer());
            auto record = fieldIndexFunctions.value->trailingSpanningTupleFIF.readSpanningRecord(
                projections, recordPtr, recordIndex, indexerMetaData, parseFunctions);
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

    /// Called by processRawBufferWithTupleDelimiter if the raw buffer contains at least one full tuple.
    /// Iterates over all full tuples, using the indexes in FieldOffsets and parses the tuples into formatted data.
    void parseRawBuffer(
        const RawTupleBuffer& rawBuffer,
        ChunkNumber::Underlying& runningChunkNumber,
        const FieldIndexFunction<FieldIndexFunctionType>& fieldIndexFunction,
        Memory::TupleBuffer& formattedBuffer,
        PipelineExecutionContext& pec) const
    {
        const auto bufferProvider = pec.getBufferManager();
        const auto numberOfTuplesInFirstFormattedBuffer = formattedBuffer.getNumberOfTuples();
        const size_t numberOfTuplesPerBuffer = bufferProvider->getBufferSize() / this->schemaInfo.getSizeOfTupleInBytes();
        PRECONDITION(numberOfTuplesPerBuffer != 0, "The capacity of a buffer must suffice to hold at least one tuple.");
        const auto numberOfBuffersToFill = calculateNumberOfRequiredFormattedBuffers(
            fieldIndexFunction.getTotalNumberOfTuples(), numberOfTuplesInFirstFormattedBuffer, numberOfTuplesPerBuffer);

        /// Determine the total number of tuples to produce, including potential prior (spanning) tuples
        /// If the first buffer is full already, the first iteration of the for loop below does 'nothing'
        size_t numberOfFormattedTuplesToProduce = fieldIndexFunction.getTotalNumberOfTuples() + numberOfTuplesInFirstFormattedBuffer;
        size_t numTuplesReadFromRawBuffer = 0;

        /// Initialize indexes for offset buffer
        for (size_t bufferIdx = 0; bufferIdx < numberOfBuffersToFill; ++bufferIdx)
        {
            /// Either fill the entire buffer, or process the leftover formatted tuples to produce
            const size_t numberOfTuplesToRead = std::min(numberOfTuplesPerBuffer, numberOfFormattedTuplesToProduce);
            /// If the current buffer is not the first buffer, set the meta data of the prior buffer, emit it, get a new buffer and reset the associated counters
            if (bufferIdx != 0)
            {
                /// The current raw buffer produces more than one formatted buffer.
                /// Each formatted buffer has the sequence number of the raw buffer and a chunk number that uniquely identifies it.
                /// Only the last formatted buffer sets the 'isLastChunk' member to true.
                setMetadataOfFormattedBuffer(rawBuffer.getRawBuffer(), formattedBuffer, runningChunkNumber, false);
                pec.emitBuffer(formattedBuffer, PipelineExecutionContext::ContinuationPolicy::POSSIBLE);
                /// The 'isLastChunk' member of a new buffer is true pre default. If we don't require another buffer, the flag stays true.
                formattedBuffer = bufferProvider->getBufferBlocking();
            }

            /// Fill current buffer until either full, or we exhausted tuples in raw buffer
            while (formattedBuffer.getNumberOfTuples() < numberOfTuplesToRead)
            {
                processTuple<FieldIndexFunctionType>(
                    rawBuffer.getBufferView(),
                    fieldIndexFunction,
                    numTuplesReadFromRawBuffer,
                    formattedBuffer,
                    this->schemaInfo,
                    this->parseFunctions,
                    *bufferProvider);
                formattedBuffer.setNumberOfTuples(formattedBuffer.getNumberOfTuples() + 1);
                ++numTuplesReadFromRawBuffer;
            }
            numberOfFormattedTuplesToProduce -= formattedBuffer.getNumberOfTuples();
        }
    }

    /// Called by execute if the buffer delimits at least two tuples.
    /// First, processes the leading spanning tuple, if the raw buffer completed it.
    /// Second, processes the raw buffer, if it contains at least one full tuple.
    /// Third, processes the trailing spanning tuple, if the raw buffer completed it.
    void processRawBufferWithTupleDelimiter(
        const RawTupleBuffer& rawBuffer,
        ChunkNumber::Underlying& runningChunkNumber,
        const FieldIndexFunction<FieldIndexFunctionType>& fieldIndexFunction,
        PipelineExecutionContext& pec) const
    {
        const auto bufferProvider = pec.getBufferManager();
        const auto [indexOfSequenceNumberInStagedBuffers, stagedBuffers] = sequenceShredder->processSequenceNumber<true>(
            StagedBuffer{
                rawBuffer,
                rawBuffer.getNumberOfBytes(),
                fieldIndexFunction.getOffsetOfFirstTupleDelimiter(),
                fieldIndexFunction.getOffsetOfLastTupleDelimiter()},
            rawBuffer.getSequenceNumber().getRawValue());

        /// 1. process leading spanning tuple if required
        auto formattedBuffer = bufferProvider->getBufferBlocking();
        if (/* hasLeadingSpanningTuple */ indexOfSequenceNumberInStagedBuffers != 0)
        {
            const auto spanningTupleBuffers = std::span(stagedBuffers).subspan(0, indexOfSequenceNumberInStagedBuffers + 1);
            processSpanningTuple<FieldIndexFunctionType>(
                spanningTupleBuffers,
                *bufferProvider,
                formattedBuffer,
                this->schemaInfo,
                this->indexerMetaData,
                *this->inputFormatIndexer,
                this->parseFunctions);
        }

        /// 2. process tuples in buffer
        // if (fieldIndexFunction.getTotalNumberOfTuples() > 0)
        // {
        //     parseRawBuffer(rawBuffer, runningChunkNumber, fieldIndexFunction, formattedBuffer, pec);
        // }

        /// 3. process trailing spanning tuple if required
        if (/* hasTrailingSpanningTuple */ indexOfSequenceNumberInStagedBuffers < (stagedBuffers.size() - 1))
        {
            const auto numBytesInFormattedBuffer = formattedBuffer.getNumberOfTuples() * this->schemaInfo.getSizeOfTupleInBytes();
            if (formattedBuffer.getBufferSize() - numBytesInFormattedBuffer < this->schemaInfo.getSizeOfTupleInBytes())
            {
                formattedBuffer.setSequenceNumber(rawBuffer.getSequenceNumber());
                formattedBuffer.setChunkNumber(NES::ChunkNumber(runningChunkNumber++));
                formattedBuffer.setOriginId(rawBuffer.getOriginId());
                pec.emitBuffer(formattedBuffer, NES::PipelineExecutionContext::ContinuationPolicy::POSSIBLE);
                formattedBuffer = bufferProvider->getBufferBlocking();
            }

            const auto spanningTupleBuffers
                = std::span(stagedBuffers)
                      .subspan(indexOfSequenceNumberInStagedBuffers, stagedBuffers.size() - indexOfSequenceNumberInStagedBuffers);
            processSpanningTuple<FieldIndexFunctionType>(
                spanningTupleBuffers,
                *bufferProvider,
                formattedBuffer,
                this->schemaInfo,
                this->indexerMetaData,
                *this->inputFormatIndexer,
                this->parseFunctions);
        }
        /// If a raw buffer contains exactly one delimiter, but does not complete a spanning tuple, the formatted buffer does not contain a tuple
        if (formattedBuffer.getNumberOfTuples() != 0)
        {
            formattedBuffer.setSequenceNumber(rawBuffer.getSequenceNumber());
            formattedBuffer.setChunkNumber(NES::ChunkNumber(runningChunkNumber++));
            formattedBuffer.setOriginId(rawBuffer.getOriginId());
            pec.emitBuffer(formattedBuffer, NES::PipelineExecutionContext::ContinuationPolicy::POSSIBLE);
        }
    }

    /// Called by execute, if the buffer does not delimit any tuples.
    /// Processes a spanning tuple, if the raw buffer connects two raw buffers that delimit tuples.
    void processRawBufferWithoutTupleDelimiter(
        const RawTupleBuffer& rawBuffer,
        ChunkNumber::Underlying& runningChunkNumber,
        const FieldIndexFunction<FieldIndexFunctionType>& fieldIndexFunction,
        PipelineExecutionContext& pec) const
    {
        const auto bufferProvider = pec.getBufferManager();
        const auto [indexOfSequenceNumberInStagedBuffers, stagedBuffers] = sequenceShredder->processSequenceNumber<false>(
            StagedBuffer{
                rawBuffer,
                rawBuffer.getNumberOfBytes(),
                fieldIndexFunction.getOffsetOfFirstTupleDelimiter(),
                fieldIndexFunction.getOffsetOfLastTupleDelimiter()},
            rawBuffer.getSequenceNumber().getRawValue());
        if (stagedBuffers.size() < 3)
        {
            return;
        }
        /// If there is a spanning tuple, get a new buffer for formatted data and process the spanning tuples
        auto formattedBuffer = bufferProvider->getBufferBlocking();
        processSpanningTuple<FieldIndexFunctionType>(
            stagedBuffers,
            *bufferProvider,
            formattedBuffer,
            this->schemaInfo,
            this->indexerMetaData,
            *this->inputFormatIndexer,
            this->parseFunctions);

        formattedBuffer.setSequenceNumber(rawBuffer.getSequenceNumber());
        formattedBuffer.setChunkNumber(NES::ChunkNumber(runningChunkNumber++));
        formattedBuffer.setOriginId(rawBuffer.getOriginId());
        pec.emitBuffer(formattedBuffer, NES::PipelineExecutionContext::ContinuationPolicy::POSSIBLE);
    }
};

}
