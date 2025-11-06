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
#include <InputFormatters/InputFormatterTaskPipeline.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Concepts.hpp>
#include <ErrorHandling.hpp>
#include <FieldIndexFunction.hpp>
#include <PipelineExecutionContext.hpp>
#include <RawTupleBuffer.hpp>
#include <RawValueParser.hpp>
#include <SequenceShredder.hpp>

#include <NautilusUtil.hpp>
#include <Nautilus/Interface/MemoryProvider/TupleBufferMemoryProvider.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Util/StdInt.hpp>

namespace NES::InputFormatters
{
/// The type that all formatters use to represent indexes to fields.
using FieldIndex = uint32_t;

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
        + ((leftOverTuplesInFirstBuffer % numberOfTuplesPerBuffer != 0U) ? (leftOverTuplesInFirstBuffer / numberOfTuplesPerBuffer) + 1
                                                                         : leftOverTuplesInFirstBuffer / numberOfTuplesPerBuffer);
    return numberOfBuffersToFill;
}

/// Takes a view over the raw bytes of a tuple, and a fieldIndexFunction that knows the field offsets in the raw bytes of the tuple.
/// Iterates over all fields of the tuple and parses each field using the corresponding 'parse function'.
template <typename FormatterType>
void processTuple(
    const std::string_view tupleView,
    const FieldIndexFunction<FormatterType>& fieldIndexFunction,
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
void processSpanningTuple(
    const std::span<const StagedBuffer>& stagedBuffersSpan,
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
// template <InputFormatIndexerType FormatterType>
// void processSpanningTuple(
//     const std::span<const StagedBuffer> stagedBuffersSpan,
//     Memory::AbstractBufferProvider& bufferProvider,
//     Memory::TupleBuffer& formattedBuffer,
//     const SchemaInfo& schemaInfo,
//     const typename FormatterType::IndexerMetaData& indexerMetaData,
//     const FormatterType& inputFormatIndexer,
//     const std::vector<RawValueParser::ParseFunctionSignature>& parseFunctions)
// {
//     INVARIANT(stagedBuffersSpan.size() >= 2, "A spanning tuple must span across at least two buffers");
//     /// If the buffers are not empty, there are at least three buffers
//     std::stringstream spanningTupleStringStream;
//     spanningTupleStringStream << indexerMetaData.getTupleDelimitingBytes();
//
//     const auto& firstBuffer = stagedBuffersSpan.front();
//     const auto firstSpanningTuple = firstBuffer.getTrailingBytes(indexerMetaData.getTupleDelimitingBytes().size());
//     spanningTupleStringStream << firstSpanningTuple;
//
//     /// Process all buffers in-between the first and the last
//     for (const auto middleBuffers = stagedBuffersSpan | std::views::drop(1) | std::views::take(stagedBuffersSpan.size() - 2);
//          const auto& buffer : middleBuffers)
//     {
//         spanningTupleStringStream << buffer.getBufferView();
//     }
//
//     auto lastBuffer = stagedBuffersSpan.back();
//     spanningTupleStringStream << lastBuffer.getLeadingBytes();
//     spanningTupleStringStream << indexerMetaData.getTupleDelimitingBytes();
//
//     const std::string completeSpanningTuple(spanningTupleStringStream.str());
//     const auto sizeOfLeadingAndTrailingTupleDelimiter = 2 * indexerMetaData.getTupleDelimitingBytes().size();
//     if (completeSpanningTuple.size() > sizeOfLeadingAndTrailingTupleDelimiter)
//     {
//         auto fieldIndexFunction = typename FormatterType::FormatterType(bufferProvider);
//         lastBuffer.setSpanningTuple(completeSpanningTuple);
//         inputFormatIndexer.indexRawBuffer(fieldIndexFunction, lastBuffer.getRawTupleBuffer(), indexerMetaData);
//         processTuple<typename FormatterType::FormatterType>(
//             completeSpanningTuple, fieldIndexFunction, 0, formattedBuffer, schemaInfo, parseFunctions, bufferProvider);
//         formattedBuffer.setNumberOfTuples(formattedBuffer.getNumberOfTuples() + 1);
//     }
// }

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
    static constexpr bool hasSpanningTuple() { return FormatterType::HasSpanningTuple; }

    explicit InputFormatterTask(
        const OriginId originId, FormatterType inputFormatIndexer, const Schema& schema, const ParserConfig& parserConfig)
        : originId(originId)
        , inputFormatIndexer(std::move(inputFormatIndexer))
        , schemaInfo(schema)
        , indexerMetaData(typename FormatterType::IndexerMetaData{parserConfig, schema})
        /// Only if we need to resolve spanning tuples, we need the SequenceShredder
        , sequenceShredder(hasSpanningTuple() ? std::make_unique<SequenceShredder>(parserConfig.tupleDelimiter.size()) : nullptr)
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
        if constexpr (hasSpanningTuple())
        {
            INVARIANT(sequenceShredder != nullptr, "The SequenceShredder handles spanning tuples, thus it must not be null.");
        }
    }

    struct SpanningTuplePOD
    {
        int8_t* leadingSpanningTuplePtr = nullptr;
        int8_t* trailingSpanningTuplePtr = nullptr;
        bool hasLeadingSpanningTupleBool = false;
        bool hasTrailingSpanningTupleBool = false;
        uint64_t totalNumberOfTuples = 0;

        typename FormatterType::FieldIndexFunctionType leadingSpanningTupleFIF;
        typename FormatterType::FieldIndexFunctionType trailingSpanningTupleFIF;
        typename FormatterType::FieldIndexFunctionType rawBufferFIF;
    };

    /// accumulates all data produced during the indexing phase, which the parsing phase requires
    /// on calling 'createSpanningTuplePOD()' it returns this data
    class SpanningTupleData
    {
    public:
        // Todo: allocate fixSizeBufferPool with 1 buffer and buffer size that matches exactly required number of indexes for spanning tuples?
        explicit SpanningTupleData()
            : leadingSpanningTupleFIF(typename FormatterType::FieldIndexFunctionType())
            , trailingSpanningTupleFIF(typename FormatterType::FieldIndexFunctionType())
            , rawBufferFIF(typename FormatterType::FieldIndexFunctionType()) { };

        void setAllocatedMemory(int8_t* spanningTuplePtr) { this->spanningTuplePtr = spanningTuplePtr; }
        void increaseLeadingSpanningTupleSize(const size_t additionalBytes) { leadingSpanningTupleSizeInBytes += additionalBytes; }
        void increaseTrailingSpanningTupleSize(const size_t additionalBytes) { trailingSpanningTupleSizeInBytes += additionalBytes; }

        size_t getSizeOfLeadingSpanningTuple() const { return leadingSpanningTupleSizeInBytes; }
        size_t getSizeOfTrailingSpanningTuple() const { return trailingSpanningTupleSizeInBytes; }
        size_t getTotalSize() const { return leadingSpanningTupleSizeInBytes + trailingSpanningTupleSizeInBytes; }

        int8_t* getLeadingSpanningTuplePointer() const { return spanningTuplePtr; }
        int8_t* getTrailingTuplePointer() const { return spanningTuplePtr + leadingSpanningTupleSizeInBytes; }

        bool hasLeadingSpanningTuple() const { return leadingSpanningTupleSizeInBytes > 0; }
        bool hasTrailingSpanningTuple() const { return trailingSpanningTupleSizeInBytes > 0; }

        typename FormatterType::FieldIndexFunctionType& getLeadingSpanningTupleFIF() { return leadingSpanningTupleFIF; }
        typename FormatterType::FieldIndexFunctionType& getTrailingSpanningTupleFIF() { return trailingSpanningTupleFIF; }
        typename FormatterType::FieldIndexFunctionType& getRawBufferFIF() { return rawBufferFIF; }

        /// returns the accumulated data of the indexing phase (to the parsing phase)
        /// don't continue to use a SpanningTupleData object after calling this function (todo: enforce comment somehow)
        SpanningTuplePOD createSpanningTuplePOD()
        {
            return SpanningTuplePOD{
                .leadingSpanningTuplePtr = this->getLeadingSpanningTuplePointer(),
                .trailingSpanningTuplePtr = this->getTrailingTuplePointer(),
                .hasLeadingSpanningTupleBool = this->hasLeadingSpanningTuple(),
                .hasTrailingSpanningTupleBool = this->hasTrailingSpanningTuple(),
                .totalNumberOfTuples = this->getRawBufferFIF().getTotalNumberOfTuples(),
                .leadingSpanningTupleFIF = std::move(this->leadingSpanningTupleFIF),
                .trailingSpanningTupleFIF = std::move(this->trailingSpanningTupleFIF),
                .rawBufferFIF = std::move(this->rawBufferFIF)};
        }

    private:
        int8_t* spanningTuplePtr = nullptr;
        size_t leadingSpanningTupleSizeInBytes = 0;
        size_t trailingSpanningTupleSizeInBytes = 0;

        typename FormatterType::FieldIndexFunctionType leadingSpanningTupleFIF;
        typename FormatterType::FieldIndexFunctionType trailingSpanningTupleFIF;
        typename FormatterType::FieldIndexFunctionType rawBufferFIF;

        bool hasLeadingSpanningTupleBool = false;
        bool hasTrailingSpanningTupleBool = false;
        uint64_t totalNumberOfTuples = 0;
    };

    static void calculateSizeOfSpanningTuples(
        SpanningTupleData& spanningTupleData,
        const bool hasTupleDelimiter,
        const std::vector<StagedBuffer>& spanningTupleBuffers,
        const size_t indexOfSequenceNumberInStagedBuffers,
        const size_t sizeOfTupleDelimiterInBytes)
    {
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
                for (size_t i = 1; i < spanningTupleBuffers.size() - 1; ++i)
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
                for (size_t i = 1; i < spanningTupleBuffers.size() - 1; ++i)
                {
                    spanningTupleData.increaseTrailingSpanningTupleSize(spanningTupleBuffers[i].getSizeOfBufferInBytes());
                }
                spanningTupleData.increaseTrailingSpanningTupleSize(spanningTupleBuffers.back().getLeadingBytes().size());
                return;
            }
            if (hasLeadingST and hasTrailingST)
            {
                spanningTupleData.increaseLeadingSpanningTupleSize(2 * sizeOfTupleDelimiterInBytes);
                /// Size of leading spanning tuple
                spanningTupleData.increaseLeadingSpanningTupleSize(
                    spanningTupleBuffers.front().getTrailingBytes(sizeOfTupleDelimiterInBytes).size());
                for (size_t i = 1; i < indexOfSequenceNumberInStagedBuffers; ++i)
                {
                    spanningTupleData.increaseLeadingSpanningTupleSize(spanningTupleBuffers[i].getSizeOfBufferInBytes());
                }
                spanningTupleData.increaseLeadingSpanningTupleSize(
                    spanningTupleBuffers[indexOfSequenceNumberInStagedBuffers].getLeadingBytes().size());

                /// Size of trailing spanning tuple
                spanningTupleData.increaseTrailingSpanningTupleSize(2 * sizeOfTupleDelimiterInBytes);
                spanningTupleData.increaseTrailingSpanningTupleSize(
                    spanningTupleBuffers[indexOfSequenceNumberInStagedBuffers].getTrailingBytes(sizeOfTupleDelimiterInBytes).size());
                for (size_t i = indexOfSequenceNumberInStagedBuffers + 1; i < spanningTupleBuffers.size() - 1; ++i)
                {
                    spanningTupleData.increaseTrailingSpanningTupleSize(spanningTupleBuffers[i].getSizeOfBufferInBytes());
                }
                spanningTupleData.increaseTrailingSpanningTupleSize(spanningTupleBuffers.back().getLeadingBytes().size());
                return;
            }
        }
        else
        {
            /// the raw buffer had no delimiter, the spanning tuple consists of at least three buffers.
            /// add tuple delimiter padding, if format has tuple delimiter
            spanningTupleData.increaseLeadingSpanningTupleSize(2 * sizeOfTupleDelimiterInBytes);
            /// add size of trailing bytes behind last tuple delimiter (start of spanning tuple)
            spanningTupleData.increaseLeadingSpanningTupleSize(spanningTupleBuffers.back().getLeadingBytes().size());
            /// add sizes of buffers in between the two buffers with delimiters
            for (size_t i = 1; i < spanningTupleBuffers.size() - 1; ++i)
            {
                spanningTupleData.increaseLeadingSpanningTupleSize(spanningTupleBuffers[i].getSizeOfBufferInBytes());
            }
            /// add size of leading bytes in front of first tuple delimiter of the last tuple delimiter (end of spanning tuple)
            spanningTupleData.increaseLeadingSpanningTupleSize(
                spanningTupleBuffers.front().getTrailingBytes(sizeOfTupleDelimiterInBytes).size());
        }
    }

    static SpanningTuplePOD* indexTuplesProxy(
        const Memory::TupleBuffer* tupleBuffer,
        PipelineExecutionContext* pec,
        InputFormatterTask* inputFormatterTask,
        const size_t configuredBufferSize,
        Arena* arenaRef)
    {
        /// each thread sets up a static address for a POD struct that contains all relevent data that the indexing phase produces
        /// this allows this proxy function to return a, guaranteed to be valid, reference to the POD tho the compiled query pipeline
        thread_local SpanningTuplePOD spanningTuplePOD{};
        SpanningTupleData spanningTupleData{};
        // Todo: reenable
        // if (not inputFormatterTask->sequenceShredder->isInRange(tupleBuffer->getSequenceNumber().getRawValue()))
        // {
        //     pec->emitBuffer(*tupleBuffer, PipelineExecutionContext::ContinuationPolicy::REPEAT);
        //     spanningTuplePOD = spanningTupleData.createSpanningTuplePOD();
        //     return &spanningTuplePOD;
        // }

        inputFormatterTask->inputFormatIndexer.indexRawBuffer(
            spanningTupleData.getRawBufferFIF(), RawTupleBuffer{*tupleBuffer}, inputFormatterTask->indexerMetaData);

        if (const bool hasTupleDelimiter = spanningTupleData.getRawBufferFIF().getOffsetOfFirstTupleDelimiter() < configuredBufferSize)
        {
            const auto [isInRange, indexOfSequenceNumberInStagedBuffers, stagedBuffers]
                = inputFormatterTask->sequenceShredder->findSTsWithDelimiter(
                    StagedBuffer{
                        RawTupleBuffer{*tupleBuffer},
                        tupleBuffer->getNumberOfTuples(), /// size in bytes
                        spanningTupleData.getRawBufferFIF().getOffsetOfFirstTupleDelimiter(),
                        spanningTupleData.getRawBufferFIF().getOffsetOfLastTupleDelimiter()});

            if (not isInRange)
            {
                // Todo: we want to 'repeat' here
                pec->emitBuffer(*tupleBuffer, PipelineExecutionContext::ContinuationPolicy::REPEAT);
                spanningTuplePOD = spanningTupleData.createSpanningTuplePOD();
                return &spanningTuplePOD;
            }

            if (stagedBuffers.size() < 2)
            {
                spanningTuplePOD = spanningTupleData.createSpanningTuplePOD();
                return &spanningTuplePOD;
            }

            calculateSizeOfSpanningTuples(
                spanningTupleData,
                hasTupleDelimiter,
                stagedBuffers,
                indexOfSequenceNumberInStagedBuffers,
                inputFormatterTask->indexerMetaData.getTupleDelimitingBytes().size());
            spanningTupleData.setAllocatedMemory(arenaRef->allocateMemory(spanningTupleData.getTotalSize()));

            if (spanningTupleData.hasLeadingSpanningTuple())
            {
                const auto leadingSpanningTupleBuffers = std::span(stagedBuffers).subspan(0, indexOfSequenceNumberInStagedBuffers + 1);
                processSpanningTuple<typename FormatterType::IndexerMetaData>(
                    leadingSpanningTupleBuffers, spanningTupleData.getLeadingSpanningTuplePointer(), inputFormatterTask->indexerMetaData);
                inputFormatterTask->inputFormatIndexer.indexRawBuffer(
                    spanningTupleData.getLeadingSpanningTupleFIF(),
                    RawTupleBuffer{
                        std::bit_cast<const char*>(spanningTupleData.getLeadingSpanningTuplePointer()),
                        spanningTupleData.getSizeOfLeadingSpanningTuple()},
                    inputFormatterTask->indexerMetaData);
            }
            if (spanningTupleData.hasTrailingSpanningTuple())
            {
                const auto trailingSpanningTupleBuffers
                    = std::span(stagedBuffers)
                          .subspan(indexOfSequenceNumberInStagedBuffers, stagedBuffers.size() - indexOfSequenceNumberInStagedBuffers);
                processSpanningTuple<typename FormatterType::IndexerMetaData>(
                    trailingSpanningTupleBuffers, spanningTupleData.getTrailingTuplePointer(), inputFormatterTask->indexerMetaData);
                inputFormatterTask->inputFormatIndexer.indexRawBuffer(
                    spanningTupleData.getTrailingSpanningTupleFIF(),
                    RawTupleBuffer{
                        std::bit_cast<const char*>(spanningTupleData.getTrailingTuplePointer()),
                        spanningTupleData.getSizeOfTrailingSpanningTuple()},
                    inputFormatterTask->indexerMetaData);
            }
        }
        else
        {
            const auto [isInRange, indexOfSequenceNumberInStagedBuffers, stagedBuffers]
                = inputFormatterTask->sequenceShredder->findSTsWithoutDelimiter(
                    StagedBuffer{
                        RawTupleBuffer{*tupleBuffer},
                        tupleBuffer->getNumberOfTuples(), /// size in bytes
                        spanningTupleData.getRawBufferFIF().getOffsetOfFirstTupleDelimiter(),
                        spanningTupleData.getRawBufferFIF().getOffsetOfLastTupleDelimiter()});

            if (not isInRange)
            {
                pec->emitBuffer(*tupleBuffer, PipelineExecutionContext::ContinuationPolicy::REPEAT);
                spanningTuplePOD = spanningTupleData.createSpanningTuplePOD();
            }

            if (stagedBuffers.size() < 3)
            {
                spanningTuplePOD = spanningTupleData.createSpanningTuplePOD();
                return &spanningTuplePOD;
            }

            calculateSizeOfSpanningTuples(
                spanningTupleData,
                hasTupleDelimiter,
                stagedBuffers,
                indexOfSequenceNumberInStagedBuffers,
                inputFormatterTask->indexerMetaData.getTupleDelimitingBytes().size());
            spanningTupleData.setAllocatedMemory(arenaRef->allocateMemory(spanningTupleData.getTotalSize()));
            if (spanningTupleData.hasLeadingSpanningTuple())
            {
                processSpanningTuple<typename FormatterType::IndexerMetaData>(
                    stagedBuffers, spanningTupleData.getLeadingSpanningTuplePointer(), inputFormatterTask->indexerMetaData);
                inputFormatterTask->inputFormatIndexer.indexRawBuffer(
                    spanningTupleData.getLeadingSpanningTupleFIF(),
                    RawTupleBuffer{
                        std::bit_cast<const char*>(spanningTupleData.getLeadingSpanningTuplePointer()),
                        spanningTupleData.getSizeOfLeadingSpanningTuple()},
                    inputFormatterTask->indexerMetaData);
            }
        }
        spanningTuplePOD = spanningTupleData.createSpanningTuplePOD();
        return &spanningTuplePOD;
    }

    /// Not supported (yet):
    /// requires(FormatterType::IsFormattingRequired and not(HasSpanningTuple))
    ///     - non-native formats without spanning tuples, since it is hard to guarantee that (mostly) text based data does not span buffers
    /// requires(not(FormatterType::IsFormattingRequired) and HasSpanningTuple)
    ///     - native format with spanning tuples, since it is hard to guarantee that we always read in full buffers and if we don't we need
    ///       a mechanism to determine the offsets of the fields in the individual buffers asynchronously
    void scanTask(
        ExecutionContext& executionCtx,
        Nautilus::RecordBuffer& recordBuffer,
        const PhysicalOperator& child,
        const std::vector<Record::RecordFieldIdentifier>& projections,
        const size_t /*configuredBufferSize*/,
        const bool isFirstOperatorAfterSource,
        const std::vector<Nautilus::Record::RecordFieldIdentifier>& requiredFields)
    requires(not(FormatterType::IsFormattingRequired) and not(FormatterType::HasSpanningTuple))
    {
        (void) requiredFields;
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

        auto fieldIndexFunction = typename FormatterType::FieldIndexFunctionType();
        for (nautilus::val<uint64_t> i = 0_u64; i < numberOfRecords; i = i + 1_u64)
        {
            auto record = fieldIndexFunction.readNextRecord(projections, recordBuffer, i, indexerMetaData);
            child.execute(executionCtx, record);
        }
    }

    void scanTask(
        ExecutionContext& executionCtx,
        Nautilus::RecordBuffer& recordBuffer,
        const PhysicalOperator& child,
        const std::vector<Record::RecordFieldIdentifier>& projections,
        const size_t configuredBufferSize,
        const bool,
        const std::vector<Record::RecordFieldIdentifier>& requiredFields)
    requires(FormatterType::IsFormattingRequired and FormatterType::HasSpanningTuple)
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

        /// index raw tuple buffer, resolve and index spanning tuples(SequenceShredder) and return pointers to resolved spanning tuples, if exist;
        auto spanningTuplePOD = nautilus::invoke(
            indexTuplesProxy,
            recordBuffer.getReference(),
            executionCtx.pipelineContext,
            nautilus::val<InputFormatterTask*>(this),
            nautilus::val<size_t>(configuredBufferSize),
            executionCtx.pipelineMemoryProvider.arena.arenaRef);

        /// parse leading spanning tuple if exists
        if (const nautilus::val<bool> hasLeadingPtr
            = *getMemberWithOffset<bool>(spanningTuplePOD, offsetof(SpanningTuplePOD, hasLeadingSpanningTupleBool));
            hasLeadingPtr)
        {
            /// Get leading field index function and a pointer to the spanning tuple 'record'
            auto leadingFIF
                = getMemberWithOffset<typename FormatterType::FieldIndexFunctionType>(spanningTuplePOD, offsetof(SpanningTuplePOD, leadingSpanningTupleFIF));
            auto spanningRecordPtr = getMemberPtrPLACEHOLDER<int8_t>(spanningTuplePOD, offsetof(SpanningTuplePOD, leadingSpanningTuplePtr));

            /// 'leadingFIF.value' is essentially the static function FormatterType::readsSpanningRecord
            auto recordIndex = nautilus::val<uint64_t>(0);
            auto record = leadingFIF.value->readSpanningRecord(projections, spanningRecordPtr, recordIndex, indexerMetaData, leadingFIF, requiredFields);
            child.execute(executionCtx, record);
        }

        /// parse raw tuple buffer (if there are complete tuples in it)
        const nautilus::val<uint64_t> totalNumberOfTuples
            = *getMemberWithOffset<uint64_t>(spanningTuplePOD, offsetof(SpanningTuplePOD, totalNumberOfTuples));
        auto rawFieldAccessFunction
            = getMemberWithOffset<typename FormatterType::FieldIndexFunctionType>(spanningTuplePOD, offsetof(SpanningTuplePOD, rawBufferFIF));

        for (nautilus::val<uint64_t> i = static_cast<uint64_t>(0); i < totalNumberOfTuples; i = i + static_cast<uint64_t>(1))
        {
            auto record = rawFieldAccessFunction.value->readSpanningRecord(
                projections, recordBuffer.getBuffer(), i, indexerMetaData, rawFieldAccessFunction, requiredFields);
            child.execute(executionCtx, record);
        }

        /// parse trailing spanning tuple if exists
        if (const nautilus::val<bool> hasTrailingPtr
            = *getMemberWithOffset<bool>(spanningTuplePOD, offsetof(SpanningTuplePOD, hasTrailingSpanningTupleBool));
            hasTrailingPtr)
        {
            auto trailingFIF
                = getMemberWithOffset<typename FormatterType::FieldIndexFunctionType>(spanningTuplePOD, offsetof(SpanningTuplePOD, trailingSpanningTupleFIF));
            auto recordPtr = getMemberPtrPLACEHOLDER<int8_t>(spanningTuplePOD, offsetof(SpanningTuplePOD, trailingSpanningTuplePtr));

            auto recordIndex = nautilus::val<uint64_t>(0);
            auto record
                = trailingFIF.value->readSpanningRecord(projections, recordPtr, recordIndex, indexerMetaData, trailingFIF, requiredFields);
            child.execute(executionCtx, record);
        }
    }

    std::ostream& taskToString(std::ostream& os) const
    {
        /// Not using fmt::format, because it fails during build, trying to pass sequenceShredder as a const value
        os << "InputFormatterTask(originId: " << originId << ", inputFormatIndexer: " << inputFormatIndexer
           << ", sequenceShredder: " << *sequenceShredder << ")\n";
        return os;
    }

private:
    OriginId originId;
    FormatterType inputFormatIndexer;
    SchemaInfo schemaInfo;
    typename FormatterType::IndexerMetaData indexerMetaData;
    std::unique_ptr<SequenceShredder> sequenceShredder; /// unique_ptr, because mutex is not copiable
};

}
