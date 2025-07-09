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
    const TupleMetaData& tupleMetaData,
    const std::vector<RawValueParser::ParseFunctionSignature>& parseFunctions,
    Memory::AbstractBufferProvider& bufferProvider /// for getting unpooled buffers for varsized data
)
{
    const size_t currentTupleIdx = formattedBuffer.getNumberOfTuples();
    const size_t offsetOfCurrentTupleInBytes = currentTupleIdx * tupleMetaData.sizeOfTupleInBytes;
    size_t offsetOfCurrentFieldInBytes = 0;

    /// Currently, we still allow only row-wise writing to the formatted buffer
    /// This will will change with #496, which implements the InputFormatterTask in Nautilus
    /// The InputFormatterTask then becomes part of a pipeline with a scan/emit phase and has access to the MemoryProvider
    for (size_t fieldIndex = 0; fieldIndex < tupleMetaData.fieldSizesInBytes.size(); ++fieldIndex)
    {
        /// Get the current field, parse it, and write it to the correct position in the formatted buffer
        const auto currentFieldSV = fieldIndexFunction.readFieldAt(tupleView, numTuplesReadFromRawBuffer, fieldIndex);
        const auto writeOffsetInBytes = offsetOfCurrentTupleInBytes + offsetOfCurrentFieldInBytes;
        parseFunctions[fieldIndex](currentFieldSV, writeOffsetInBytes, bufferProvider, formattedBuffer);

        /// Add the size of the current field to the running offset to get the offset of the next field
        const auto sizeOfCurrentFieldInBytes = tupleMetaData.fieldSizesInBytes[fieldIndex];
        offsetOfCurrentFieldInBytes += sizeOfCurrentFieldInBytes;
    }
}

/// Constructs a spanning tuple (string) that spans over at least two buffers (buffersToFormat).
/// First, determines the start of the spanning tuple in the first buffer to format. Constructs a spanning tuple from the required bytes.
/// Second, appends all bytes of all raw buffers that are not the last buffer to the spanning tuple.
/// Third, determines the end of the spanning tuple in the last buffer to format. Appends the required bytes to the spanning tuple.
/// Lastly, formats the full spanning tuple.
template <typename FieldIndexFunctionType, bool IsFormattingRequired>
void processSpanningTuple(
    const std::span<const StagedBuffer> stagedBuffersSpan,
    Memory::AbstractBufferProvider& bufferProvider,
    Memory::TupleBuffer& formattedBuffer,
    const TupleMetaData& tupleMetaData,
    const InputFormatIndexer<FieldIndexFunctionType, IsFormattingRequired>& inputFormatIndexer,
    const std::vector<RawValueParser::ParseFunctionSignature>& parseFunctions)
{
    INVARIANT(stagedBuffersSpan.size() >= 2, "A spanning tuple must span across at least two buffers");
    /// If the buffers are not empty, there are at least three buffers
    std::stringstream spanningTupleStringStream;
    spanningTupleStringStream << tupleMetaData.tupleDelimiter;

    auto firstBuffer = stagedBuffersSpan.front();
    const auto firstSpanningTuple
        = (firstBuffer.isValidRawBuffer()) ? firstBuffer.getTrailingBytes(tupleMetaData.tupleDelimiter.size()) : "";
    spanningTupleStringStream << firstSpanningTuple;

    /// Process all buffers in-between the first and the last
    for (const auto middleBuffers = stagedBuffersSpan | std::views::drop(1) | std::views::take(stagedBuffersSpan.size() - 2);
         const auto& buffer : middleBuffers)
    {
        spanningTupleStringStream << buffer.getBufferView();
    }

    auto lastBuffer = stagedBuffersSpan.back();
    spanningTupleStringStream << lastBuffer.getLeadingBytes();
    spanningTupleStringStream << tupleMetaData.tupleDelimiter;

    const std::string completeSpanningTuple(spanningTupleStringStream.str());
    const auto sizeOfLeadingAndTrailingTupleDelimiter = 2 * tupleMetaData.tupleDelimiter.size();
    if (completeSpanningTuple.size() > sizeOfLeadingAndTrailingTupleDelimiter)
    {
        auto fieldIndexFunction = FieldIndexFunctionType(bufferProvider);
        lastBuffer.setSpanningTuple(completeSpanningTuple);
        inputFormatIndexer.indexRawBuffer(fieldIndexFunction, lastBuffer.getRawTupleBuffer(), tupleMetaData);
        processTuple<FieldIndexFunctionType>(
            completeSpanningTuple, fieldIndexFunction, 0, formattedBuffer, tupleMetaData, parseFunctions, bufferProvider);
        formattedBuffer.setNumberOfTuples(formattedBuffer.getNumberOfTuples() + 1);
    }
}

/// InputFormatterTasks concurrently take (potentially) raw input buffers and format all full tuples in these raw input buffers that the
/// individual InputFormatterTasks see during execution.
/// The only point of synchronization is a call to the SequenceShredder data structure, which determines which buffers the InputFormatterTask
/// needs to process (resolving tuples that span multiple raw buffers).
/// An InputFormatterTask belongs to exactly one source. The source reads raw data into buffers, constructs a Task from the
/// raw buffer and its successor (the InputFormatterTask) and writes it to the task queue of the QueryEngine.
/// The QueryEngine concurrently executes InputFormatterTasks. Thus, even if the source writes the InputFormatterTasks to the task queue sequentially,
/// the QueryEngine may still execute them in any order.
template <typename FormatterType, typename FieldIndexFunctionType, bool HasSpanningTuple>
requires(HasSpanningTuple or not FormatterType::IsFormattingRequired)
class InputFormatterTask
{
public:
    static constexpr bool hasSpanningTuple() { return HasSpanningTuple; }
    explicit InputFormatterTask(
        const OriginId originId,
        std::unique_ptr<InputFormatIndexer<FieldIndexFunctionType, FormatterType::IsFormattingRequired>> inputFormatIndexer,
        const Schema& schema,
        const ParserConfig& parserConfig)
        : originId(originId), inputFormatIndexer(std::move(inputFormatIndexer))
    {
        /// Only if we need to resolve spanning tuples, we need the SequenceShredder
        this->sequenceShredder = (HasSpanningTuple) ? std::make_unique<SequenceShredder>(parserConfig.tupleDelimiter.size()) : nullptr;
        this->tupleMetaData.sizeOfTupleInBytes = schema.getSizeOfSchemaInBytes();
        this->tupleMetaData.fieldSizesInBytes.reserve(schema.getNumberOfFields());
        this->tupleMetaData.tupleDelimiter = parserConfig.tupleDelimiter;
        this->tupleMetaData.fieldDelimiter = parserConfig.fieldDelimiter;
        this->parseFunctions.reserve(this->tupleMetaData.fieldSizesInBytes.size());

        /// Since we know the schema, we can create a vector that contains a function that converts the string representation of a field value
        /// to our internal representation in the correct order. During parsing, we iterate over the fields in each tuple, and, using the current
        /// field number, load the correct function for parsing from the vector.
        size_t priorFieldOffset = 0;
        for (const auto& field : schema.getFields())
        {
            /// Store the size of the field in bytes (for offset calculations).
            /// Store the parsing function in a vector.
            if (field.dataType.isType(DataType::Type::VARSIZED))
            {
                this->parseFunctions.emplace_back(RawValueParser::getBasicStringParseFunction());
            }
            else
            {
                this->parseFunctions.emplace_back(RawValueParser::getBasicTypeParseFunction(field.dataType.type));
            }
            this->tupleMetaData.fieldSizesInBytes.emplace_back(field.dataType.getSizeInBytes());
            this->tupleMetaData.fieldOffsetsInBytes.emplace_back() = priorFieldOffset + field.dataType.getSizeInBytes();
            priorFieldOffset = this->tupleMetaData.fieldOffsetsInBytes.back();
        }
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
        if (HasSpanningTuple)
        {
            INVARIANT(sequenceShredder != nullptr, "The SequenceShredder handles spanning tuples, thus it must not be null.");
            sequenceShredder->validateState();
        }
    }

    /// Not supported (yet):
    /// requires(FormatterType::IsFormattingRequired and not(HasSpanningTuple))
    ///     - non-native formats without spanning tuples, since it is hard to guarantee that (mostly) text based data does not span buffers
    /// requires(not(FormatterType::IsFormattingRequired) and HasSpanningTuple)
    ///     - native format with spanning tuples, since it is hard to guarantee that we always read in full buffers and if we don't we need
    ///       a mechanism to determine the offsets of the fields in the individual buffers asynchronously
    void executeTask(const RawTupleBuffer& rawBuffer, PipelineExecutionContext& pec)
    requires(not(FormatterType::IsFormattingRequired) and not(HasSpanningTuple))
    {
        /// If the format has fixed size tuple and no spanning tuples (give the assumption that tuples are aligned with the start of the buffers)
        /// the InputFormatterTask does not need to do anything.
        /// @Note: with a Nautilus implementation, we can skip the proxy function call that triggers formatting/indexing during tracing,
        /// leading to generated code that immediately operates on the data.
        const auto [div, mod] = std::lldiv(static_cast<long long>(rawBuffer.getNumberOfTuples()), this->tupleMetaData.sizeOfTupleInBytes);
        PRECONDITION(
            mod == 0,
            "Raw buffer contained {} bytes, which is not a multiple of the tuple size {} bytes.",
            rawBuffer.getNumberOfBytes(),
            this->tupleMetaData.sizeOfTupleInBytes);
        /// @Note: We assume that '.getNumberOfBytes()' ALWAYS returns the number of bytes at this point (set by source)
        const auto numberOfTuplesInFormattedBuffer = rawBuffer.getNumberOfBytes() / this->tupleMetaData.sizeOfTupleInBytes;
        rawBuffer.setNumberOfTuples(numberOfTuplesInFormattedBuffer);
        /// The 'rawBuffer' is already formatted, so we can use it without any formatting.
        rawBuffer.emit(pec, PipelineExecutionContext::ContinuationPolicy::POSSIBLE);
    }

    void executeTask(const RawTupleBuffer& rawBuffer, PipelineExecutionContext& pec)
    requires(FormatterType::IsFormattingRequired and HasSpanningTuple)
    {
        /// Check if the current sequence number is in the range of the ring buffer of the sequence shredder.
        /// If not (should very rarely be the case), we put the task back.
        /// After enough out-of-range requests, the SequenceShredder increases the size of its ring buffer.
        if (not sequenceShredder->isInRange(rawBuffer.getSequenceNumber().getRawValue()))
        {
            rawBuffer.emit(pec, PipelineExecutionContext::ContinuationPolicy::REPEAT);
            return;
        }

        /// Get field delimiter indices of the raw buffer by using the InputFormatIndexer implementation
        auto fieldIndexFunction = FieldIndexFunctionType(*pec.getBufferManager());
        inputFormatIndexer->indexRawBuffer(fieldIndexFunction, rawBuffer, tupleMetaData);

        /// If the offset of the _first_ tuple delimiter is not within the rawBuffer, the InputFormatIndexer did not find any tuple delimiter
        ChunkNumber::Underlying runningChunkNumber = ChunkNumber::INITIAL;
        if (fieldIndexFunction.getOffsetOfFirstTupleDelimiter() < rawBuffer.getBufferSize())
        {
            /// If the buffer delimits at least two tuples, it may produce two (leading/trailing) spanning tuples and may contain full tuples
            /// in its raw input buffer.
            processRawBufferWithTupleDelimiter(rawBuffer, runningChunkNumber, fieldIndexFunction, pec);
        }
        else
        {
            /// If the buffer does not delimit a single tuple, it may still connect two buffers that delimit tuples and therefore comple a
            /// spanning tuple.
            processRawBufferWithoutTupleDelimiter(rawBuffer, runningChunkNumber, fieldIndexFunction, pec);
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
    std::unique_ptr<InputFormatIndexer<FieldIndexFunctionType, FormatterType::IsFormattingRequired>>
        inputFormatIndexer; /// unique_ptr, because InputFormatIndexer is abstract class
    std::unique_ptr<SequenceShredder> sequenceShredder; /// unique_ptr, because mutex is not copiable
    std::vector<RawValueParser::ParseFunctionSignature> parseFunctions;
    TupleMetaData tupleMetaData;

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
        const size_t numberOfTuplesPerBuffer = bufferProvider->getBufferSize() / this->tupleMetaData.sizeOfTupleInBytes;
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
                    this->tupleMetaData,
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
                this->tupleMetaData,
                *this->inputFormatIndexer,
                this->parseFunctions);
        }

        /// 2. process tuples in buffer
        if (fieldIndexFunction.getTotalNumberOfTuples() > 0)
        {
            parseRawBuffer(rawBuffer, runningChunkNumber, fieldIndexFunction, formattedBuffer, pec);
        }

        /// 3. process trailing spanning tuple if required
        if (/* hasTrailingSpanningTuple */ indexOfSequenceNumberInStagedBuffers < (stagedBuffers.size() - 1))
        {
            const auto numBytesInFormattedBuffer = formattedBuffer.getNumberOfTuples() * this->tupleMetaData.sizeOfTupleInBytes;
            if (formattedBuffer.getBufferSize() - numBytesInFormattedBuffer < this->tupleMetaData.sizeOfTupleInBytes)
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
                this->tupleMetaData,
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
            stagedBuffers, *bufferProvider, formattedBuffer, this->tupleMetaData, *this->inputFormatIndexer, this->parseFunctions);

        formattedBuffer.setSequenceNumber(rawBuffer.getSequenceNumber());
        formattedBuffer.setChunkNumber(NES::ChunkNumber(runningChunkNumber++));
        formattedBuffer.setOriginId(rawBuffer.getOriginId());
        pec.emitBuffer(formattedBuffer, NES::PipelineExecutionContext::ContinuationPolicy::POSSIBLE);
    }
};

}
