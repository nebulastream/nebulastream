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

#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <InputFormatters/InputFormatter.hpp>
#include <InputFormatters/InputFormatterTaskPipeline.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Common.hpp>
#include <ErrorHandling.hpp>
#include <FieldAccessFunction.hpp>
#include <PipelineExecutionContext.hpp>
#include <RawInputDataParser.hpp>
#include <SequenceShredder.hpp>
#include <Common/DataTypes/VariableSizedDataType.hpp>
#include <Common/PhysicalTypes/BasicPhysicalType.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>


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
    const auto numberOfTuplesThatDoNotFitIntoFirstFormattedBuffer = totalNumberOfTuplesInRawBuffer - capacityOfFirstBuffer;
    /// We need at least one (first) formatted buffer. We need additional buffers if we have leftover raw tuples.
    /// Overflow-safe calculation of ceil taken from (https://stackoverflow.com/questions/2745074/fast-ceiling-of-an-integer-division-in-c-c)
    /// 1 + ceil(numberOfTuplesThatDoNotFitIntoFirstFormattedBuffer / numberOfTuplesPerBuffer)
    const auto numberOfBuffersToFill = 1
        + ((numberOfTuplesThatDoNotFitIntoFirstFormattedBuffer % numberOfTuplesPerBuffer != 0u)
               ? (numberOfTuplesThatDoNotFitIntoFirstFormattedBuffer / numberOfTuplesPerBuffer) + 1
               : numberOfTuplesThatDoNotFitIntoFirstFormattedBuffer / numberOfTuplesPerBuffer);
    return numberOfBuffersToFill;
}

/// Takes a view over the raw bytes of a tuple, and a fieldAccessFunction that knows the field offsets in the raw bytes of the tuple.
/// Iterates over all fields of the tuple and parses each field using the corresponding 'parse function'.
template <typename FieldAccessFunctionType>
void processTuple(
    const std::string_view tupleView,
    const FieldAccessFunction<FieldAccessFunctionType>& fieldAccessFunction,
    const size_t numTuplesReadFromRawBuffer,
    Memory::TupleBuffer& formattedBuffer,
    const TupleMetaData& tupleMetaData,
    const std::vector<RawInputDataParser::ParseFunctionSignature>& parseFunctions,
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
        const auto currentFieldSV = fieldAccessFunction.readFieldAt(tupleView, numTuplesReadFromRawBuffer, fieldIndex);
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
template <typename FieldAccessFunctionType, bool UsesNativeFormat>
void processSpanningTuple(
    const std::span<const StagedBuffer> stagedBuffersSpan,
    Memory::AbstractBufferProvider& bufferProvider,
    Memory::TupleBuffer& formattedBuffer,
    const TupleMetaData& tupleMetaData,
    const InputFormatter<FieldAccessFunctionType, UsesNativeFormat>& inputFormatter,
    const std::vector<RawInputDataParser::ParseFunctionSignature>& parseFunctions)
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
        auto fieldAccessFunction = FieldAccessFunctionType(bufferProvider);
        lastBuffer.setSpanningTuple(completeSpanningTuple);
        inputFormatter.setupFieldAccessFunctionForBuffer(fieldAccessFunction, lastBuffer.getRawTupleBuffer(), tupleMetaData);
        processTuple<FieldAccessFunctionType>(
            completeSpanningTuple, fieldAccessFunction, 0, formattedBuffer, tupleMetaData, parseFunctions, bufferProvider);
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
template <typename FormatterType, typename FieldAccessFunctionType, bool HasSpanningTuple>
class InputFormatterTask
{
public:
    static constexpr bool hasSpanningTuple() { return HasSpanningTuple; }
    explicit InputFormatterTask(
        const OriginId originId,
        std::unique_ptr<InputFormatter<FieldAccessFunctionType, FormatterType::UsesNativeFormat>> inputFormatter,
        const Schema& schema,
        const Sources::ParserConfig& parserConfig)
        : originId(originId), inputFormatter(std::move(inputFormatter))
    {
        const auto schemaContainsVarSized = std::ranges::any_of(
            schema.getFieldNames(),
            [&schema](const auto& fieldName)
            { return Util::instanceOf<VariableSizedDataType>(schema.getFieldByName(fieldName).value()->getDataType()); });
        PRECONDITION(
            not(FormatterType::UsesNativeFormat and schemaContainsVarSized and HasSpanningTuple),
            "Not supporting variable sized data for the internal format with spanning tuples.");
        /// Only if we need to resolve spanning tuples, we need the SequenceShredder
        this->sequenceShredder = (HasSpanningTuple) ? std::make_unique<SequenceShredder>(parserConfig.tupleDelimiter.size()) : nullptr;
        this->tupleMetaData.sizeOfTupleInBytes = schema.getSchemaSizeInBytes();
        this->tupleMetaData.fieldSizesInBytes.reserve(schema.getFieldCount());
        this->tupleMetaData.tupleDelimiter = parserConfig.tupleDelimiter;
        this->tupleMetaData.fieldDelimiter = parserConfig.fieldDelimiter;
        this->parseFunctions.reserve(this->tupleMetaData.fieldSizesInBytes.size());

        /// Since we know the schema, we can create a vector that contains a function that converts the string representation of a field value
        /// to our internal representation in the correct order. During parsing, we iterate over the fields in each tuple, and, using the current
        /// field number, load the correct function for parsing from the vector.
        size_t priorFieldOffset = 0;
        for (const std::shared_ptr<AttributeField>& field : schema)
        {
            const auto defaultPhysicalTypeFactory = DefaultPhysicalTypeFactory();
            const auto physicalType = defaultPhysicalTypeFactory.getPhysicalType(field->getDataType());
            /// Store the size of the field in bytes (for offset calculations).
            /// Store the parsing function in a vector.
            if (const auto basicPhysicalType = std::dynamic_pointer_cast<BasicPhysicalType>(physicalType))
            {
                this->parseFunctions.emplace_back(RawInputDataParser::getBasicTypeParseFunction(*basicPhysicalType));
            }
            else
            {
                this->parseFunctions.emplace_back(RawInputDataParser::getBasicStringParseFunction());
            }
            this->tupleMetaData.fieldSizesInBytes.emplace_back(physicalType->size());
            this->tupleMetaData.fieldOffsetsInBytes.emplace_back() = priorFieldOffset + physicalType->size();
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

    void executeTask(const Memory::TupleBuffer&, Runtime::Execution::PipelineExecutionContext&)
    requires(not(FormatterType::UsesNativeFormat) and not(HasSpanningTuple))
    {
        throw NotImplemented("The InputFormatterTask does not explicitly support non-internal formats without spanning tuples yet.");
    }

    void executeTask(const RawTupleBuffer& rawBuffer, Runtime::Execution::PipelineExecutionContext& pec)
    requires(FormatterType::UsesNativeFormat and not(HasSpanningTuple))
    {
        /// If the format has fixed size tuple and no spanning tuples (give the assumption that tuples are aligned with the start of the buffers)
        /// the InputFormatterTask does not need to do anything.
        /// @Note: with a Nautilus implementation, we can skip the proxy function call that triggers formatting/indexing during tracing,
        /// leading to generated code that immediately operates on the data.
        const auto [div, mod] = std::lldiv(static_cast<long long>(rawBuffer.getNumberOfBytes()), this->tupleMetaData.sizeOfTupleInBytes);
        PRECONDITION(
            mod == 0,
            "Raw buffer contained {} bytes, which is not a multiple of the tuple size {} bytes.",
            rawBuffer.getNumberOfBytes(),
            this->tupleMetaData.sizeOfTupleInBytes);
        /// @Note: We assume that '.getNumberOfBytes()' ALWAYS returns the number of bytes at this point (set by source)
        const auto numberOfTuplesInFormattedBuffer = rawBuffer.getNumberOfBytes() / this->tupleMetaData.sizeOfTupleInBytes;
        rawBuffer.setNumberOfTuples(numberOfTuplesInFormattedBuffer);
        /// The 'rawBuffer' is already formatted, so we can use it without any formatting.
        rawBuffer.emit(pec, Runtime::Execution::PipelineExecutionContext::ContinuationPolicy::POSSIBLE);
    }

    /// @Note: throws CannotAccessBuffer exception if the copy is out of bounds of the formatted buffer
    static void
    copyWithBoundsCheck(const std::string_view rawBufferBytes, Memory::TupleBuffer formattedBuffer, const size_t formattedBufferOffset)
    {
        if (formattedBufferOffset + rawBufferBytes.size() >= formattedBuffer.getBufferSize())
        {
            throw CannotAccessBuffer(
                "Tried to copy {} to a formatted buffer of size {} bytes, starting at offset {}",
                rawBufferBytes.size(),
                formattedBuffer.getBufferSize(),
                rawBufferBytes.size());
        }
        std::memcpy(
            formattedBuffer.getBuffer() + formattedBufferOffset, ///NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
            rawBufferBytes.data(),
            rawBufferBytes.size());
    }

    void executeTask(const RawTupleBuffer& rawBuffer, Runtime::Execution::PipelineExecutionContext& pec)
    requires(FormatterType::UsesNativeFormat and HasSpanningTuple)
    {
        /// Check if the current sequence number is in the range of the ring buffer of the sequence shredder.
        /// If not (should very rarely be the case), we put the task back.
        /// After enough out-of-range requests, the SequenceShredder increases the size of its ring buffer.
        if (not sequenceShredder->isInRange(rawBuffer.getSequenceNumber().getRawValue()))
        {
            rawBuffer.emit(pec, Runtime::Execution::PipelineExecutionContext::ContinuationPolicy::REPEAT);
            return;
        }

        auto bufferProvider = pec.getBufferManager();
        ChunkNumber::Underlying runningChunkNumber = ChunkNumber::INITIAL;

        /// Setting up the field access function (may use buffer information like the sequence number to determine where the first
        /// tuple and individual fields start.
        auto fieldAccessFunction = FieldAccessFunctionType(*bufferProvider);
        inputFormatter->setupFieldAccessFunctionForBuffer(fieldAccessFunction, rawBuffer, tupleMetaData);

        const auto offsetOfFirstTupleDelimiter = fieldAccessFunction.getOffsetOfFirstTupleDelimiter();
        const auto offsetOfLastTupleDelimiter = fieldAccessFunction.getOffsetOfLastTupleDelimiter();

        /// Assuming that there is at least one tuple delimiter in the buffer, because we assume that a tuple cannot be larger than a buffer
        INVARIANT(
            offsetOfFirstTupleDelimiter < rawBuffer.getBufferSize(), "A buffer with fixed-size tuples must delimit at least one tuple.");
        const auto [indexOfSequenceNumberInStagedBuffers, stagedBuffers] = this->sequenceShredder->template processSequenceNumber<true>(
            StagedBuffer{rawBuffer, rawBuffer.getNumberOfBytes(), offsetOfFirstTupleDelimiter, offsetOfLastTupleDelimiter},
            rawBuffer.getSequenceNumber().getRawValue());

        /// Handle leading spanning tuple
        size_t formattedBufferOffset = 0;
        auto formattedBuffer = bufferProvider->getBufferBlocking();
        /// A raw buffer can have a leading spanning tuple, as long as its first tuple does not start exactly at the first byte of the raw buffer
        const auto firstBuffer = stagedBuffers.front();
        const bool canHaveLeadingSpanningTuple = offsetOfFirstTupleDelimiter != 0;
        if (/* hasLeadingSpanningTuple */ indexOfSequenceNumberInStagedBuffers != 0 and firstBuffer.isValidRawBuffer()
            and canHaveLeadingSpanningTuple)
        {
            /// Copy leading spanning tuple
            const auto trailingSpanningTupleView = firstBuffer.getTrailingBytes(0);
            copyWithBoundsCheck(trailingSpanningTupleView, formattedBuffer, formattedBufferOffset);
            formattedBufferOffset += trailingSpanningTupleView.size();
            copyWithBoundsCheck(rawBuffer.getBufferView().substr(0, offsetOfFirstTupleDelimiter), formattedBuffer, formattedBufferOffset);
            formattedBufferOffset += offsetOfFirstTupleDelimiter;
            formattedBuffer.setNumberOfTuples(1);
        }

        /// Fill formatted tuple buffers with data from raw buffers until full
        const auto maxNumberOfTuplesInRawBuffer
            = (rawBuffer.getNumberOfTuples() - offsetOfFirstTupleDelimiter) / this->tupleMetaData.sizeOfTupleInBytes;
        auto numberOfTuplesInRawBuffer = std::min(maxNumberOfTuplesInRawBuffer, fieldAccessFunction.getTotalNumberOfTuples());
        auto bytesReadFromRawBuffer = offsetOfFirstTupleDelimiter;
        while (numberOfTuplesInRawBuffer > 0)
        {
            /// Determine the maximum number of tuples we can copy to the current formatted buffer
            size_t bytesLeftInFormattedBuffer
                = formattedBuffer.getBufferSize() - (formattedBuffer.getNumberOfTuples() * this->tupleMetaData.sizeOfTupleInBytes);
            if (bytesLeftInFormattedBuffer < this->tupleMetaData.sizeOfTupleInBytes)
            {
                setMetadataOfFormattedBuffer(rawBuffer.getRawBuffer(), formattedBuffer, runningChunkNumber, false);
                pec.emitBuffer(formattedBuffer, NES::Runtime::Execution::PipelineExecutionContext::ContinuationPolicy::POSSIBLE);
                formattedBuffer = bufferProvider->getBufferBlocking();
                formattedBufferOffset = 0;
                bytesLeftInFormattedBuffer = formattedBuffer.getBufferSize();
            }
            /// Determine how many tuples to copy from the raw buffer into the formatted buffer
            const auto tupleCapacityOfFormattedBuffer = bytesLeftInFormattedBuffer / this->tupleMetaData.sizeOfTupleInBytes;
            const auto numberOfTuplesToCopy = std::min(tupleCapacityOfFormattedBuffer, numberOfTuplesInRawBuffer);
            const auto numBytesToCopy = numberOfTuplesToCopy * this->tupleMetaData.sizeOfTupleInBytes;
            /// Copy tuples from raw buffer into formatted buffer, udpate offsets and number of tuples in formatted buffer
            copyWithBoundsCheck(
                rawBuffer.getBufferView().substr(bytesReadFromRawBuffer, numBytesToCopy), formattedBuffer, formattedBufferOffset);
            bytesReadFromRawBuffer += numBytesToCopy;
            formattedBufferOffset += numBytesToCopy;
            formattedBuffer.setNumberOfTuples(formattedBuffer.getNumberOfTuples() + numberOfTuplesToCopy);
            /// Determine whether there are still tuples left in the raw buffer (requires another iteration)
            numberOfTuplesInRawBuffer -= numberOfTuplesToCopy;
        }

        /// Handle trailing spanning tuple
        /// A raw buffer can have a trailing spanning tuple, as long as its last tuple does not end exactly in the last byte of the raw buffer
        const bool canHaveTrailingTuple = offsetOfLastTupleDelimiter != rawBuffer.getNumberOfTuples();
        if (/* hasTrailingSpanningTuple */ indexOfSequenceNumberInStagedBuffers < (stagedBuffers.size() - 1) and canHaveTrailingTuple)
        {
            if ((formattedBuffer.getNumberOfTuples() + 1) * this->tupleMetaData.sizeOfTupleInBytes > formattedBuffer.getBufferSize())
            {
                setMetadataOfFormattedBuffer(rawBuffer.getRawBuffer(), formattedBuffer, runningChunkNumber, false);
                pec.emitBuffer(formattedBuffer, NES::Runtime::Execution::PipelineExecutionContext::ContinuationPolicy::POSSIBLE);
                formattedBuffer = bufferProvider->getBufferBlocking();
                formattedBufferOffset = 0;
            }
            const auto sizeOfTrailingSpanningTuple = rawBuffer.getBufferSize() - offsetOfLastTupleDelimiter;
            copyWithBoundsCheck(
                rawBuffer.getBufferView().substr(offsetOfLastTupleDelimiter, sizeOfTrailingSpanningTuple),
                formattedBuffer,
                formattedBufferOffset);
            formattedBufferOffset += sizeOfTrailingSpanningTuple;
            copyWithBoundsCheck(stagedBuffers.back().getLeadingBytes(), formattedBuffer, formattedBufferOffset);
            formattedBuffer.setNumberOfTuples(formattedBuffer.getNumberOfTuples() + 1);
        }
        /// A buffer may be too small to have any tuple and may not trigger any spanning tuple, in which case we never write tuples to the buffer
        if (formattedBuffer.getNumberOfTuples() > 0)
        {
            /// This is the last formatted buffer we produce, so set the 'isLastChunk' flag to true
            setMetadataOfFormattedBuffer(rawBuffer.getRawBuffer(), formattedBuffer, runningChunkNumber, true);
            pec.emitBuffer(formattedBuffer, NES::Runtime::Execution::PipelineExecutionContext::ContinuationPolicy::POSSIBLE);
        }
    }

    void executeTask(const RawTupleBuffer& rawBuffer, Runtime::Execution::PipelineExecutionContext& pec)
    requires(not(FormatterType::UsesNativeFormat) and HasSpanningTuple)
    {
        /// Check if the current sequence number is in the range of the ring buffer of the sequence shredder.
        /// If not (should very rarely be the case), we put the task back.
        /// After enough out-of-range requests, the SequenceShredder increases the size of its ring buffer.
        if (not sequenceShredder->isInRange(rawBuffer.getSequenceNumber().getRawValue()))
        {
            rawBuffer.emit(pec, Runtime::Execution::PipelineExecutionContext::ContinuationPolicy::REPEAT);
            return;
        }

        /// Get indexes field delimiters in the raw buffer using the InputFormatter implementation
        auto fieldAccessFunction = FieldAccessFunctionType(*pec.getBufferManager());
        inputFormatter->setupFieldAccessFunctionForBuffer(fieldAccessFunction, rawBuffer, tupleMetaData);

        /// If the offset of the _first_ tuple delimiter is not within the rawBuffer, the InputFormatter did not find any tuple delimiter
        ChunkNumber::Underlying runningChunkNumber = ChunkNumber::INITIAL;
        if (fieldAccessFunction.getOffsetOfFirstTupleDelimiter() < rawBuffer.getBufferSize())
        {
            /// If the buffer delimits at least two tuples, it may produce two (leading/trailing) spanning tuples and may contain full tuples
            /// in its raw input buffer.
            processRawBufferWithTupleDelimiter(rawBuffer, runningChunkNumber, fieldAccessFunction, pec);
        }
        else
        {
            /// If the buffer does not delimit a single tuple, it may still connect two buffers that delimit tuples and therefore comple a
            /// spanning tuple.
            processRawBufferWithoutTupleDelimiter(rawBuffer, runningChunkNumber, fieldAccessFunction, pec);
        }
    }

    std::ostream& taskToString(std::ostream& os) const
    {
        /// Not using fmt::format, because it fails during build, trying to pass sequenceShredder as a const value
        os << "InputFormatterTask(originId: " << originId << ", inputFormatter: " << *inputFormatter
           << ", sequenceShredder: " << *sequenceShredder << ")\n";
        return os;
    }

private:
    OriginId originId;
    std::unique_ptr<InputFormatter<FieldAccessFunctionType, FormatterType::UsesNativeFormat>>
        inputFormatter; /// unique_ptr, because InputFormatter is abstract class
    std::unique_ptr<SequenceShredder> sequenceShredder; /// unique_ptr, because mutex is not copiable
    std::vector<RawInputDataParser::ParseFunctionSignature> parseFunctions;
    TupleMetaData tupleMetaData;

    /// Called by processRawBufferWithTupleDelimiter if the raw buffer contains at least one full tuple.
    /// Iterates over all full tuples, using the indexes in FieldOffsets and parses the tuples into formatted data.
    void parseRawBuffer(
        const RawTupleBuffer& rawBuffer,
        ChunkNumber::Underlying& runningChunkNumber,
        const FieldAccessFunction<FieldAccessFunctionType>& fieldAccessFunction,
        Memory::TupleBuffer& formattedBuffer,
        Runtime::Execution::PipelineExecutionContext& pec) const
    {
        const auto bufferProvider = pec.getBufferManager();
        const auto numberOfTuplesInFirstFormattedBuffer = formattedBuffer.getNumberOfTuples();
        const size_t numberOfTuplesPerBuffer = bufferProvider->getBufferSize() / this->tupleMetaData.sizeOfTupleInBytes;
        PRECONDITION(numberOfTuplesPerBuffer != 0, "The capacity of a buffer must suffice to hold at least one tuple.");
        const auto numberOfBuffersToFill = calculateNumberOfRequiredFormattedBuffers(
            fieldAccessFunction.getTotalNumberOfTuples(), numberOfTuplesInFirstFormattedBuffer, numberOfTuplesPerBuffer);

        /// Determine the total number of tuples to produce, including potential prior (spanning) tuples
        /// If the first buffer is full already, the first iteration of the for loop below does 'nothing'
        size_t numberOfFormattedTuplesToProduce = fieldAccessFunction.getTotalNumberOfTuples() + numberOfTuplesInFirstFormattedBuffer;
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
                pec.emitBuffer(formattedBuffer, Runtime::Execution::PipelineExecutionContext::ContinuationPolicy::POSSIBLE);
                /// The 'isLastChunk' member of a new buffer is true pre default. If we don't require another buffer, the flag stays true.
                formattedBuffer = bufferProvider->getBufferBlocking();
            }

            /// Fill current buffer until either full, or we exhausted tuples in raw buffer
            while (formattedBuffer.getNumberOfTuples() < numberOfTuplesToRead)
            {
                processTuple<FieldAccessFunctionType>(
                    rawBuffer.getBufferView(),
                    fieldAccessFunction,
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
        const FieldAccessFunction<FieldAccessFunctionType>& fieldAccessFunction,
        Runtime::Execution::PipelineExecutionContext& pec) const
    {
        const auto bufferProvider = pec.getBufferManager();
        const auto [indexOfSequenceNumberInStagedBuffers, stagedBuffers] = sequenceShredder->processSequenceNumber<true>(
            StagedBuffer{
                rawBuffer,
                rawBuffer.getNumberOfBytes(),
                fieldAccessFunction.getOffsetOfFirstTupleDelimiter(),
                fieldAccessFunction.getOffsetOfLastTupleDelimiter()},
            rawBuffer.getSequenceNumber().getRawValue());

        /// 1. process leading spanning tuple if required
        auto formattedBuffer = bufferProvider->getBufferBlocking();
        if (/* hasLeadingSpanningTuple */ indexOfSequenceNumberInStagedBuffers != 0)
        {
            const auto spanningTupleBuffers = std::span(stagedBuffers).subspan(0, indexOfSequenceNumberInStagedBuffers + 1);
            processSpanningTuple<FieldAccessFunctionType, FormatterType::UsesNativeFormat>(
                spanningTupleBuffers, *bufferProvider, formattedBuffer, this->tupleMetaData, *this->inputFormatter, this->parseFunctions);
        }

        /// 2. process tuples in buffer
        if (fieldAccessFunction.getTotalNumberOfTuples() > 0)
        {
            parseRawBuffer(rawBuffer, runningChunkNumber, fieldAccessFunction, formattedBuffer, pec);
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
                pec.emitBuffer(formattedBuffer, NES::Runtime::Execution::PipelineExecutionContext::ContinuationPolicy::POSSIBLE);
                formattedBuffer = bufferProvider->getBufferBlocking();
            }

            const auto spanningTupleBuffers
                = std::span(stagedBuffers)
                      .subspan(indexOfSequenceNumberInStagedBuffers, stagedBuffers.size() - indexOfSequenceNumberInStagedBuffers);
            processSpanningTuple<FieldAccessFunctionType, FormatterType::UsesNativeFormat>(
                spanningTupleBuffers, *bufferProvider, formattedBuffer, this->tupleMetaData, *this->inputFormatter, this->parseFunctions);
        }
        /// If a raw buffer contains exactly one delimiter, but does not complete a spanning tuple, the formatted buffer does not contain a tuple
        if (formattedBuffer.getNumberOfTuples() != 0)
        {
            formattedBuffer.setSequenceNumber(rawBuffer.getSequenceNumber());
            formattedBuffer.setChunkNumber(NES::ChunkNumber(runningChunkNumber++));
            formattedBuffer.setOriginId(rawBuffer.getOriginId());
            pec.emitBuffer(formattedBuffer, NES::Runtime::Execution::PipelineExecutionContext::ContinuationPolicy::POSSIBLE);
        }
    }

    /// Called by execute, if the buffer does not delimit any tuples.
    /// Processes a spanning tuple, if the raw buffer connects two raw buffers that delimit tuples.
    void processRawBufferWithoutTupleDelimiter(
        const RawTupleBuffer& rawBuffer,
        ChunkNumber::Underlying& runningChunkNumber,
        const FieldAccessFunction<FieldAccessFunctionType>& fieldAccessFunction,
        Runtime::Execution::PipelineExecutionContext& pec) const
    {
        const auto bufferProvider = pec.getBufferManager();
        const auto [indexOfSequenceNumberInStagedBuffers, stagedBuffers] = sequenceShredder->processSequenceNumber<false>(
            StagedBuffer{
                rawBuffer,
                rawBuffer.getNumberOfBytes(),
                fieldAccessFunction.getOffsetOfFirstTupleDelimiter(),
                fieldAccessFunction.getOffsetOfLastTupleDelimiter()},
            rawBuffer.getSequenceNumber().getRawValue());
        if (stagedBuffers.size() < 3)
        {
            return;
        }
        /// If there is a spanning tuple, get a new buffer for formatted data and process the spanning tuples
        auto formattedBuffer = bufferProvider->getBufferBlocking();
        processSpanningTuple<FieldAccessFunctionType, FormatterType::UsesNativeFormat>(
            stagedBuffers, *bufferProvider, formattedBuffer, this->tupleMetaData, *this->inputFormatter, this->parseFunctions);

        formattedBuffer.setSequenceNumber(rawBuffer.getSequenceNumber());
        formattedBuffer.setChunkNumber(NES::ChunkNumber(runningChunkNumber++));
        formattedBuffer.setOriginId(rawBuffer.getOriginId());
        pec.emitBuffer(formattedBuffer, NES::Runtime::Execution::PipelineExecutionContext::ContinuationPolicy::POSSIBLE);
    }
};

}
