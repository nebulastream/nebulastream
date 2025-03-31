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

#include <algorithm>
#include <cstdint>
#include <memory>
#include <ostream>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>
#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <InputFormatters/InputFormatter.hpp>
#include <InputFormatters/InputFormatterTask.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <ErrorHandling.hpp>
#include <FieldOffsetsIterator.hpp>
#include <RawInputDataParser.hpp>
#include <SequenceShredder.hpp>
#include <Common/PhysicalTypes/BasicPhysicalType.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>

namespace
{
size_t estimateNumberOfRequiredBuffers(
    const size_t totalNumberOfTuplesInRawBuffer, const size_t numberOfTuplesInFirstFormattedBuffer, const size_t numberOfTuplesPerBuffer)
{
    const auto capacityOfFirstBuffer
        = std::min(numberOfTuplesPerBuffer - numberOfTuplesInFirstFormattedBuffer, totalNumberOfTuplesInRawBuffer);
    const auto numberOfTuplesThatDoNotFitIntoFirstFormattedBuffer = totalNumberOfTuplesInRawBuffer - capacityOfFirstBuffer;
    /// We need at least one (first) formatted buffer. We need additional buffers if we have leftover raw tuples.
    /// Overflow-safe calculation from (https://stackoverflow.com/questions/2745074/fast-ceiling-of-an-integer-division-in-c-c)
    const auto numberOfBuffersToFill = 1
        + (((numberOfTuplesThatDoNotFitIntoFirstFormattedBuffer % numberOfTuplesPerBuffer) != 0u)
               ? (numberOfTuplesThatDoNotFitIntoFirstFormattedBuffer / numberOfTuplesPerBuffer) + 1
               : numberOfTuplesThatDoNotFitIntoFirstFormattedBuffer / numberOfTuplesPerBuffer);
    return numberOfBuffersToFill;
}


template <typename FieldOffsetsType>
void processTuple(
    const char* tupleStart,
    FieldOffsetsType fieldIndexes,
    const size_t numberOfFields,
    const size_t sizeOfFieldDelimiter,
    NES::Memory::AbstractBufferProvider& bufferProvider,
    NES::Memory::TupleBuffer& formattedBuffer,
    const std::vector<NES::InputFormatters::InputFormatterTask::ParseFunctionSignature>& fieldParseFunctions,
    const NES::InputFormatters::FieldOffsetsType offsetToFormattedBuffer,
    const std::vector<size_t>& fieldSizes)
{
    size_t fieldOffsetInRawData = 0;
    size_t fieldOffsetInFormattedData = 0;
    /// There is one more field offset than there are fields, to enclose all fields, which allows to exactly calculate the size of all fields
    /// including the first and the last filed of the tuple
    for (size_t fieldIndex = 0; fieldIndex < numberOfFields; ++fieldIndex)
    {
        NES::InputFormatters::FieldOffsetsType currentFieldOffset{};
        NES::InputFormatters::FieldOffsetsType sizeOfCurrentRawField{};
        if constexpr (std::is_pointer_v<std::remove_reference_t<FieldOffsetsType>>)
        {
            currentFieldOffset = fieldIndexes[fieldIndex];
            sizeOfCurrentRawField = fieldIndexes[fieldIndex + 1] - currentFieldOffset;
        }
        else
        {
            currentFieldOffset = fieldIndexes.read();
            ++fieldIndexes;
            sizeOfCurrentRawField = fieldIndexes.read() - currentFieldOffset;
        }
        const auto* const currentRawFieldsPtr = tupleStart + currentFieldOffset;
        auto* const currentFormattedFieldsPtr = formattedBuffer.getBuffer() + fieldOffsetInFormattedData + offsetToFormattedBuffer;

        /// Deduct the size of the field delimiter for all fields except for the last
        const auto isLastField = fieldIndex == numberOfFields - 1;
        sizeOfCurrentRawField = (isLastField) ? sizeOfCurrentRawField : sizeOfCurrentRawField - sizeOfFieldDelimiter;

        const auto sizeOfCurrentFormattedField = fieldSizes[fieldIndex];

        const auto currentFieldSV = std::string_view(currentRawFieldsPtr, sizeOfCurrentRawField);
        fieldParseFunctions[fieldIndex](currentFieldSV, currentFormattedFieldsPtr, bufferProvider, formattedBuffer);

        fieldOffsetInRawData += sizeOfCurrentRawField;
        fieldOffsetInFormattedData += sizeOfCurrentFormattedField;
    }
    if constexpr (not(std::is_pointer_v<std::remove_reference_t<FieldOffsetsType>>))
    {
        ++fieldIndexes;
    }
}

}

namespace NES::InputFormatters
{

struct BufferData
{
    Memory::TupleBuffer rawBuffer;
    size_t numberOfTuplesInRawBuffer;
    size_t sizeOfFormattedTupleBufferInBytes;
    SequenceNumber::Underlying sequenceNumberOfCurrentBuffer;
    InputFormatter::BufferOffsets offsets;
    Memory::AbstractBufferProvider* bufferProvider;
};

InputFormatterTask::InputFormatterTask(
    const OriginId originId,
    std::string tupleDelimiter,
    std::string fieldDelimiter,
    const Schema& schema,
    std::unique_ptr<InputFormatter> inputFormatter)
    : originId(originId)
    , sequenceShredder(std::make_unique<SequenceShredder>(tupleDelimiter.size()))
    , tupleDelimiter(std::move(tupleDelimiter))
    , fieldDelimiter(std::move(fieldDelimiter))
    , numberOfFieldsInSchema(schema.getFieldCount())
    , inputFormatter(std::move(inputFormatter))
    , sizeOfTupleInBytes(schema.getSchemaSizeInBytes())
{
    this->fieldSizes.reserve(numberOfFieldsInSchema);
    this->fieldParseFunctions.reserve(numberOfFieldsInSchema);
    std::vector<std::shared_ptr<PhysicalType>> physicalTypes;
    const auto defaultPhysicalTypeFactory = DefaultPhysicalTypeFactory();
    physicalTypes.reserve(numberOfFieldsInSchema);
    for (const std::shared_ptr<AttributeField>& field : schema)
    {
        physicalTypes.emplace_back(defaultPhysicalTypeFactory.getPhysicalType(field->getDataType()));
    }

    /// Since we know the schema, we can create a vector that contains a function that converts the string representation of a field value
    /// to our internal representation in the correct order. During parsing, we iterate over the fields in each tuple, and, using the current
    /// field number, load the correct function for parsing from the vector.
    for (const auto& physicalType : physicalTypes)
    {
        /// Store the size of the field in bytes (for offset calculations).
        this->fieldSizes.emplace_back(physicalType->size());
        /// Store the parsing function in a vector.
        if (const auto basicPhysicalType = std::dynamic_pointer_cast<BasicPhysicalType>(physicalType))
        {
            RawInputDataParser::addBasicTypeParseFunction(*basicPhysicalType, this->fieldParseFunctions);
        }
        else
        {
            RawInputDataParser::addBasicStringParseFunction(this->fieldParseFunctions);
        }
    }
}
InputFormatterTask::~InputFormatterTask() = default;

void InputFormatterTask::processSpanningTuple(
    const FieldOffsetsType partialTupleStartIdx,
    const FieldOffsetsType partialTupleEndIdx,
    const size_t sizeOfTupleDelimiter,
    const std::vector<StagedBuffer>& buffersToFormat,
    Memory::AbstractBufferProvider& bufferProvider,
    Memory::TupleBuffer& formattedBuffer,
    const size_t offsetToFormattedBuffer) const
{
    /// If the buffers are not empty, there are at least three buffers
    std::string partialTuple;
    /// 1. Process the first buffer
    const auto& firstBuffer = buffersToFormat[partialTupleStartIdx];
    const auto sizeOfLeadingPartialTuple
        = firstBuffer.sizeOfBufferInBytes - (firstBuffer.offsetOfLastTupleDelimiter + sizeOfTupleDelimiter);
    const std::string_view firstPartialTuple = std::string_view(
        firstBuffer.buffer.getBuffer<const char>() + (firstBuffer.offsetOfLastTupleDelimiter + 1), sizeOfLeadingPartialTuple);
    partialTuple.append(firstPartialTuple);
    /// 2. Process all buffers in-between the first and the last
    for (size_t bufferIndex = partialTupleStartIdx + 1; bufferIndex < partialTupleEndIdx; ++bufferIndex)
    {
        const auto& currentBuffer = buffersToFormat[bufferIndex];
        const std::string_view intermediatePartialTuple
            = std::string_view(currentBuffer.buffer.getBuffer<const char>(), currentBuffer.sizeOfBufferInBytes);
        partialTuple.append(intermediatePartialTuple);
    }

    /// 3. Process the last buffer
    const auto& lastBuffer = buffersToFormat[partialTupleEndIdx];
    const std::string_view lastPartialTuple
        = std::string_view(lastBuffer.buffer.getBuffer<const char>(), lastBuffer.offsetOfFirstTupleDelimiter);
    partialTuple.append(lastPartialTuple);

    /// A partial tuple may currently be empty (the 'stop' call produces an empty tuple, if the last buffer ends in a tuple delimiter)
    if (not partialTuple.empty())
    {
        std::vector<NES::InputFormatters::FieldOffsetsType> partialTupleOffset(numberOfFieldsInSchema + 1);
        inputFormatter->indexSpanningTuple(partialTuple, fieldDelimiter, partialTupleOffset.data(), 0, partialTuple.size(), 0);

        processTuple<NES::InputFormatters::FieldOffsetsType*>(
            partialTuple.data(),
            partialTupleOffset.data(),
            numberOfFieldsInSchema,
            fieldDelimiter.size(),
            bufferProvider,
            formattedBuffer,
            fieldParseFunctions,
            offsetToFormattedBuffer,
            fieldSizes);
        formattedBuffer.setNumberOfTuples(formattedBuffer.getNumberOfTuples() + 1);
    }
}

void InputFormatterTask::stop(Runtime::Execution::PipelineExecutionContext& pipelineExecutionContext)
{
    const auto [resultBuffers, sequenceNumberToUseForFlushedTuple] = sequenceShredder->flushFinalPartialTuple();
    const auto flushedBuffers = resultBuffers.stagedBuffers;
    /// The sequence shredder injects a dummy buffer, which it always returns. Thus, a single returned buffer means no final partial tuple.
    if (flushedBuffers.size() == 1)
    {
        return;
    }
    /// Get the final buffer (size - 1 is dummy buffer that just contains tuple delimiter)
    /// Allocate buffer to write formatted tuples into.
    auto formattedBuffer = pipelineExecutionContext.getBufferManager()->getBufferBlocking();

    processSpanningTuple(
        0,
        flushedBuffers.size() - 1,
        tupleDelimiter.size(),
        flushedBuffers,
        *pipelineExecutionContext.getBufferManager(),
        formattedBuffer,
        0);
    /// Emit formatted buffer with single flushed tuple
    if (formattedBuffer.getNumberOfTuples() != 0)
    {
        formattedBuffer.setSequenceNumber(NES::SequenceNumber(sequenceNumberToUseForFlushedTuple));
        pipelineExecutionContext.emitBuffer(
            formattedBuffer, NES::Runtime::Execution::PipelineExecutionContext::ContinuationPolicy::POSSIBLE);
    }
}

void InputFormatterTask::processRawBuffer(
    const BufferData& bufferData,
    NES::ChunkNumber::Underlying& runningChunkNumber,
    FieldOffsetIterator& fieldOffsets,
    NES::Memory::TupleBuffer& formattedBuffer,
    Runtime::Execution::PipelineExecutionContext& pec) const
{
    const auto numberOfTuplesInFirstFormattedBuffer = formattedBuffer.getNumberOfTuples();
    const size_t numberOfTuplesPerBuffer = bufferData.sizeOfFormattedTupleBufferInBytes / sizeOfTupleInBytes;
    PRECONDITION(numberOfTuplesPerBuffer != 0, "The capacity of a buffer must suffice to hold at least one buffer");
    const auto numberOfBuffersToFill = estimateNumberOfRequiredBuffers(
        bufferData.numberOfTuplesInRawBuffer, numberOfTuplesInFirstFormattedBuffer, numberOfTuplesPerBuffer);

    /// Determine the total number of tuples to produce, including potential prior (partial) tuples
    /// If the first buffer is full already, the first iteration of the for loop below does 'nothing'
    size_t numberOfFormattedTuplesToProduce = bufferData.numberOfTuplesInRawBuffer + numberOfTuplesInFirstFormattedBuffer;
    size_t tupleIdxOfCurrentFormattedBuffer = numberOfTuplesInFirstFormattedBuffer;

    /// Initialize indexes for offset buffer
    for (size_t bufferIdx = 0; bufferIdx < numberOfBuffersToFill; ++bufferIdx)
    {
        /// Either fill the entire buffer, or process the leftover formatted tuples to produce
        const size_t numberOfTuplesToRead = std::min(numberOfTuplesPerBuffer, numberOfFormattedTuplesToProduce);
        /// If the current buffer is not the first buffer, set the meta data of the prior buffer, emit it, get a new buffer and reset the associated counters
        if (bufferIdx != 0)
        {
            formattedBuffer.setSequenceNumber(bufferData.rawBuffer.getSequenceNumber());
            formattedBuffer.setChunkNumber(NES::ChunkNumber(runningChunkNumber++));
            formattedBuffer.setOriginId(bufferData.rawBuffer.getOriginId());
            pec.emitBuffer(formattedBuffer, Runtime::Execution::PipelineExecutionContext::ContinuationPolicy::POSSIBLE);
            formattedBuffer = bufferData.bufferProvider->getBufferBlocking();

            tupleIdxOfCurrentFormattedBuffer = 0;
        }

        /// Fill current buffer until either full, or we exhausted tuples in raw buffer
        while (tupleIdxOfCurrentFormattedBuffer < numberOfTuplesToRead)
        {
            processTuple<FieldOffsetIterator&>(
                bufferData.rawBuffer.getBuffer<char>(),
                fieldOffsets,
                numberOfFieldsInSchema,
                fieldDelimiter.size(),
                *bufferData.bufferProvider,
                formattedBuffer,
                fieldParseFunctions,
                tupleIdxOfCurrentFormattedBuffer * sizeOfTupleInBytes,
                this->fieldSizes);
            ++tupleIdxOfCurrentFormattedBuffer;
        }
        numberOfFormattedTuplesToProduce -= (tupleIdxOfCurrentFormattedBuffer);
        formattedBuffer.setNumberOfTuples(tupleIdxOfCurrentFormattedBuffer);
    }
}


void InputFormatterTask::processRawBufferWithTupleDelimiter(
    const BufferData& bufferData,
    ChunkNumber::Underlying& runningChunkNumber,
    FieldOffsetIterator& fieldOffsets,
    Runtime::Execution::PipelineExecutionContext& pec) const
{
    const auto [indexOfSequenceNumberInStagedBuffers, stagedBuffers] = sequenceShredder->processSequenceNumber<true>(
        StagedBuffer{
            bufferData.rawBuffer,
            bufferData.rawBuffer.getNumberOfTuples(),
            bufferData.offsets.offsetOfFirstTupleDelimiter,
            bufferData.offsets.offsetOfLastTupleDelimiter},
        bufferData.sequenceNumberOfCurrentBuffer);

    /// 1. process leading partial tuple if required
    auto formattedBuffer = bufferData.bufferProvider->getBufferBlocking();
    auto hasLeadingPartialTuple = indexOfSequenceNumberInStagedBuffers != 0;
    if (hasLeadingPartialTuple)
    {
        processSpanningTuple(
            0, indexOfSequenceNumberInStagedBuffers, tupleDelimiter.size(), stagedBuffers, *bufferData.bufferProvider, formattedBuffer, 0);
    }

    /// 2. process tuples in buffer
    if (bufferData.numberOfTuplesInRawBuffer > 0)
    {
        processRawBuffer(bufferData, runningChunkNumber, fieldOffsets, formattedBuffer, pec);
    }

    /// 3. process trailing partial tuple if required
    const auto hasTrailingPartialTuple = indexOfSequenceNumberInStagedBuffers < stagedBuffers.size() - 1;
    if (hasTrailingPartialTuple)
    {
        const auto numBytesInFormattedBuffer = formattedBuffer.getNumberOfTuples() * sizeOfTupleInBytes;
        if (formattedBuffer.getBufferSize() - numBytesInFormattedBuffer < sizeOfTupleInBytes)
        {
            formattedBuffer.setSequenceNumber(bufferData.rawBuffer.getSequenceNumber());
            formattedBuffer.setChunkNumber(NES::ChunkNumber(runningChunkNumber++));
            formattedBuffer.setOriginId(bufferData.rawBuffer.getOriginId());
            pec.emitBuffer(formattedBuffer, NES::Runtime::Execution::PipelineExecutionContext::ContinuationPolicy::POSSIBLE);
            formattedBuffer = bufferData.bufferProvider->getBufferBlocking();
        }

        processSpanningTuple(
            indexOfSequenceNumberInStagedBuffers,
            stagedBuffers.size() - 1,
            tupleDelimiter.size(),
            stagedBuffers,
            *bufferData.bufferProvider,
            formattedBuffer,
            (formattedBuffer.getNumberOfTuples() * sizeOfTupleInBytes));
    }
    /// If a raw buffer contains exactly one delimiter, but does not complete a partial tuple, the formatted buffer does not contain a tuple
    if (formattedBuffer.getNumberOfTuples() != 0)
    {
        formattedBuffer.setSequenceNumber(bufferData.rawBuffer.getSequenceNumber());
        formattedBuffer.setChunkNumber(NES::ChunkNumber(runningChunkNumber++));
        formattedBuffer.setOriginId(bufferData.rawBuffer.getOriginId());
        pec.emitBuffer(formattedBuffer, NES::Runtime::Execution::PipelineExecutionContext::ContinuationPolicy::POSSIBLE);
    }
}

void InputFormatterTask::processRawBufferWithoutTupleDelimiter(
    const BufferData& bufferData, ChunkNumber::Underlying& runningChunkNumber, Runtime::Execution::PipelineExecutionContext& pec) const
{
    const auto [indexOfSequenceNumberInStagedBuffers, stagedBuffers] = sequenceShredder->processSequenceNumber<false>(
        StagedBuffer{
            bufferData.rawBuffer,
            bufferData.rawBuffer.getNumberOfTuples(),
            bufferData.offsets.offsetOfFirstTupleDelimiter,
            bufferData.offsets.offsetOfLastTupleDelimiter},
        bufferData.sequenceNumberOfCurrentBuffer);
    if (stagedBuffers.size() < 3)
    {
        return;
    }
    /// If there is a spanning tuple, get a new buffer for formatted data and process the partial tuples
    auto formattedBuffer = bufferData.bufferProvider->getBufferBlocking();
    processSpanningTuple(0, stagedBuffers.size() - 1, tupleDelimiter.size(), stagedBuffers, *bufferData.bufferProvider, formattedBuffer, 0);

    formattedBuffer.setSequenceNumber(bufferData.rawBuffer.getSequenceNumber());
    formattedBuffer.setChunkNumber(NES::ChunkNumber(runningChunkNumber++));
    formattedBuffer.setOriginId(bufferData.rawBuffer.getOriginId());
    pec.emitBuffer(formattedBuffer, NES::Runtime::Execution::PipelineExecutionContext::ContinuationPolicy::POSSIBLE);
}

void InputFormatterTask::execute(
    const Memory::TupleBuffer& rawBuffer, Runtime::Execution::PipelineExecutionContext& pipelineExecutionContext)
{
    if (rawBuffer.getBufferSize() == 0)
    {
        NES_WARNING("Received empty buffer in InputFormatterTask.");
        return;
    }
    const auto sequenceNumberOfCurrentBuffer = rawBuffer.getSequenceNumber().getRawValue();
    /// Check if the current sequence number is in the range of the ring buffer of the sequence shredder.
    /// If not (should very rarely be the case), we put the task back.
    /// After enough out-of-range requests, the SequenceShredder increases the size of its ring buffer.
    if (not sequenceShredder->isInRange(sequenceNumberOfCurrentBuffer))
    {
        pipelineExecutionContext.emitBuffer(rawBuffer, Runtime::Execution::PipelineExecutionContext::ContinuationPolicy::REPEAT);
        return;
    }

    NES::ChunkNumber::Underlying runningChunkNumber = ChunkNumber::INITIAL;
    /// Get a raw pointer to the buffer manager to avoid reference counting of shared pointer and virtual function calls to getBufferManager
    auto* bufferManager = pipelineExecutionContext.getBufferManager().get();
    // Todo: incrementally build up 'BufferData'?
    const auto sizeOfFormattedTupleBufferInBytes = bufferManager->getBufferSize();

    /// Construct new FieldOffsets object
    auto fieldOffsets
        = FieldOffsetIterator(numberOfFieldsInSchema, sizeOfFormattedTupleBufferInBytes, pipelineExecutionContext.getBufferManager());

    /// Get indexes to raw buffer using the InputFormatter implementation
    auto bufferOffsets = inputFormatter->indexRawBuffer(
        rawBuffer.getBuffer<char>(), rawBuffer.getNumberOfTuples(), fieldOffsets, tupleDelimiter, fieldDelimiter);

    /// Finalize the state of the field offsets and get the final number of tuples.
    /// Determine whether raw input buffer delimits at least two tuples.
    const auto totalNumberOfTuples = fieldOffsets.finishRead();

    const auto bufferData = BufferData{
        .rawBuffer = rawBuffer,
        .numberOfTuplesInRawBuffer = totalNumberOfTuples,
        .sizeOfFormattedTupleBufferInBytes = sizeOfFormattedTupleBufferInBytes,
        .sequenceNumberOfCurrentBuffer = sequenceNumberOfCurrentBuffer,
        .offsets = std::move(bufferOffsets),
        .bufferProvider = bufferManager,
    };

    if (/* hasTupleDelimiter */ bufferOffsets.offsetOfLastTupleDelimiter != static_cast<FieldOffsetsType>(std::string::npos))
    {
        /// If the buffer delimits at least two tuples, it may produce two (leading/trailing) spanning tuples and may contain full tuples
        /// in its raw input buffer.
        processRawBufferWithTupleDelimiter(bufferData, runningChunkNumber, fieldOffsets, pipelineExecutionContext);
    }
    else
    {
        /// If the buffer does not delimit a single tuple, it may still connect two buffers that delimit tuples and therefore comple a
        /// spanning tuple.
        processRawBufferWithoutTupleDelimiter(bufferData, runningChunkNumber, pipelineExecutionContext);
    }
}
std::ostream& InputFormatterTask::toString(std::ostream& os) const
{
    os << "InputFormatterTask: {\n";
    os << "  originId: " << originId << ",\n";
    os << *inputFormatter;
    os << *sequenceShredder;
    os << "}\n";
    return os;
}
}
