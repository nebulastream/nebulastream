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
#include <cstddef>
#include <ostream>
#include <string>
#include <string_view>
#include <utility>

#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <InputFormatters/InputFormatter.hpp>
#include <InputFormatters/InputFormatterTask.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <FieldOffsets.hpp>
#include <PipelineExecutionContext.hpp>
#include <RawInputDataParser.hpp>
#include <SequenceShredder.hpp>
#include <Common/PhysicalTypes/BasicPhysicalType.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>

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

}

namespace NES::InputFormatters
{

struct BufferData
{
    Memory::TupleBuffer rawBuffer;
    SequenceNumber::Underlying sequenceNumberOfCurrentBuffer;
    Memory::AbstractBufferProvider* bufferProvider;
    size_t sizeOfFormattedTupleBufferInBytes;
    InputFormatter::FirstAndLastTupleDelimiterOffsets bufferOffsets;
    size_t numberOfTuplesInRawBuffer;
};

InputFormatterTask::InputFormatterTask(
    const OriginId originId,
    std::unique_ptr<InputFormatter> inputFormatter,
    const Schema& schema,
    const Sources::InputFormatterConfig& parserConfig)
    : originId(originId)
    , inputFormatter(std::move(inputFormatter))
    , sequenceShredder(std::make_unique<SequenceShredder>(parserConfig.tupleDelimiter.size()))
    , tupleDelimiter(parserConfig.tupleDelimiter)
    , fieldDelimiter(parserConfig.fieldDelimiter)
    , numberOfFieldsInSchema(schema.getFieldCount())
    , sizeOfTupleInBytes(schema.getSchemaSizeInBytes())
{
    this->fieldConfigs.reserve(numberOfFieldsInSchema);

    /// Since we know the schema, we can create a vector that contains a function that converts the string representation of a field value
    /// to our internal representation in the correct order. During parsing, we iterate over the fields in each tuple, and, using the current
    /// field number, load the correct function for parsing from the vector.
    for (const std::shared_ptr<AttributeField>& field : schema)
    {
        const auto defaultPhysicalTypeFactory = DefaultPhysicalTypeFactory();
        const auto physicalType = defaultPhysicalTypeFactory.getPhysicalType(field->getDataType());
        /// Store the size of the field in bytes (for offset calculations).
        /// Store the parsing function in a vector.
        if (const auto basicPhysicalType = std::dynamic_pointer_cast<BasicPhysicalType>(physicalType))
        {
            fieldConfigs.emplace_back(physicalType->getSizeInBytes(), RawInputDataParser::getBasicTypeParseFunction(*basicPhysicalType));
        }
        else
        {
            fieldConfigs.emplace_back(physicalType->getSizeInBytes(), RawInputDataParser::getBasicStringParseFunction());
        }
    }
}

InputFormatterTask::~InputFormatterTask() = default;

void InputFormatterTask::execute(const Memory::TupleBuffer& rawBuffer, Runtime::Execution::PipelineExecutionContext& pec)
{
    if (rawBuffer.getBufferSize() == 0)
    {
        NES_WARNING("Received empty buffer in InputFormatterTask.");
        return;
    }
    /// Check if the current sequence number is in the range of the ring buffer of the sequence shredder.
    /// If not (should very rarely be the case), we put the task back.
    /// After enough out-of-range requests, the SequenceShredder increases the size of its ring buffer.
    if (not sequenceShredder->isInRange(rawBuffer.getSequenceNumber().getRawValue()))
    {
        pec.emitBuffer(rawBuffer, Runtime::Execution::PipelineExecutionContext::ContinuationPolicy::REPEAT);
        return;
    }

    auto bufferData = BufferData{};
    bufferData.rawBuffer = rawBuffer;
    bufferData.sequenceNumberOfCurrentBuffer = bufferData.rawBuffer.getSequenceNumber().getRawValue();
    ChunkNumber::Underlying runningChunkNumber = ChunkNumber::INITIAL;
    /// Get a raw pointer to the buffer manager to avoid reference counting of shared pointer and virtual function calls to getBufferManager
    bufferData.bufferProvider = pec.getBufferManager().get();
    bufferData.sizeOfFormattedTupleBufferInBytes = bufferData.bufferProvider->getBufferSize();

    /// Construct new FieldOffsets object
    auto fieldOffsets = FieldOffsets(numberOfFieldsInSchema, bufferData.sizeOfFormattedTupleBufferInBytes, bufferData.bufferProvider);

    /// Get indexes field delimiters in the raw buffer using the InputFormatter implementation
    const auto bufferView = std::string_view(bufferData.rawBuffer.getBuffer<char>(), bufferData.rawBuffer.getNumberOfTuples());
    bufferData.bufferOffsets = inputFormatter->indexBuffer(bufferView, fieldOffsets);

    /// Finalize the state of the field offsets and get the final number of tuples.
    /// Determine whether raw input buffer delimits at least two tuples.
    bufferData.numberOfTuplesInRawBuffer = fieldOffsets.finishWrite();

    if (/* hasTupleDelimiter */ bufferData.bufferOffsets.offsetOfLastTupleDelimiter != static_cast<FieldOffsetsType>(std::string::npos))
    {
        /// If the buffer delimits at least two tuples, it may produce two (leading/trailing) spanning tuples and may contain full tuples
        /// in its raw input buffer.
        processRawBufferWithTupleDelimiter(bufferData, runningChunkNumber, fieldOffsets, pec);
    }
    else
    {
        /// If the buffer does not delimit a single tuple, it may still connect two buffers that delimit tuples and therefore comple a
        /// spanning tuple.
        processRawBufferWithoutTupleDelimiter(bufferData, runningChunkNumber, pec);
    }
}

void InputFormatterTask::processRawBufferWithTupleDelimiter(
    const BufferData& bufferData,
    ChunkNumber::Underlying& runningChunkNumber,
    FieldOffsets& fieldOffsets,
    Runtime::Execution::PipelineExecutionContext& pec) const
{
    const auto [indexOfSequenceNumberInStagedBuffers, stagedBuffers] = sequenceShredder->processSequenceNumber<true>(
        StagedBuffer{
            bufferData.rawBuffer,
            bufferData.rawBuffer.getNumberOfTuples(),
            bufferData.bufferOffsets.offsetOfFirstTupleDelimiter,
            bufferData.bufferOffsets.offsetOfLastTupleDelimiter},
        bufferData.sequenceNumberOfCurrentBuffer);

    /// 1. process leading spanning tuple if required
    auto formattedBuffer = bufferData.bufferProvider->getBufferBlocking();
    if (/* hasLeadingSpanningTuple */ indexOfSequenceNumberInStagedBuffers != 0)
    {
        processSpanningTuple(
            {.spanStart = 0, .spanEnd = indexOfSequenceNumberInStagedBuffers}, stagedBuffers, *bufferData.bufferProvider, formattedBuffer);
    }

    /// 2. process tuples in buffer
    if (bufferData.numberOfTuplesInRawBuffer > 0)
    {
        processRawBuffer(bufferData, runningChunkNumber, fieldOffsets, formattedBuffer, pec);
    }

    /// 3. process trailing spanning tuple if required
    if (/* hasTrailingPartialTuple */ indexOfSequenceNumberInStagedBuffers < (stagedBuffers.size() - 1))
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
            {.spanStart = indexOfSequenceNumberInStagedBuffers, .spanEnd = stagedBuffers.size() - 1},
            stagedBuffers,
            *bufferData.bufferProvider,
            formattedBuffer);
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
            bufferData.bufferOffsets.offsetOfFirstTupleDelimiter,
            bufferData.bufferOffsets.offsetOfLastTupleDelimiter},
        bufferData.sequenceNumberOfCurrentBuffer);
    if (stagedBuffers.size() < 3)
    {
        return;
    }
    /// If there is a spanning tuple, get a new buffer for formatted data and process the partial tuples
    auto formattedBuffer = bufferData.bufferProvider->getBufferBlocking();
    processSpanningTuple({.spanStart = 0, .spanEnd = stagedBuffers.size() - 1}, stagedBuffers, *bufferData.bufferProvider, formattedBuffer);

    formattedBuffer.setSequenceNumber(bufferData.rawBuffer.getSequenceNumber());
    formattedBuffer.setChunkNumber(NES::ChunkNumber(runningChunkNumber++));
    formattedBuffer.setOriginId(bufferData.rawBuffer.getOriginId());
    pec.emitBuffer(formattedBuffer, NES::Runtime::Execution::PipelineExecutionContext::ContinuationPolicy::POSSIBLE);
}

void InputFormatterTask::processTuple(
    const char* tupleStart,
    const FieldOffsetsType* fieldIndexes,
    Memory::AbstractBufferProvider& bufferProvider,
    Memory::TupleBuffer& formattedBuffer) const
{
    size_t fieldOffsetInRawData = 0;
    size_t fieldOffsetInFormattedData = 0;
    /// There is one more field offset than there are fields, to enclose all fields, which allows to exactly calculate the size of all fields
    /// including the first and the last field of the tuple
    for (size_t fieldIndex = 0; fieldIndex < numberOfFieldsInSchema; ++fieldIndex)
    {
        FieldOffsetsType currentFieldOffset{};
        FieldOffsetsType sizeOfCurrentRawField{};
        currentFieldOffset = fieldIndexes[fieldIndex];
        sizeOfCurrentRawField = fieldIndexes[fieldIndex + 1] - currentFieldOffset;
        const auto* const currentRawFieldsPtr = tupleStart + currentFieldOffset;
        auto* const currentFormattedFieldsPtr
            = formattedBuffer.getBuffer() + fieldOffsetInFormattedData + (formattedBuffer.getNumberOfTuples() * sizeOfTupleInBytes);

        /// Deduct the size of the field delimiter for all fields except for the last
        const auto isLastField = fieldIndex == numberOfFieldsInSchema - 1;
        sizeOfCurrentRawField = (isLastField) ? sizeOfCurrentRawField : sizeOfCurrentRawField - fieldDelimiter.size();

        const auto sizeOfCurrentFormattedField = fieldConfigs[fieldIndex].size;

        const auto currentFieldSV = std::string_view(currentRawFieldsPtr, sizeOfCurrentRawField);
        fieldConfigs[fieldIndex].parseFunction(currentFieldSV, currentFormattedFieldsPtr, bufferProvider, formattedBuffer);

        fieldOffsetInRawData += sizeOfCurrentRawField;
        fieldOffsetInFormattedData += sizeOfCurrentFormattedField;
    }
}

void InputFormatterTask::processSpanningTuple(
    const SpanningTuple& spanningTuple,
    const std::vector<StagedBuffer>& buffersToFormat,
    Memory::AbstractBufferProvider& bufferProvider,
    Memory::TupleBuffer& formattedBuffer) const
{
    /// If the buffers are not empty, there are at least three buffers
    std::string partialTuple;
    /// 1. Process the first buffer
    const auto& firstBuffer = buffersToFormat[spanningTuple.spanStart];
    // Todo: add preconditions to catch errors concerning the string_views
    const auto sizeOfLeadingPartialTuple
        = firstBuffer.sizeOfBufferInBytes - (firstBuffer.offsetOfLastTupleDelimiter + tupleDelimiter.size());
    const std::string_view firstPartialTuple = std::string_view(
        firstBuffer.buffer.getBuffer<const char>() + (firstBuffer.offsetOfLastTupleDelimiter + 1), sizeOfLeadingPartialTuple);
    partialTuple.append(firstPartialTuple);
    /// 2. Process all buffers in-between the first and the last
    for (size_t bufferIndex = spanningTuple.spanStart + 1; bufferIndex < spanningTuple.spanEnd; ++bufferIndex)
    {
        const auto& currentBuffer = buffersToFormat[bufferIndex];
        const std::string_view intermediatePartialTuple
            = std::string_view(currentBuffer.buffer.getBuffer<const char>(), currentBuffer.sizeOfBufferInBytes);
        partialTuple.append(intermediatePartialTuple);
    }

    /// 3. Process the last buffer
    const auto& lastBuffer = buffersToFormat[spanningTuple.spanEnd];
    const std::string_view lastPartialTuple
        = std::string_view(lastBuffer.buffer.getBuffer<const char>(), lastBuffer.offsetOfFirstTupleDelimiter);
    partialTuple.append(lastPartialTuple);

    /// A partial tuple may currently be empty (the 'stop' call produces an empty tuple, if the last buffer ends in a tuple delimiter)
    if (not partialTuple.empty())
    {
        std::vector<FieldOffsetsType> partialTupleOffset(numberOfFieldsInSchema + 1);
        inputFormatter->indexTuple(partialTuple, partialTupleOffset.data(), 0, partialTuple.size());

        processTuple(partialTuple.data(), partialTupleOffset.data(), bufferProvider, formattedBuffer);
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
        {.spanStart = 0, .spanEnd = flushedBuffers.size() - 1},
        flushedBuffers,
        *pipelineExecutionContext.getBufferManager(),
        formattedBuffer);
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
    ChunkNumber::Underlying& runningChunkNumber,
    FieldOffsets& fieldOffsets,
    Memory::TupleBuffer& formattedBuffer,
    Runtime::Execution::PipelineExecutionContext& pec) const
{
    const auto numberOfTuplesInFirstFormattedBuffer = formattedBuffer.getNumberOfTuples();
    const size_t numberOfTuplesPerBuffer = bufferData.sizeOfFormattedTupleBufferInBytes / sizeOfTupleInBytes;
    PRECONDITION(numberOfTuplesPerBuffer != 0, "The capacity of a buffer must suffice to hold at least one tuple.");
    const auto numberOfBuffersToFill = estimateNumberOfRequiredBuffers(
        bufferData.numberOfTuplesInRawBuffer, numberOfTuplesInFirstFormattedBuffer, numberOfTuplesPerBuffer);

    /// Determine the total number of tuples to produce, including potential prior (partial) tuples
    /// If the first buffer is full already, the first iteration of the for loop below does 'nothing'
    size_t numberOfFormattedTuplesToProduce = bufferData.numberOfTuplesInRawBuffer + numberOfTuplesInFirstFormattedBuffer;

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
            formattedBuffer.setLastChunk(false);
            formattedBuffer.setSequenceNumber(bufferData.rawBuffer.getSequenceNumber());
            formattedBuffer.setChunkNumber(ChunkNumber(runningChunkNumber++));
            formattedBuffer.setOriginId(bufferData.rawBuffer.getOriginId());
            pec.emitBuffer(formattedBuffer, Runtime::Execution::PipelineExecutionContext::ContinuationPolicy::POSSIBLE);
            /// The 'isLastChunk' member of a new buffer is true pre default. If we don't require another buffer, the flag stays true.
            formattedBuffer = bufferData.bufferProvider->getBufferBlocking();
        }

        /// Fill current buffer until either full, or we exhausted tuples in raw buffer
        while (formattedBuffer.getNumberOfTuples() < numberOfTuplesToRead)
        {
            processTuple(bufferData.rawBuffer.getBuffer<char>(), fieldOffsets.readNextTuple(), *bufferData.bufferProvider, formattedBuffer);
            formattedBuffer.setNumberOfTuples(formattedBuffer.getNumberOfTuples() + 1);
        }
        numberOfFormattedTuplesToProduce -= formattedBuffer.getNumberOfTuples();
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