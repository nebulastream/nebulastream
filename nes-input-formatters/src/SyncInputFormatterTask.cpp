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
#include <ranges>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>
#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <InputFormatters/InputFormatter.hpp>
#include <InputFormatters/SyncInputFormatterTask.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <ErrorHandling.hpp>
#include <FieldOffsetsIterator.hpp>
#include <RawInputDataParser.hpp>
#include <Common/PhysicalTypes/BasicPhysicalType.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>

namespace
{
size_t calculateNumberOfRequiredBuffers(
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
    const std::vector<NES::InputFormatters::SyncInputFormatterTask::CastFunctionSignature>& fieldParseFunctions,
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

void executeStatelessFormatting(
    const NES::Memory::TupleBuffer& rawBuffer,
    NES::ChunkNumber::Underlying& runningChunkNumber,
    NES::InputFormatters::FieldOffsetIterator& fieldOffsets,
    NES::Memory::TupleBuffer& formattedBuffer,
    NES::Memory::AbstractBufferProvider& bufferProvider,
    const size_t totalNumberOfTuplesInRawBuffer,
    const size_t sizeOfFormattedTupleBufferInBytes,
    NES::Runtime::Execution::PipelineExecutionContext& pec,
    const size_t sizeOfFieldDelimiter,
    const uint32_t numberOfFieldsInSchema,
    const uint32_t sizeOfTupleInBytes,
    const std::vector<size_t>& fieldSizes,
    const std::vector<NES::InputFormatters::SyncInputFormatterTask::CastFunctionSignature>& fieldParseFunctions)
{
    /// 2. process tuples in buffer
    if (totalNumberOfTuplesInRawBuffer > 0)
    {
        const auto numberOfTuplesInFirstFormattedBuffer = formattedBuffer.getNumberOfTuples();
        const size_t numberOfTuplesPerBuffer = sizeOfFormattedTupleBufferInBytes / sizeOfTupleInBytes;
        PRECONDITION(numberOfTuplesPerBuffer != 0, "The capacity of a buffer must suffice to hold at least one buffer");
        const auto numberOfBuffersToFill = calculateNumberOfRequiredBuffers(
            totalNumberOfTuplesInRawBuffer, numberOfTuplesInFirstFormattedBuffer, numberOfTuplesPerBuffer);

        /// Determine the total number of tuples to produce, including potential prior (partial) tuples
        /// If the first buffer is full already, the first iteration of the for loop below does 'nothing'
        size_t numberOfFormattedTuplesToProduce = totalNumberOfTuplesInRawBuffer + numberOfTuplesInFirstFormattedBuffer;
        size_t tupleIdxOfCurrentFormattedBuffer = numberOfTuplesInFirstFormattedBuffer;

        /// Initialize indexes for offset buffer
        for (size_t bufferIdx = 0; bufferIdx < numberOfBuffersToFill; ++bufferIdx)
        {
            /// Either fill the entire buffer, or process the leftover formatted tuples to produce
            const size_t numberOfTuplesToRead = std::min(numberOfTuplesPerBuffer, numberOfFormattedTuplesToProduce);
            /// If the current buffer is not the first buffer, set the meta data of the prior buffer, emit it, get a new buffer and reset the associated counters
            if (bufferIdx != 0)
            {
                formattedBuffer.setSequenceNumber(rawBuffer.getSequenceNumber());
                formattedBuffer.setChunkNumber(NES::ChunkNumber(runningChunkNumber++));
                formattedBuffer.setOriginId(rawBuffer.getOriginId());
                pec.emitBuffer(formattedBuffer, NES::Runtime::Execution::PipelineExecutionContext::ContinuationPolicy::POSSIBLE);
                formattedBuffer = bufferProvider.getBufferBlocking();

                tupleIdxOfCurrentFormattedBuffer = 0;
            }

            /// Fill current buffer until either full, or we exhausted tuples in raw buffer
            while (tupleIdxOfCurrentFormattedBuffer < numberOfTuplesToRead)
            {
                processTuple<NES::InputFormatters::FieldOffsetIterator&>(
                    rawBuffer.getBuffer<char>(),
                    fieldOffsets,
                    numberOfFieldsInSchema,
                    sizeOfFieldDelimiter,
                    bufferProvider,
                    formattedBuffer,
                    fieldParseFunctions,
                    tupleIdxOfCurrentFormattedBuffer * sizeOfTupleInBytes,
                    fieldSizes);
                ++tupleIdxOfCurrentFormattedBuffer;
            }
            numberOfFormattedTuplesToProduce -= (tupleIdxOfCurrentFormattedBuffer);
            formattedBuffer.setNumberOfTuples(tupleIdxOfCurrentFormattedBuffer);
        }
    }
    /// If a raw buffer contains exactly one delimiter, but does not complete a partial tuple, the formatted buffer does not contain a tuple
    if (formattedBuffer.getNumberOfTuples() != 0)
    {
        formattedBuffer.setSequenceNumber(rawBuffer.getSequenceNumber());
        formattedBuffer.setChunkNumber(NES::ChunkNumber(runningChunkNumber++));
        formattedBuffer.setOriginId(rawBuffer.getOriginId());
        pec.emitBuffer(formattedBuffer, NES::Runtime::Execution::PipelineExecutionContext::ContinuationPolicy::POSSIBLE);
    }
}
}

namespace NES::InputFormatters
{

SyncInputFormatterTask::SyncInputFormatterTask(
    const OriginId originId,
    std::string tupleDelimiter,
    std::string fieldDelimiter,
    const Schema& schema,
    std::unique_ptr<InputFormatter> inputFormatter,
    std::shared_ptr<Notifier> syncInputFormatterTaskNotifier)
    : originId(originId)
    , tupleDelimiter(std::move(tupleDelimiter))
    , fieldDelimiter(std::move(fieldDelimiter))
    , numberOfFieldsInSchema(schema.getFieldCount())
    , inputFormatter(std::move(inputFormatter))
    , sizeOfTupleInBytes(schema.getSchemaSizeInBytes())
    , syncInputFormatterTaskNotifier(std::move(syncInputFormatterTaskNotifier))
{
    /// Place dummy buffer in StagedBuffers that guarantees that the first tuple has a leading tuple delimiter
    this->stagedBuffers.emplace_back(StagedBuffer{
        .buffer = Memory::TupleBuffer{},
        .sizeOfBufferInBytes = this->tupleDelimiter.size(),
        .offsetOfLastTupleDelimiter = 0,
        .hasTupleDelimiter = true});
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
SyncInputFormatterTask::~SyncInputFormatterTask() = default;

void SyncInputFormatterTask::processLeadingSpanningTuple(
    const Memory::TupleBuffer& rawBuffer,
    const size_t offsetToFirstTupleDelimiter,
    Memory::AbstractBufferProvider& bufferProvider,
    Memory::TupleBuffer& formattedBuffer,
    const size_t offsetToFormattedBuffer)
{
    /// If the buffers are not empty, there are at least three buffers
    std::string partialTuple;
    /// 1. Process the first buffer
    const auto& firstBuffer = stagedBuffers.front();
    const auto sizeOfLeadingPartialTuple
        = firstBuffer.sizeOfBufferInBytes - (firstBuffer.offsetOfLastTupleDelimiter + tupleDelimiter.size());
    const std::string_view firstPartialTuple = std::string_view(
        firstBuffer.buffer.getBuffer<const char>() + (firstBuffer.offsetOfLastTupleDelimiter + 1), sizeOfLeadingPartialTuple);
    partialTuple.append(firstPartialTuple);
    /// 2. Process all buffers in-between the first and the last
    for (const auto& stagedBuffer : stagedBuffers | std::views::drop(1))
    {
        const std::string_view intermediatePartialTuple
            = std::string_view(stagedBuffer.buffer.getBuffer<const char>(), stagedBuffer.sizeOfBufferInBytes);
        partialTuple.append(intermediatePartialTuple);
    }

    /// 3. Process the last buffer
    const std::string_view lastPartialTuple = std::string_view(rawBuffer.getBuffer<const char>(), offsetToFirstTupleDelimiter);
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

void SyncInputFormatterTask::stop(Runtime::Execution::PipelineExecutionContext& pipelineExecutionContext)
{
    /// processPartialTuple handles empty tuple that spans over two raw buffers
    if (stagedBuffers.empty())
    {
        return;
    }
    /// Get the final buffer (size - 1 is dummy buffer that just contains tuple delimiter)
    /// Allocate formatted buffer to write formatted tuples into.
    auto formattedBuffer = pipelineExecutionContext.getBufferManager()->getBufferBlocking();

    if (stagedBuffers.size() == 1) /// there is a spanning tuple that starts and ends in the final buffer
    {
        /// If there is only one buffer, 'processSpanningTuple()' gets the trailing bytes from the buffer (which is the entire spanning tuple),
        /// then skips over the middle buffers (there are none) and reads 0 bytes from the empty tuple buffer provided in the function call.
        processLeadingSpanningTuple(Memory::TupleBuffer{}, 0, *pipelineExecutionContext.getBufferManager(), formattedBuffer, 0);
    }
    else /// there is a spanning tuple that spans over multiple buffers
    {
        /// All bytes in the final buffer are part of the spanning tuple. Thus, we safely pass it to 'processSpanningTuple' as the last buffer.
        const auto lastBuffer = std::move(stagedBuffers.back());
        stagedBuffers.pop_back();
        processLeadingSpanningTuple(
            lastBuffer.buffer, lastBuffer.offsetOfLastTupleDelimiter, *pipelineExecutionContext.getBufferManager(), formattedBuffer, 0);
    }

    /// Emit formatted buffer with single flushed tuple
    if (formattedBuffer.getNumberOfTuples() != 0)
    {
        formattedBuffer.setSequenceNumber(SequenceNumber(stagedBuffers.back().buffer.getSequenceNumber().getRawValue()));
        pipelineExecutionContext.emitBuffer(formattedBuffer, Runtime::Execution::PipelineExecutionContext::ContinuationPolicy::POSSIBLE);
    }
}

void SyncInputFormatterTask::execute(
    const Memory::TupleBuffer& rawBuffer, Runtime::Execution::PipelineExecutionContext& pipelineExecutionContext)
{
    if (rawBuffer.getBufferSize() == 0)
    {
        NES_WARNING("Received empty buffer in SyncInputFormatterTask.");
        return;
    }
    ChunkNumber::Underlying runningChunkNumber = ChunkNumber::INITIAL;
    /// Get a raw pointer to the buffer manager to avoid reference counting of shared pointer and virtual function calls to getBufferManager
    auto* bufferManager = pipelineExecutionContext.getBufferManager().get();
    const auto sizeOfFormattedTupleBufferInBytes = bufferManager->getBufferSize();

    /// Construct new FieldOffsets object
    auto fieldOffsets
        = FieldOffsetIterator(numberOfFieldsInSchema, sizeOfFormattedTupleBufferInBytes, pipelineExecutionContext.getBufferManager());

    /// Get indexes to raw buffer using the InputFormatter implementation
    FieldOffsetsType offsetOfFirstTupleDelimiter = 0;
    FieldOffsetsType offsetOfLastTupleDelimiter = 0;
    inputFormatter->indexRawBuffer(
        rawBuffer.getBuffer<char>(),
        rawBuffer.getNumberOfTuples(),
        fieldOffsets,
        tupleDelimiter,
        fieldDelimiter,
        offsetOfFirstTupleDelimiter,
        offsetOfLastTupleDelimiter);

    /// Finalize the state of the field offsets and get the final number of tuples.
    /// Determine whether raw input buffer delimits at least two tuples.
    const auto totalNumberOfTuples = fieldOffsets.finishRead();

    if (/* hasTupleDelimiter */ offsetOfLastTupleDelimiter != static_cast<FieldOffsetsType>(std::string::npos))
    {
        /// 1. process leading partial tuple if required
        auto formattedBuffer = bufferManager->getBufferBlocking();
        if (not stagedBuffers.empty())
        {
            processLeadingSpanningTuple(rawBuffer, offsetOfFirstTupleDelimiter, *bufferManager, formattedBuffer, 0);
        }
        /// Clear the staged buffers. Emplace the current buffer. Notify
        stagedBuffers.clear();
        stagedBuffers.emplace_back(StagedBuffer{
            .buffer = rawBuffer,
            .sizeOfBufferInBytes = rawBuffer.getNumberOfTuples(),
            .offsetOfLastTupleDelimiter = offsetOfLastTupleDelimiter,
            .hasTupleDelimiter = true});

        /// The below code does not require synchronization (stateless).
        /// Notify the source that it is safe to emit another raw input buffer and perform stateless computatitions.
        syncInputFormatterTaskNotifier->notifyWaiterThread();
        executeStatelessFormatting(
            rawBuffer,
            runningChunkNumber,
            fieldOffsets,
            formattedBuffer,
            *bufferManager,
            totalNumberOfTuples,
            sizeOfFormattedTupleBufferInBytes,
            pipelineExecutionContext,
            fieldDelimiter.size(),
            numberOfFieldsInSchema,
            sizeOfTupleInBytes,
            fieldSizes,
            fieldParseFunctions);
    }
    else
    {
        stagedBuffers.emplace_back(StagedBuffer{
            .buffer = std::move(rawBuffer),
            .sizeOfBufferInBytes = rawBuffer.getNumberOfTuples(),
            .offsetOfLastTupleDelimiter = 0,
            .hasTupleDelimiter = false});
        syncInputFormatterTaskNotifier->notifyWaiterThread();
    }
}
std::ostream& SyncInputFormatterTask::toString(std::ostream& os) const
{
    std::string formattedTupleDelimiter = (tupleDelimiter == "\n") ? "\\n" : tupleDelimiter;
    os << fmt::format(
        "SyncInputFormatterTask(originId: {}, tupleDelimiter: '{}', fieldDelimiter: '{}', number of fields in schema: {}, sizes of fields: "
        "{}, "
        "size of formatted tuple in bytes: {}, number of staged buffers: {})",
        originId.getRawValue(),
        formattedTupleDelimiter,
        fieldDelimiter,
        numberOfFieldsInSchema,
        fmt::join(fieldSizes, ", "),
        sizeOfTupleInBytes,
        stagedBuffers.size());
    return os;
}
}
