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
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <Concepts.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>
#include <UncompiledFastParse.hpp>
#include <UncompiledFieldIndexFunction.hpp>
#include <UncompiledParserProvider.hpp>
#include <UncompiledRawTupleBuffer.hpp>
#include <UncompiledSequenceShredder.hpp>

namespace NES
{
/// The type that all formatters use to represent indexes to fields.
using UncompiledFieldIndex = uint32_t;

inline void setUncompiledMetadataOfFormattedBuffer(
    const TupleBuffer& rawBuffer, TupleBuffer& formattedBuffer, ChunkNumber::Underlying& runningChunkNumber, const bool isLastChunk)
{
    formattedBuffer.setLastChunk(isLastChunk);
    formattedBuffer.setSequenceNumber(rawBuffer.getSequenceNumber());
    formattedBuffer.setChunkNumber(ChunkNumber(runningChunkNumber++));
    formattedBuffer.setOriginId(rawBuffer.getOriginId());
    /// Propagate the source read-start stamp like the compiled scan/emit pair does (ScanPhysicalOperator ->
    /// ctx.sourceCreationTimestamp -> EmitPhysicalOperator); pooled buffers otherwise carry 0/stale stamps and
    /// latency-metering sinks (MonitoringSink) compute garbage latencies and a zero wall-span start.
    formattedBuffer.setSourceCreationTimestampInMS(rawBuffer.getSourceCreationTimestampInMS());
}

/// Given that we know the number of tuples in a raw buffer, the number of (spanning) tuples that we already wrote into our current formatted
/// buffer and the max number of tuples that fit into a formatted buffer (given a schema), we can precisely calculate how many formatted buffers
/// we need to store all tuples from the raw buffer.
inline size_t calculateUncompiledNumberOfRequiredFormattedBuffers(
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
template <typename UncompiledFieldIndexFunctionType>
void processUncompiledTuple(
    const std::string_view tupleView,
    UncompiledFieldIndexFunction<UncompiledFieldIndexFunctionType>& fieldIndexFunction,
    const size_t numTuplesReadFromRawBuffer,
    TupleBuffer& formattedBuffer,
    const UncompiledSchemaInfo& schemaInfo,
    const std::vector<UncompiledParseFunctionSignature>& parseFunctions,
    AbstractBufferProvider& bufferProvider /// for getting unpooled buffers for varsized data
)
{
    const size_t currentTupleIdx = formattedBuffer.getNumberOfTuples();
    const size_t offsetOfCurrentTupleInBytes = currentTupleIdx * schemaInfo.getSizeOfTupleInBytes();
    size_t offsetOfCurrentFieldInBytes = 0;

    /// Currently, we still allow only row-wise writing to the formatted buffer
    /// This will will change with #496, which implements the UncompiledInputFormatterTask in Nautilus
    /// The UncompiledInputFormatterTask then becomes part of a pipeline with a scan/emit phase and has access to the BufferRef
    for (size_t fieldIndex = 0; fieldIndex < schemaInfo.getFieldSizesInBytes().size(); ++fieldIndex)
    {
        /// Get the current field, parse it, and write it to the correct position in the formatted buffer
        const auto currentFieldSV = fieldIndexFunction.readFieldAt(tupleView, numTuplesReadFromRawBuffer, fieldIndex);
        const auto writeOffsetInBytes = offsetOfCurrentTupleInBytes + offsetOfCurrentFieldInBytes;
        /// Hardcoded fast parsers (fast_float) for the fixed-size numeric types, dispatched DIRECTLY -- no
        /// per-field std::function/registry indirection on the hot loop. Non-numeric types (BOOL/CHAR/VARSIZED)
        /// keep the registry parse function (rare, and VARSIZED needs the child-buffer machinery).
        switch (schemaInfo.getFieldTypes()[fieldIndex])
        {
            case DataType::Type::INT8:
                uncompiledWriteFixed(uncompiledFastParse<int8_t>(currentFieldSV), writeOffsetInBytes, formattedBuffer);
                break;
            case DataType::Type::INT16:
                uncompiledWriteFixed(uncompiledFastParse<int16_t>(currentFieldSV), writeOffsetInBytes, formattedBuffer);
                break;
            case DataType::Type::INT32:
                uncompiledWriteFixed(uncompiledFastParse<int32_t>(currentFieldSV), writeOffsetInBytes, formattedBuffer);
                break;
            case DataType::Type::INT64:
                uncompiledWriteFixed(uncompiledFastParse<int64_t>(currentFieldSV), writeOffsetInBytes, formattedBuffer);
                break;
            case DataType::Type::UINT8:
                uncompiledWriteFixed(uncompiledFastParse<uint8_t>(currentFieldSV), writeOffsetInBytes, formattedBuffer);
                break;
            case DataType::Type::UINT16:
                uncompiledWriteFixed(uncompiledFastParse<uint16_t>(currentFieldSV), writeOffsetInBytes, formattedBuffer);
                break;
            case DataType::Type::UINT32:
                uncompiledWriteFixed(uncompiledFastParse<uint32_t>(currentFieldSV), writeOffsetInBytes, formattedBuffer);
                break;
            case DataType::Type::UINT64:
                uncompiledWriteFixed(uncompiledFastParse<uint64_t>(currentFieldSV), writeOffsetInBytes, formattedBuffer);
                break;
            case DataType::Type::FLOAT32:
                uncompiledWriteFixed(uncompiledFastParse<float>(currentFieldSV), writeOffsetInBytes, formattedBuffer);
                break;
            case DataType::Type::FLOAT64:
                uncompiledWriteFixed(uncompiledFastParse<double>(currentFieldSV), writeOffsetInBytes, formattedBuffer);
                break;
            default:
                parseFunctions[fieldIndex](currentFieldSV, writeOffsetInBytes, bufferProvider, formattedBuffer);
                break;
        }

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
template <UncompiledInputFormatIndexerType FormatterType>
void processUncompiledSpanningTuple(
    const std::span<const UncompiledStagedBuffer> stagedBuffersSpan,
    AbstractBufferProvider& bufferProvider,
    TupleBuffer& formattedBuffer,
    const UncompiledSchemaInfo& schemaInfo,
    const typename FormatterType::UncompiledIndexerMetaData& uncompiledIndexerMetaData,
    const FormatterType& inputFormatIndexer,
    const std::vector<UncompiledParseFunctionSignature>& parseFunctions)
{
    INVARIANT(stagedBuffersSpan.size() >= 2, "A spanning tuple must span across at least two buffers");
    /// If the buffers are not empty, there are at least three buffers
    std::stringstream spanningTupleStringStream;
    spanningTupleStringStream << uncompiledIndexerMetaData.getTupleDelimitingBytes();

    const auto& firstBuffer = stagedBuffersSpan.front();
    const auto firstSpanningTuple = firstBuffer.getTrailingBytes(uncompiledIndexerMetaData.getTupleDelimitingBytes().size());
    spanningTupleStringStream << firstSpanningTuple;

    /// Process all buffers in-between the first and the last
    for (const auto middleBuffers = stagedBuffersSpan | std::views::drop(1) | std::views::take(stagedBuffersSpan.size() - 2);
         const auto& buffer : middleBuffers)
    {
        spanningTupleStringStream << buffer.getBufferView();
    }

    auto lastBuffer = stagedBuffersSpan.back();
    spanningTupleStringStream << lastBuffer.getLeadingBytes();
    spanningTupleStringStream << uncompiledIndexerMetaData.getTupleDelimitingBytes();

    const std::string completeSpanningTuple(spanningTupleStringStream.str());
    const auto sizeOfLeadingAndTrailingTupleDelimiter = 2 * uncompiledIndexerMetaData.getTupleDelimitingBytes().size();
    if (completeSpanningTuple.size() > sizeOfLeadingAndTrailingTupleDelimiter)
    {
        auto fieldIndexFunction = typename FormatterType::UncompiledFieldIndexFunctionType();
        lastBuffer.setSpanningTuple(completeSpanningTuple);
        inputFormatIndexer.indexRawBuffer(fieldIndexFunction, lastBuffer.getRawTupleBuffer(), uncompiledIndexerMetaData);
        processUncompiledTuple<typename FormatterType::UncompiledFieldIndexFunctionType>(
            completeSpanningTuple, fieldIndexFunction, 0, formattedBuffer, schemaInfo, parseFunctions, bufferProvider);
        formattedBuffer.setNumberOfTuples(formattedBuffer.getNumberOfTuples() + 1);
    }
}

/// Maps a DataType to the corresponding parser name from the InputFormatterDescriptor.
/// For VARSIZED, uses a hardcoded default since the descriptor does not (yet) have a VARSIZED parser config.
inline std::string getUncompiledParserName(const DataType::Type type, const InputFormatterDescriptor& config)
{
    switch (type)
    {
        case DataType::Type::BOOLEAN:
            return config.getFromConfig(InputFormatterDescriptor::BOOL_PARSER);
        case DataType::Type::CHAR:
            return config.getFromConfig(InputFormatterDescriptor::CHAR_PARSER);
        case DataType::Type::INT8:
            return config.getFromConfig(InputFormatterDescriptor::INT8_PARSER);
        case DataType::Type::INT16:
            return config.getFromConfig(InputFormatterDescriptor::INT16_PARSER);
        case DataType::Type::INT32:
            return config.getFromConfig(InputFormatterDescriptor::INT32_PARSER);
        case DataType::Type::INT64:
            return config.getFromConfig(InputFormatterDescriptor::INT64_PARSER);
        case DataType::Type::UINT8:
            return config.getFromConfig(InputFormatterDescriptor::UINT8_PARSER);
        case DataType::Type::UINT16:
            return config.getFromConfig(InputFormatterDescriptor::UINT16_PARSER);
        case DataType::Type::UINT32:
            return config.getFromConfig(InputFormatterDescriptor::UINT32_PARSER);
        case DataType::Type::UINT64:
            return config.getFromConfig(InputFormatterDescriptor::UINT64_PARSER);
        case DataType::Type::FLOAT32:
            return config.getFromConfig(InputFormatterDescriptor::F32_PARSER);
        case DataType::Type::FLOAT64:
            return config.getFromConfig(InputFormatterDescriptor::F64_PARSER);
        case DataType::Type::VARSIZED:
            return "DefaultVARSIZED";
        case DataType::Type::UNDEFINED:
            throw NotImplemented("Cannot parse undefined type.");
    }
    std::unreachable();
}

/// InputFormatterTasks concurrently take (potentially) raw input buffers and format all full tuples in these raw input buffers that the
/// individual InputFormatterTasks see during execution.
/// The only point of synchronization is a call to the UncompiledSequenceShredder data structure, which determines which buffers the UncompiledInputFormatterTask
/// needs to process (resolving tuples that span multiple raw buffers).
/// An UncompiledInputFormatterTask belongs to exactly one source. The source reads raw data into buffers, constructs a Task from the
/// raw buffer and its successor (the UncompiledInputFormatterTask) and writes it to the task queue of the QueryEngine.
/// The QueryEngine concurrently executes InputFormatterTasks. Thus, even if the source writes the InputFormatterTasks to the task queue sequentially,
/// the QueryEngine may still execute them in any order.
template <UncompiledInputFormatIndexerType FormatterType>
class UncompiledInputFormatterTask
{
public:
    static constexpr bool hasSpanningTuple() { return FormatterType::HasSpanningTuple; }

    static constexpr bool isSequential() { return FormatterType::IsSequential; }

    explicit UncompiledInputFormatterTask(
        FormatterType inputFormatIndexer,
        const Schema& schema,
        const UncompiledQuotationType quotationType,
        const InputFormatterDescriptor& parserConfig)

        : inputFormatIndexer(std::move(inputFormatIndexer))
        , schemaInfo(schema)
        , uncompiledIndexerMetaData(typename FormatterType::UncompiledIndexerMetaData{parserConfig, schema})
        /// Since we know the schema, we can create a vector that contains a function that converts the string representation of a field value
        /// to our internal representation in the correct order. During parsing, we iterate over the fields in each tuple, and, using the current
        /// field number, load the correct function for parsing from the vector.
        , parseFunctions(
              schema.getFields()
              | std::views::transform(
                  [&parserConfig, quotationType](const auto& field)
                  { return provideUncompiledParseFunction(getUncompiledParserName(field.dataType.type, parserConfig), quotationType); })
              | std::ranges::to<std::vector>())
    {
        if constexpr (hasSpanningTuple() and not isSequential())
        {
            sequenceShredder = std::make_unique<UncompiledSequenceShredder>(uncompiledIndexerMetaData.getTupleDelimitingBytes().size());
        }
    }

    ~UncompiledInputFormatterTask() = default;

    UncompiledInputFormatterTask(const UncompiledInputFormatterTask&) = delete;
    UncompiledInputFormatterTask& operator=(const UncompiledInputFormatterTask&) = delete;
    UncompiledInputFormatterTask(UncompiledInputFormatterTask&&) = default;
    UncompiledInputFormatterTask& operator=(UncompiledInputFormatterTask&&) = delete;

    void startTask() { /* noop */ }

    void stopTask() const
    {
        if constexpr (isSequential())
        {
            INVARIANT(
                accumulatedSpanningTupleBuffers.empty(), "Sequential input formatter should have no accumulated buffers when stopping.");
        }
        else if constexpr (hasSpanningTuple())
        {
            INVARIANT(sequenceShredder != nullptr, "The UncompiledSequenceShredder handles spanning tuples, thus it must not be null.");
        }
    }

    // ========== Sequential (single-threaded) execution path ==========
    void executeTask(const UncompiledRawTupleBuffer& rawBuffer, PipelineExecutionContext& pec)
    requires(FormatterType::IsFormattingRequired and isSequential())
    {
        auto fieldIndexFunction = typename FormatterType::UncompiledFieldIndexFunctionType();
        inputFormatIndexer.indexRawBuffer(fieldIndexFunction, rawBuffer, uncompiledIndexerMetaData);

        ChunkNumber::Underlying runningChunkNumber = ChunkNumber::INITIAL;

        if (fieldIndexFunction.getOffsetOfFirstTupleDelimiter() < rawBuffer.getBufferSize())
        {
            /// Buffer has at least one delimiter — complete the leading row, parse the bulk, carry the tail
            processSequentialBufferWithDelimiter(rawBuffer, runningChunkNumber, fieldIndexFunction, pec);
        }
        else
        {
            /// Buffer has no delimiter — the whole buffer is interior of a multi-buffer spanning tuple; carry it
            /// (zero-copy, ref-counted) for the next buffer that completes the row.
            accumulatedSpanningTupleBuffers.push_back({rawBuffer.getRawBuffer(), 0, rawBuffer.getNumberOfBytes()});
        }
    }

    /// Not supported (yet):
    /// requires(FormatterType::IsFormattingRequired and not(HasSpanningTuple))
    ///     - non-native formats without spanning tuples, since it is hard to guarantee that (mostly) text based data does not span buffers
    /// requires(not(FormatterType::IsFormattingRequired) and HasSpanningTuple)
    ///     - native format with spanning tuples, since it is hard to guarantee that we always read in full buffers and if we don't we need
    ///       a mechanism to determine the offsets of the fields in the individual buffers asynchronously
    void executeTask(const UncompiledRawTupleBuffer& rawBuffer, PipelineExecutionContext& pec)
    requires(not(FormatterType::IsFormattingRequired) and not(hasSpanningTuple()))
    {
        /// If the format has fixed size tuple and no spanning tuples (give the assumption that tuples are aligned with the start of the buffers)
        /// the UncompiledInputFormatterTask does not need to do anything.
        /// @Note: with a Nautilus implementation, we can skip the proxy function call that triggers formatting/indexing during tracing,
        /// leading to generated code that immediately operates on the data.
        const auto [div, mod] = std::lldiv(static_cast<int64_t>(rawBuffer.getNumberOfTuples()), this->schemaInfo.getSizeOfTupleInBytes());
        PRECONDITION(
            mod == 0,
            "Raw buffer contained {} bytes, which is not a multiple of the tuple size {} bytes.",
            rawBuffer.getNumberOfBytes(),
            this->schemaInfo.getSizeOfTupleInBytes());
        /// @Note: We assume that '.getNumberOfBytes()' ALWAYS returns the number of bytes at this point (set by source)
        const auto numberOfTuplesInFormattedBuffer = rawBuffer.getNumberOfBytes() / this->schemaInfo.getSizeOfTupleInBytes();
        rawBuffer.setNumberOfTuples(numberOfTuplesInFormattedBuffer);
        /// The 'rawBuffer' is already formatted, so we can use it without any formatting.
        rawBuffer.emit(pec, PipelineExecutionContext::ContinuationPolicy::POSSIBLE);
    }

    void executeTask(const UncompiledRawTupleBuffer& rawBuffer, PipelineExecutionContext& pec)
    requires(FormatterType::IsFormattingRequired and hasSpanningTuple() and not isSequential())
    {
        /// Get field delimiter indices of the raw buffer by using the UncompiledInputFormatIndexer implementation
        auto fieldIndexFunction = typename FormatterType::UncompiledFieldIndexFunctionType();
        inputFormatIndexer.indexRawBuffer(fieldIndexFunction, rawBuffer, uncompiledIndexerMetaData);

        /// If the offset of the _first_ tuple delimiter is not within the rawBuffer, the UncompiledInputFormatIndexer did not find any tuple delimiter
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
        os << "UncompiledInputFormatterTask(inputFormatIndexer: " << inputFormatIndexer;
        if constexpr (hasSpanningTuple() and not isSequential())
        {
            os << ", sequenceShredder: " << *sequenceShredder;
        }
        os << ")\n";
        return os;
    }

private:
    FormatterType inputFormatIndexer;
    UncompiledSchemaInfo schemaInfo;
    typename FormatterType::UncompiledIndexerMetaData uncompiledIndexerMetaData;

    struct Empty
    {
    };

    /// Concurrent mode only: lock-free spanning tuple resolution across threads
    [[no_unique_address]] std::conditional_t<hasSpanningTuple() and not isSequential(), std::unique_ptr<UncompiledSequenceShredder>, Empty>
        sequenceShredder;

    std::vector<UncompiledParseFunctionSignature> parseFunctions;

    /// Sequential mode only: the region of a buffer that is part of a spanning tuple straddling buffers, kept
    /// alive (TupleBuffer is ref-counted) with ZERO copy until the next buffer completes the tuple.
    /// `offset..offset+length` is the carried region: a trailing partial uses offset = lastDelimiter +
    /// delimiterSize; a whole delimiter-less interior buffer uses offset 0. Mirrors the compiled SequentialCarry.
    struct SequentialCarry
    {
        TupleBuffer buffer;
        size_t offset;
        size_t length;
    };

    [[no_unique_address]] std::conditional_t<isSequential(), std::vector<SequentialCarry>, Empty> accumulatedSpanningTupleBuffers;

    /// Called by processRawBufferWithTupleDelimiter if the raw buffer contains at least one full tuple.
    /// Iterates over all full tuples using hasNext() and parses the tuples into formatted data.
    void parseRawBuffer(
        const UncompiledRawTupleBuffer& rawBuffer,
        ChunkNumber::Underlying& runningChunkNumber,
        UncompiledFieldIndexFunction<typename FormatterType::UncompiledFieldIndexFunctionType>& fieldIndexFunction,
        TupleBuffer& formattedBuffer,
        PipelineExecutionContext& pec,
        const size_t startTupleIdx = 0) const
    {
        const auto bufferProvider = pec.getBufferManager();
        const size_t numberOfTuplesPerBuffer = bufferProvider->getBufferSize() / this->schemaInfo.getSizeOfTupleInBytes();
        PRECONDITION(numberOfTuplesPerBuffer != 0, "The capacity of a buffer must suffice to hold at least one tuple.");
        size_t numTuplesReadFromRawBuffer = startTupleIdx;

        while (fieldIndexFunction.hasNext(numTuplesReadFromRawBuffer))
        {
            /// If the current formatted buffer is full, emit it and get a new one
            if (formattedBuffer.getNumberOfTuples() >= numberOfTuplesPerBuffer)
            {
                setUncompiledMetadataOfFormattedBuffer(rawBuffer.getRawBuffer(), formattedBuffer, runningChunkNumber, false);
                pec.emitBuffer(formattedBuffer, PipelineExecutionContext::ContinuationPolicy::POSSIBLE);
                formattedBuffer = bufferProvider->getBufferBlocking();
            }

            processUncompiledTuple<typename FormatterType::UncompiledFieldIndexFunctionType>(
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
    }

    /// Called by execute if the buffer delimits at least two tuples.
    /// First, processes the leading spanning tuple, if the raw buffer completed it.
    /// Second, processes the raw buffer, if it contains at least one full tuple.
    /// Third, processes the trailing spanning tuple, if the raw buffer completed it.
    void processRawBufferWithTupleDelimiter(
        const UncompiledRawTupleBuffer& rawBuffer,
        ChunkNumber::Underlying& runningChunkNumber,
        UncompiledFieldIndexFunction<typename FormatterType::UncompiledFieldIndexFunctionType>& fieldIndexFunction,
        PipelineExecutionContext& pec) const
    {
        const auto bufferProvider = pec.getBufferManager();
        auto [isInRange, leadingSTBuffers] = sequenceShredder->findLeadingSTWithDelimiter(UncompiledStagedBuffer{
            rawBuffer, fieldIndexFunction.getOffsetOfFirstTupleDelimiter(), fieldIndexFunction.getOffsetOfLastTupleDelimiter()});
        if (not isInRange)
        {
            NES_WARNING("Sequence Number {} was out of range", rawBuffer.getSequenceNumber().getRawValue());
            rawBuffer.repeat(pec);
            return;
        }

        /// 1. process leading spanning tuple if required
        auto formattedBuffer = bufferProvider->getBufferBlocking();
        if (/* hasLeadingSpanningTuple */ leadingSTBuffers.hasSpanningTuple())
        {
            processUncompiledSpanningTuple<FormatterType>(
                leadingSTBuffers.getSpanningBuffers(),
                *bufferProvider,
                formattedBuffer,
                this->schemaInfo,
                this->uncompiledIndexerMetaData,
                this->inputFormatIndexer,
                this->parseFunctions);
        }

        /// 2. process tuples in buffer — iterate for as long as the field index function indicates more tuples
        if (fieldIndexFunction.hasNext(0))
        {
            parseRawBuffer(rawBuffer, runningChunkNumber, fieldIndexFunction, formattedBuffer, pec);
        }

        /// 3. process trailing spanning tuple if required
        if (const auto trailingSTBuffers = sequenceShredder->findTrailingSTWithDelimiter(rawBuffer.getSequenceNumber());
            trailingSTBuffers.hasSpanningTuple())
        {
            const auto numBytesInFormattedBuffer = formattedBuffer.getNumberOfTuples() * this->schemaInfo.getSizeOfTupleInBytes();
            if (formattedBuffer.getBufferSize() - numBytesInFormattedBuffer < this->schemaInfo.getSizeOfTupleInBytes())
            {
                setUncompiledMetadataOfFormattedBuffer(rawBuffer.getRawBuffer(), formattedBuffer, runningChunkNumber, false);
                pec.emitBuffer(formattedBuffer, PipelineExecutionContext::ContinuationPolicy::POSSIBLE);
                formattedBuffer = bufferProvider->getBufferBlocking();
            }

            processUncompiledSpanningTuple<FormatterType>(
                trailingSTBuffers.getSpanningBuffers(),
                *bufferProvider,
                formattedBuffer,
                this->schemaInfo,
                this->uncompiledIndexerMetaData,
                this->inputFormatIndexer,
                this->parseFunctions);
        }
        /// If a raw buffer contains exactly one delimiter, but does not complete a spanning tuple, the formatted buffer does not contain a tuple
        if (formattedBuffer.getNumberOfTuples() != 0)
        {
            setUncompiledMetadataOfFormattedBuffer(rawBuffer.getRawBuffer(), formattedBuffer, runningChunkNumber, true);
            pec.emitBuffer(formattedBuffer, PipelineExecutionContext::ContinuationPolicy::POSSIBLE);
        }
    }

    /// Called by execute, if the buffer does not delimit any tuples.
    /// Processes a spanning tuple, if the raw buffer connects two raw buffers that delimit tuples.
    void processRawBufferWithoutTupleDelimiter(
        const UncompiledRawTupleBuffer& rawBuffer,
        ChunkNumber::Underlying& runningChunkNumber,
        UncompiledFieldIndexFunction<typename FormatterType::UncompiledFieldIndexFunctionType>& fieldIndexFunction,
        PipelineExecutionContext& pec) const
    {
        const auto bufferProvider = pec.getBufferManager();
        auto [isInRange, stagedBuffers] = sequenceShredder->findSTWithoutDelimiter(UncompiledStagedBuffer{
            rawBuffer, fieldIndexFunction.getOffsetOfFirstTupleDelimiter(), fieldIndexFunction.getOffsetOfLastTupleDelimiter()});
        if (not isInRange)
        {
            rawBuffer.repeat(pec);
            return;
        }

        if (stagedBuffers.getSize() < 3)
        {
            return;
        }
        /// If there is a spanning tuple, get a new buffer for formatted data and process the spanning tuples
        auto formattedBuffer = bufferProvider->getBufferBlocking();
        processUncompiledSpanningTuple<FormatterType>(
            stagedBuffers.getSpanningBuffers(),
            *bufferProvider,
            formattedBuffer,
            this->schemaInfo,
            this->uncompiledIndexerMetaData,
            this->inputFormatIndexer,
            this->parseFunctions);

        formattedBuffer.setSequenceNumber(rawBuffer.getSequenceNumber());
        formattedBuffer.setChunkNumber(ChunkNumber(runningChunkNumber++));
        formattedBuffer.setOriginId(rawBuffer.getOriginId());
        formattedBuffer.setSourceCreationTimestampInMS(rawBuffer.getRawBuffer().getSourceCreationTimestampInMS());
        pec.emitBuffer(formattedBuffer, PipelineExecutionContext::ContinuationPolicy::POSSIBLE);
    }

private:
    /// Sequential path: buffer has at least one delimiter. Mirrors the compiled SequentialCSV (indexRawBuffer
    /// uses the PARALLEL band, b0 at the first newline): the leading row [0 -> firstDelim] is NOT in the band --
    /// complete it from the carry accumulator and parse it as one leading record; parse the bulk between the
    /// first and last delimiter in place; carry the trailing partial after the last delimiter to the next buffer.
    void processSequentialBufferWithDelimiter(
        const UncompiledRawTupleBuffer& rawBuffer,
        ChunkNumber::Underlying& runningChunkNumber,
        UncompiledFieldIndexFunction<typename FormatterType::UncompiledFieldIndexFunctionType>& fieldIndexFunction,
        PipelineExecutionContext& pec)
    {
        const auto bufferProvider = pec.getBufferManager();
        auto formattedBuffer = bufferProvider->getBufferBlocking();

        /// 1. Leading record: [0 -> firstDelim] completed with the carried tail(s). Fires for a spanning row
        /// (carry non-empty) OR a standalone first-of-buffer row (firstDelim > 0). Skipped only when the buffer
        /// starts with a delimiter and there is no carry (nothing to make a row from).
        const auto offsetOfFirstDelimiter = fieldIndexFunction.getOffsetOfFirstTupleDelimiter();
        if (!accumulatedSpanningTupleBuffers.empty() || offsetOfFirstDelimiter > 0)
        {
            const auto leadingBytes = rawBuffer.getBufferView().substr(0, offsetOfFirstDelimiter);
            processSequentialLeadingRecord(rawBuffer, leadingBytes, formattedBuffer, *bufferProvider);
        }

        /// 2. Bulk: the complete tuples the band found (between the first and last delimiter).
        if (fieldIndexFunction.hasNext(0))
        {
            parseRawBuffer(rawBuffer, runningChunkNumber, fieldIndexFunction, formattedBuffer, pec, 0);
        }

        /// 3. Carry THIS buffer's trailing partial (bytes after the last delimiter) for the next buffer, keeping
        /// the buffer alive (ref-counted), ZERO copy. Replaces the old runner-prepend (setNumberOfTuples) contract.
        const auto delimiterSize = uncompiledIndexerMetaData.getTupleDelimitingBytes().size();
        const auto tailStart = static_cast<size_t>(fieldIndexFunction.getOffsetOfLastTupleDelimiter()) + delimiterSize;
        const auto totalBytes = rawBuffer.getNumberOfBytes();
        if (tailStart < totalBytes)
        {
            accumulatedSpanningTupleBuffers.push_back({rawBuffer.getRawBuffer(), tailStart, totalBytes - tailStart});
        }

        /// Emit the formatted buffer if it has tuples
        if (formattedBuffer.getNumberOfTuples() != 0)
        {
            setUncompiledMetadataOfFormattedBuffer(rawBuffer.getRawBuffer(), formattedBuffer, runningChunkNumber, true);
            pec.emitBuffer(formattedBuffer, PipelineExecutionContext::ContinuationPolicy::POSSIBLE);
        }
    }

    /// Sequential path: completes the leading row of a buffer -- assembles `delim + carried_tail(s) +
    /// leadingBytes + delim`, indexes it, and parses it as one record. Handles both a spanning row (carry
    /// non-empty) and a standalone first-of-buffer row (carry empty, leadingBytes = the whole first row).
    void processSequentialLeadingRecord(
        const UncompiledRawTupleBuffer& rawBuffer,
        const std::string_view leadingBytes,
        TupleBuffer& formattedBuffer,
        AbstractBufferProvider& bufferProvider)
    {
        const auto delimiterBytes = uncompiledIndexerMetaData.getTupleDelimitingBytes();

        /// Build the leading spanning tuple: delimiter + carried_tail(s) + leading_bytes + delimiter
        std::stringstream spanningTupleStringStream;
        spanningTupleStringStream << delimiterBytes;
        for (const auto& carry : accumulatedSpanningTupleBuffers)
        {
            spanningTupleStringStream << std::string_view(
                carry.buffer.template getAvailableMemoryArea<char>().data() + carry.offset, carry.length);
        }
        spanningTupleStringStream << leadingBytes;
        spanningTupleStringStream << delimiterBytes;

        const std::string completeSpanningTuple(spanningTupleStringStream.str());

        /// Release the carried buffers back to the pool
        accumulatedSpanningTupleBuffers.clear();

        /// Parse only if there is an actual row between the two delimiters
        const auto sizeOfTwoDelimiters = 2 * delimiterBytes.size();
        if (completeSpanningTuple.size() > sizeOfTwoDelimiters)
        {
            auto tempFIF = typename FormatterType::UncompiledFieldIndexFunctionType();
            auto tempRawBuffer = rawBuffer; /// shallow copy (TupleBuffer is ref-counted); only its view is used
            tempRawBuffer.setSpanningTuple(completeSpanningTuple);
            inputFormatIndexer.indexRawBuffer(tempFIF, tempRawBuffer, uncompiledIndexerMetaData);

            processUncompiledTuple<typename FormatterType::UncompiledFieldIndexFunctionType>(
                completeSpanningTuple, tempFIF, 0, formattedBuffer, this->schemaInfo, this->parseFunctions, bufferProvider);
            formattedBuffer.setNumberOfTuples(formattedBuffer.getNumberOfTuples() + 1);
        }
    }
};

}
