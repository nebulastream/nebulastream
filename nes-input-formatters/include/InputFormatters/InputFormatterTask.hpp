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

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <ostream>
#include <string>
#include <string_view>
#include <vector>
#include <API/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <ExecutablePipelineStage.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES::InputFormatters
{
/// The type that all formatters use to represent indexes to fields.
using FieldOffsetsType = uint32_t;

/// Forward referencing SequenceShredder/InputFormatter/FieldOffsets to keep it as a private implementation detail of nes-input-formatters
class SequenceShredder;
class InputFormatter;
class FieldOffsetIterator;


/// TODO #496: Implement InputFormatterTask a Nautilus Operator (CompiledExecutablePipelineStage/Special Scan)
/// -> figure out how to best call SequenceShredder and handle return values of SequenceShredder
/// -> figure out how to best process spanning tuples (currently we use a string to allocate memory)
/// -> convert FieldOffsetIterator to Nautilus data structure
/// -> implement CSVInputFormatter as Nautilus operator/function
/// (potentially make heavy use of proxy functions first)

/// InputFormatterTasks concurrently take (potentially) raw input buffers and format all full tuples in these raw input buffers that the
/// individual InputFormatterTasks see during execution.
/// The only point of synchronization is a call to the SequenceShredder data structure, which determines which buffers the InputFormatterTask
/// needs to process (resolving tuples that span multiple raw buffers).
/// An InputFormatterTask belongs to exactly one source. The source reads raw data into buffers, constructs a Task from the
/// raw buffer and its successor (the InputFormatterTask) and writes it to the task queue of the QueryEngine.
/// The QueryEngine concurrently executes InputFormatterTasks. Thus, even if the source writes the InputFormatterTasks to the task queue sequentially,
/// the QueryEngine may still execute them in any order.
class InputFormatterTask final : public NES::Runtime::Execution::ExecutablePipelineStage
{
public:
    ///
    using ParseFunctionSignature = std::function<void(
        std::string_view inputString,
        int8_t* fieldPointer,
        Memory::AbstractBufferProvider& bufferProvider,
        Memory::TupleBuffer& tupleBufferFormatted)>;

    explicit InputFormatterTask(
        OriginId originId,
        std::string tupleDelimiter,
        std::string fieldDelimiter,
        const Schema& schema,
        std::unique_ptr<InputFormatter> inputFormatter);
    ~InputFormatterTask() override;

    InputFormatterTask(const InputFormatterTask&) = delete;
    InputFormatterTask& operator=(const InputFormatterTask&) = delete;
    InputFormatterTask(InputFormatterTask&&) = delete;
    InputFormatterTask& operator=(InputFormatterTask&&) = delete;

    void start(Runtime::Execution::PipelineExecutionContext&) override { /* noop */ }

    /// Attempts to flush out a final (spanning) tuple that ends in the last byte of the last seen raw buffer.
    void stop(Runtime::Execution::PipelineExecutionContext&) override;

    /// (concurrently) executes an InputFormatterTask.
    /// First, uses the concrete input formatter implementation to determine the indexes of all fields of all full tuples.
    /// Second, uses the SequenceShredder to find partial tuples.
    /// Third, processes (leading) partial tuple and if it contains full tuples, process all full tuples and trailing partial tuple
    void
    execute(const Memory::TupleBuffer& inputTupleBuffer, Runtime::Execution::PipelineExecutionContext& pipelineExecutionContext) override;

    std::ostream& toString(std::ostream& os) const override;

private:
    OriginId originId;
    std::unique_ptr<SequenceShredder> sequenceShredder;
    std::string tupleDelimiter;
    std::string fieldDelimiter;
    uint32_t numberOfFieldsInSchema;
    std::vector<size_t> fieldSizes;
    std::vector<ParseFunctionSignature> fieldParseFunctions;
    std::unique_ptr<InputFormatter> inputFormatter;
    uint32_t sizeOfTupleInBytes;

private:
    /// Called by processRawBufferWithTupleDelimiter if the raw buffer contains at least one full tuple.
    /// Iterates over all full tuples, using the indexes in FieldOffsets and parses the tuples into formatted data.
    void processRawBuffer(
        const NES::Memory::TupleBuffer& rawBuffer,
        NES::ChunkNumber::Underlying& runningChunkNumber,
        FieldOffsetIterator& fieldOffsets,
        NES::Memory::TupleBuffer& formattedBuffer,
        Memory::AbstractBufferProvider& bufferProvider,
        size_t totalNumberOfTuplesInRawBuffer,
        size_t sizeOfFormattedTupleBufferInBytes,
        Runtime::Execution::PipelineExecutionContext& pec) const;

    /// Called by execute if the buffer delimits at least two tuples.
    /// First, processes the leading spanning tuple, if the raw buffer completed it.
    /// Second, processes the raw buffer, if it contains at least one full tuple.
    /// Third, processes the trailing spanning tuple, if the raw buffer completed it.
    void processRawBufferWithTupleDelimiter(
        const NES::Memory::TupleBuffer& rawBuffer,
        FieldOffsetsType offsetOfFirstTupleDelimiter,
        FieldOffsetsType offsetOfLastTupleDelimiter,
        SequenceNumber::Underlying sequenceNumberOfCurrentBuffer,
        Memory::AbstractBufferProvider& bufferProvider,
        size_t sizeOfFormattedTupleBufferInBytes,
        size_t totalNumberOfTuplesInRawBuffer,
        ChunkNumber::Underlying& runningChunkNumber,
        FieldOffsetIterator& fieldOffsets,
        Runtime::Execution::PipelineExecutionContext& pec) const;

    /// Called by execute, if the buffer does not delimit any tuples.
    /// Processes a spanning tuple, if the raw buffer connects two raw buffers that delimit tuples.
    void processRawBufferWithoutTupleDelimiter(
        const NES::Memory::TupleBuffer& rawBuffer,
        FieldOffsetsType offsetOfFirstTupleDelimiter,
        FieldOffsetsType offsetOfLastTupleDelimiter,
        SequenceNumber::Underlying sequenceNumberOfCurrentBuffer,
        Memory::AbstractBufferProvider& bufferProvider,
        ChunkNumber::Underlying& runningChunkNumber,
        Runtime::Execution::PipelineExecutionContext& pec) const;
};


}
FMT_OSTREAM(NES::InputFormatters::InputFormatterTask);
