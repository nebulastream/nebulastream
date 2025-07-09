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

#include <cstdint>
#include <memory>
#include <ostream>
#include <string>
#include <string_view>
#include <vector>
#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <ExecutablePipelineStage.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES::InputFormatters
{
/// The type that all formatters use to represent indexes to fields.
using FieldOffsetsType = uint32_t;

/// Forward referencing SequenceShredder/InputFormatter/FieldOffsets to keep it as a private implementation detail of nes-input-formatters
class SequenceShredder;
class InputFormatter;
class FieldOffsets;
/// Data structures of private classes (or containing types of private classes)
struct RawBufferData;
struct SpanningTuple;
struct StagedBuffer;

namespace RawInputDataParser
{
struct FieldConfig;
}

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
class InputFormatterTask final : public ExecutablePipelineStage
{
public:
    explicit InputFormatterTask(
        OriginId originId, std::unique_ptr<InputFormatter> inputFormatter, const Schema& schema, const ParserConfig& parserConfig);
    ~InputFormatterTask() override;

    InputFormatterTask(const InputFormatterTask&) = delete;
    InputFormatterTask& operator=(const InputFormatterTask&) = delete;
    InputFormatterTask(InputFormatterTask&&) = delete;
    InputFormatterTask& operator=(InputFormatterTask&&) = delete;

    void start(PipelineExecutionContext&) override { /* noop */ }

    /// Logs the final state of the SequenceShredder.
    void stop(PipelineExecutionContext&) override;

    /// (concurrently) executes an InputFormatterTask.
    /// First, uses the concrete input formatter implementation to determine the indexes of all fields of all full tuples.
    /// Second, uses the SequenceShredder to find spanning tuples.
    /// Third, processes (leading) spanning tuple and if it contains at least two tuple delimiters and therefore one complete tuple,
    /// process all complete tuples and trailing spanning tuple.
    void execute(const Memory::TupleBuffer& rawBuffer, PipelineExecutionContext& pec) override;

    std::ostream& toString(std::ostream& os) const override;

private:
    OriginId originId;
    std::unique_ptr<InputFormatter> inputFormatter;
    std::unique_ptr<SequenceShredder> sequenceShredder;
    std::vector<RawInputDataParser::FieldConfig> fieldConfigs;
    std::string tupleDelimiter;
    std::string fieldDelimiter;
    const uint32_t numberOfFieldsInSchema;
    uint32_t sizeOfTupleInBytes;

    /// Called by processRawBufferWithTupleDelimiter if the raw buffer contains at least one full tuple.
    /// Iterates over all full tuples, using the indexes in FieldOffsets and parses the tuples into formatted data.
    void processRawBuffer(
        const RawBufferData& bufferData,
        NES::ChunkNumber::Underlying& runningChunkNumber,
        FieldOffsets& fieldOffsets,
        NES::Memory::TupleBuffer& formattedBuffer,
        PipelineExecutionContext& pec) const;

    /// Called by execute if the buffer delimits at least two tuples.
    /// First, processes the leading spanning tuple, if the raw buffer completed it.
    /// Second, processes the raw buffer, if it contains at least one full tuple.
    /// Third, processes the trailing spanning tuple, if the raw buffer completed it.
    void processRawBufferWithTupleDelimiter(
        const RawBufferData& bufferData,
        ChunkNumber::Underlying& runningChunkNumber,
        FieldOffsets& fieldOffsets,
        PipelineExecutionContext& pec) const;

    /// Called by execute, if the buffer does not delimit any tuples.
    /// Processes a spanning tuple, if the raw buffer connects two raw buffers that delimit tuples.
    void processRawBufferWithoutTupleDelimiter(
        const RawBufferData& bufferData, ChunkNumber::Underlying& runningChunkNumber, PipelineExecutionContext& pec) const;

    /// Constructs a spanning tuple (string) that spans over at least two buffers (buffersToFormat).
    /// First, determines the start of the spanning tuple in the first buffer to format. Constructs a spanning tuple from the required bytes.
    /// Second, appends all bytes of all raw buffers that are not the last buffer to the spanning tuple.
    /// Third, determines the end of the spanning tuple in the last buffer to format. Appends the required bytes to the spanning tuple.
    /// Lastly, formats the full spanning tuple.
    void processSpanningTuple(
        const SpanningTuple& spanningTuple,
        const std::vector<StagedBuffer>& buffersToFormat,
        Memory::AbstractBufferProvider& bufferProvider,
        Memory::TupleBuffer& formattedBuffer) const;

    void processTuple(
        std::string_view tuple,
        const FieldOffsetsType* fieldIndexes,
        Memory::AbstractBufferProvider& bufferProvider,
        Memory::TupleBuffer& formattedBuffer) const;
};


}
FMT_OSTREAM(NES::InputFormatters::InputFormatterTask);
