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
#include <Util/Notifier.hpp>
#include <ExecutablePipelineStage.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES::InputFormatters
{
/// The type that all formatters use to represent indexes to fields.
using FieldOffsetsType = uint32_t;

/// Forward referencing InputFormatter/FieldOffsets to keep it as a private implementation detail of nes-input-formatters
class InputFormatter;
class FieldOffsetIterator;

/// The SyncInputFormatterTask formats raw input buffers, synchronously resolving dependencies between raw input buffers.
/// It is tightly coupled to a Source (S1).
/// The SyncInputFormatterTask and the Source share a 'Notifier', which allows the SyncInputFormatterTask to notify the Source when there
/// are no more possible conflicts with other threads that concurrently process buffers from the same Source S1.
/// The SyncInputFormatterTask makes two assumptions:
/// A1: it receives all (potentially) raw buffers from S1 in the correct order (not only the ingestion order, but the semantically correct order).
/// A2: it is the only thread formatting buffers from S1, until it notifies S1. Thus, until it notifies S1, it can safely modify state.
class SyncInputFormatterTask final : public Runtime::Execution::ExecutablePipelineStage
{
    struct StagedBuffer
    {
        Memory::TupleBuffer buffer;
        size_t sizeOfBufferInBytes;
        uint32_t offsetOfLastTupleDelimiter;
        bool hasTupleDelimiter;
    };

public:
    using CastFunctionSignature = std::function<void(
        std::string_view inputString,
        int8_t* fieldPointer,
        Memory::AbstractBufferProvider& bufferProvider,
        Memory::TupleBuffer& tupleBufferFormatted)>;

    explicit SyncInputFormatterTask(
        OriginId originId,
        std::string tupleDelimiter,
        std::string fieldDelimiter,
        const Schema& schema,
        std::unique_ptr<InputFormatter> inputFormatter,
        std::shared_ptr<Notifier> syncInputFormatterTaskNotifier);
    ~SyncInputFormatterTask() override;

    SyncInputFormatterTask(const SyncInputFormatterTask&) = delete;
    SyncInputFormatterTask& operator=(const SyncInputFormatterTask&) = delete;
    SyncInputFormatterTask(SyncInputFormatterTask&&) = delete;
    SyncInputFormatterTask& operator=(SyncInputFormatterTask&&) = delete;

    void start(Runtime::Execution::PipelineExecutionContext&) override { /* noop */ }

    /// Attempts to flush out a final (spanning) tuple that ends in the last byte of the last seen raw buffer.
    void stop(Runtime::Execution::PipelineExecutionContext&) override;

    /// 1. Uses the concrete input formatter implementation to determine the indexes of all fields of all full tuples.
    /// 2. If the raw input buffer contains a delimiter, constructs and formats a spanning tuple, using all staged buffers.
    /// 3. Notifies source that it can put the next raw buffer into to task queue for an arbitrary worker thread to process
    /// 4. Statelessly processes the tuples in its own input buffer.
    void execute(const Memory::TupleBuffer& rawBuffer, Runtime::Execution::PipelineExecutionContext& pipelineExecutionContext) override;

    std::ostream& toString(std::ostream& os) const override;

private:
    OriginId originId;
    std::string tupleDelimiter;
    std::string fieldDelimiter;
    uint32_t numberOfFieldsInSchema;
    std::vector<size_t> fieldSizes;
    std::vector<CastFunctionSignature> fieldParseFunctions;
    std::unique_ptr<InputFormatter> inputFormatter;
    uint32_t sizeOfTupleInBytes;
    std::shared_ptr<Notifier> syncInputFormatterTaskNotifier;
    std::vector<StagedBuffer> stagedBuffers;

private:
    /// Iterates over all staged buffers and the raw input buffer.
    /// Constructs a string that contains all bytes of the spanning tuple.
    /// Formats the spanning tuple and writes the formatted data to 'formattedBuffer'.
    /// @Note assumes that no other thread can access/modify state of the SyncInputFormatterTask (see description of class)
    void processLeadingSpanningTuple(
        const Memory::TupleBuffer& rawBuffer,
        size_t offsetToFirstTupleDelimiter,
        Memory::AbstractBufferProvider& bufferProvider,
        Memory::TupleBuffer& formattedBuffer,
        size_t offsetToFormattedBuffer);
};


}
FMT_OSTREAM(NES::InputFormatters::SyncInputFormatterTask);
