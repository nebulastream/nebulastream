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

#include <concepts>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <ostream>
#include <span>
#include <string_view>
#include <type_traits>
#include <utility>

#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/Formatter.hpp>
#include <Util/Logger/Logger.hpp>
#include <ExecutablePipelineStage.hpp>
#include <PipelineExecutionContext.hpp>


namespace NES::InputFormatters
{



/// Takes a tuple buffer containing raw, unformatted data and wraps it into an object that fulfills the following purposes:
/// 1. The RawTupleBuffer allows its users to operate on string_views, instead of handling raw pointers (which a would TupleBuffer require)
/// 2. It exposes only the function of the TupleBuffer that are required for formatting
/// 3. It selectively exposes the reduced set of functions, to prohibit setting, e.g., the SequenceNumber in InputFormatIndexer
/// 4. It exposes functions with more descriptive names, e.g., `getNumberOfBytes()` instead of `getNumberOfTuples`
/// 5. The type (RawTupleBuffer) makes it clear that we are dealing with raw data and not with (formatted) tuples
class RawTupleBuffer
{
    friend struct StagedBuffer;

public:
    RawTupleBuffer() = default;
    ~RawTupleBuffer() = default;
    explicit RawTupleBuffer(Memory::TupleBuffer rawTupleBuffer)
        : rawBuffer(std::move(rawTupleBuffer)), bufferView(rawBuffer.getBuffer<const char>(), rawBuffer.getNumberOfTuples()) { };

    RawTupleBuffer(RawTupleBuffer&& other) noexcept = default;
    RawTupleBuffer& operator=(RawTupleBuffer&& other) noexcept = default;
    RawTupleBuffer(const RawTupleBuffer& other) = default;
    RawTupleBuffer& operator=(const RawTupleBuffer& other) = default;

    [[nodiscard]] size_t getNumberOfBytes() const noexcept { return rawBuffer.getNumberOfTuples(); }
    [[nodiscard]] size_t getBufferSize() const noexcept { return rawBuffer.getBufferSize(); }
    [[nodiscard]] SequenceNumber getSequenceNumber() const noexcept { return rawBuffer.getSequenceNumber(); }
    [[nodiscard]] ChunkNumber getChunkNumber() const noexcept { return rawBuffer.getChunkNumber(); }
    [[nodiscard]] OriginId getOriginId() const noexcept { return rawBuffer.getOriginId(); }
    [[nodiscard]] std::string_view getBufferView() const noexcept { return bufferView; }

    [[nodiscard]] uint64_t getNumberOfTuples() const noexcept { return rawBuffer.getNumberOfTuples(); }
    void setNumberOfTuples(const uint64_t numberOfTuples) const noexcept { rawBuffer.setNumberOfTuples(numberOfTuples); }
    /// Allows to emit the underlying buffer without exposing it to the outside
    void emit(PipelineExecutionContext& pec, const PipelineExecutionContext::ContinuationPolicy continuationPolicy) const
    {
        pec.emitBuffer(rawBuffer, continuationPolicy);
    }
    [[nodiscard]] const Memory::TupleBuffer& getRawBuffer() const noexcept { return rawBuffer; }

    void setSpanningTuple(const std::string_view spanningTuple) { this->bufferView = spanningTuple; }

    Memory::TupleBuffer rawBuffer;
    std::string_view bufferView;
};


/// A staged buffer represents a raw buffer together with the locations of the first and last tuple delimiter.
/// Thus, the SequenceShredder can determine the spanning tuple(s) starting/ending in or containing the StagedBuffer.
struct StagedBuffer
{
private:
    friend class SequenceShredder;

public:
    StagedBuffer() = default;
    StagedBuffer(
        RawTupleBuffer rawTupleBuffer,
        const size_t sizeOfBufferInBytes,
        const uint32_t offsetOfFirstTupleDelimiter,
        const uint32_t offsetOfLastTupleDelimiter)
        : rawBuffer(std::move(rawTupleBuffer))
        , sizeOfBufferInBytes(sizeOfBufferInBytes)
        , offsetOfFirstTupleDelimiter(offsetOfFirstTupleDelimiter)
        , offsetOfLastTupleDelimiter(offsetOfLastTupleDelimiter) { };

    StagedBuffer(StagedBuffer&& other) noexcept = default;
    StagedBuffer& operator=(StagedBuffer&& other) noexcept = default;
    StagedBuffer(const StagedBuffer& other) = default;
    StagedBuffer& operator=(const StagedBuffer& other) = default;

    [[nodiscard]] std::string_view getBufferView() const { return rawBuffer.getBufferView(); }
    /// Returns the _first_ bytes of a staged buffer that were not processed by another thread yet.
    /// Typically, these are the bytes of a spanning tuple that _ends_ in the staged buffer.
    [[nodiscard]] std::string_view getLeadingBytes() const { return rawBuffer.getBufferView().substr(0, offsetOfFirstTupleDelimiter); }

    /// Returns the _last_ bytes of a staged buffer that were not processed by another thread yet.
    /// Typically, these are the bytes of spanning tuple that _starts_ in the staged buffer.
    [[nodiscard]] std::string_view getTrailingBytes(const size_t sizeOfTupleDelimiter) const
    {
        const auto sizeOfTrailingSpanningTuple = sizeOfBufferInBytes - (offsetOfLastTupleDelimiter + sizeOfTupleDelimiter);
        const auto startOfTrailingSpanningTuple = offsetOfLastTupleDelimiter + sizeOfTupleDelimiter;
        return rawBuffer.getBufferView().substr(startOfTrailingSpanningTuple, sizeOfTrailingSpanningTuple);
    }

    [[nodiscard]] size_t getSizeOfBufferInBytes() const { return this->sizeOfBufferInBytes; }

    [[nodiscard]] const RawTupleBuffer& getRawTupleBuffer() const { return rawBuffer; }

    [[nodiscard]] bool isValidRawBuffer() const { return rawBuffer.getRawBuffer().getBuffer() != nullptr; }

    void setSpanningTuple(const std::string_view spanningTuple) { rawBuffer.setSpanningTuple(spanningTuple); }


protected:
    RawTupleBuffer rawBuffer;
    size_t sizeOfBufferInBytes{};
    uint32_t offsetOfFirstTupleDelimiter{};
    uint32_t offsetOfLastTupleDelimiter{};
};

using FieldOffsetsType = uint32_t;

/// Restricts the IndexerMetaData that an InputFormatIndexer receives from the InputFormatterTask
template <typename T>
concept IndexerMetaDataType = requires(ParserConfig config, Schema schema, T indexerMetaData, std::ostream& spanningTuple) {
    T(config, schema);
    /// Assumes a fixed set of symbols that separate tuples
    /// InputFormatIndexers without tuple delimiters should return an empty string
    { indexerMetaData.getTupleDelimitingBytes() } -> std::same_as<std::string_view>;
};

struct SpanningTupleBuffers;

/// 'Interface' for SequenceShredder
template <typename T>
concept SequenceShredderType = requires(size_t tupleDelimiterSize, T sequenceShredder, SequenceNumber::Underlying sequenceNumber, StagedBuffer stagedBuffer) {
    T(tupleDelimiterSize);
    { sequenceShredder.template processSequenceNumber<true>(stagedBuffer, sequenceNumber) } -> std::same_as<SpanningTupleBuffers>;
    { sequenceShredder.template processSequenceNumber<false>(stagedBuffer, sequenceNumber) } -> std::same_as<SpanningTupleBuffers>;
    { sequenceShredder.isInRange(sequenceNumber) } -> std::same_as<bool>;
    { sequenceShredder.validateState() } -> std::same_as<bool>;
};


/// Forward declaring 'InputFormatterTask' to constrain the template parameter of 'InputFormatterTaskPipeline'
template <typename FormatterType, typename FieldIndexFunctionType, IndexerMetaDataType MetaData, bool HasSpanningTuple, SequenceShredderType SequenceShredderImpl>
requires(HasSpanningTuple or not FormatterType::IsFormattingRequired)
class InputFormatterTask;
template <typename T>
concept InputFormatterTaskType = requires {
    []<typename FormatterType, typename FieldIndexFunctionType, IndexerMetaDataType IndexerMetaData, bool HasSpanningTuple, SequenceShredderType SequenceShredderImpl>(
        InputFormatterTask<FormatterType, FieldIndexFunctionType, IndexerMetaData, HasSpanningTuple, SequenceShredderImpl>&) { }(std::declval<T&>());
};

/// Type-erased wrapper around InputFormatterTask
class InputFormatterTaskPipeline final : public ExecutablePipelineStage
{
public:
    template <InputFormatterTaskType T>
    requires(not std::same_as<std::decay_t<T>, InputFormatterTaskPipeline>)
    explicit InputFormatterTaskPipeline(T&& inputFormatterTask)
        : inputFormatterTask(std::make_shared<InputFormatterTaskModel<T>>(std::forward<T>(inputFormatterTask)))
    {
    }

    ~InputFormatterTaskPipeline() override = default;

    void start(PipelineExecutionContext&) override { this->inputFormatterTask->startTask(); }
    /// Attempts to flush out a final (spanning) tuple that ends in the last byte of the last seen raw buffer.
    void stop(PipelineExecutionContext&) override { this->inputFormatterTask->stopTask(); }
    /// (concurrently) executes an InputFormatterTask.
    /// First, uses the concrete input formatter implementation to determine the indexes of all fields of all full tuples.
    /// Second, uses the SequenceShredder to find spanning tuples.
    /// Third, processes (leading) spanning tuple and if it contains at least two tuple delimiters and therefore one complete tuple,
    /// process all complete tuples and trailing spanning tuple.
    void execute(const Memory::TupleBuffer& rawTupleBuffer, PipelineExecutionContext& pec) override
    {
        /// If the buffer is empty, we simply return without submitting any unnecessary work on empty buffers.
        if (rawTupleBuffer.getBufferSize() == 0)
        {
            NES_WARNING("Received empty buffer in InputFormatterTask.");
            return;
        }

        this->inputFormatterTask->executeTask(RawTupleBuffer{rawTupleBuffer}, pec);
    }

    std::ostream& toString(std::ostream& os) const override { return this->inputFormatterTask->toString(os); }

    /// Describes what a InputFormatterTask that is in the InputFormatterTaskPipeline does (interface).
    struct InputFormatterTaskConcept
    {
        virtual ~InputFormatterTaskConcept() = default;
        virtual void startTask() = 0;
        virtual void stopTask() = 0;
        virtual void executeTask(const RawTupleBuffer& rawTupleBuffer, PipelineExecutionContext& pec) = 0;
        virtual std::ostream& toString(std::ostream& os) const = 0;
    };

    /// Defines the concrete behavior of the InputFormatterTaskConcept, i.e., which specific functions from T to call.
    template <InputFormatterTaskType T>
    struct InputFormatterTaskModel final : InputFormatterTaskConcept
    {
        explicit InputFormatterTaskModel(T&& inputFormatterTask) : InputFormatterTask(std::move(inputFormatterTask)) { }
        void startTask() override { InputFormatterTask.startTask(); }
        void stopTask() override { InputFormatterTask.stopTask(); }
        void executeTask(const RawTupleBuffer& rawTupleBuffer, PipelineExecutionContext& pec) override
        {
            InputFormatterTask.executeTask(rawTupleBuffer, pec);
        }
        std::ostream& toString(std::ostream& os) const override { return InputFormatterTask.taskToString(os); }

    private:
        T InputFormatterTask;
    };

    std::shared_ptr<InputFormatterTaskConcept> inputFormatterTask;
};


}

FMT_OSTREAM(NES::InputFormatters::InputFormatterTaskPipeline);
