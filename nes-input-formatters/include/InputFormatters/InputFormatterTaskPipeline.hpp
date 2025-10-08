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

#include <memory>
#include <ostream>
#include <utility>

#include <Nautilus/Interface/BufferRef/TupleBufferRef.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Formatter.hpp>
#include <ExecutionContext.hpp>
#include <PhysicalOperator.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES
{
class RawTupleBuffer;

/// Type-erased wrapper around InputFormatterTask
class InputFormatterTaskPipeline final : public NES::Nautilus::Interface::BufferRef::TupleBufferRef
{
public:
    template <typename T>
    requires(not std::same_as<std::decay_t<T>, InputFormatterTaskPipeline>)
    explicit InputFormatterTaskPipeline(T&& inputFormatterTask)
        : inputFormatterTask(std::make_unique<InputFormatterTaskModel<T>>(std::forward<T>(inputFormatterTask)))
    {
    }

    ~InputFormatterTaskPipeline() override = default;

    [[nodiscard]] std::shared_ptr<MemoryLayout> getMemoryLayout() const override;

    Record readRecord(
        const std::vector<Record::RecordFieldIdentifier>& projections,
        const RecordBuffer& recordBuffer,
        nautilus::val<uint64_t>& recordIndex) const override;

    void writeRecord(
        nautilus::val<uint64_t>&,
        const RecordBuffer&,
        const Record&,
        const nautilus::val<AbstractBufferProvider*>&) const override
    {
        INVARIANT(false, "unsupported operation on InputFormatterBufferRef");
    }

    void open(RecordBuffer& recordBuffer, ArenaRef& arenaRef) override;
    // nautilus::val<uint64_t> getNumberOfRecords(const RecordBuffer& recordBuffer) const override;

    // OpenReturnState scan(ExecutionContext& executionCtx, Nautilus::RecordBuffer& recordBuffer, const PhysicalOperator& child) const;

    /// Attempts to flush out a final (spanning) tuple that ends in the last byte of the last seen raw buffer.
    void close(PipelineExecutionContext&) const;
    /// (concurrently) executes an InputFormatterTask.
    /// First, uses the concrete InputFormatIndexer implementation to determine the indexes of all fields of all full tuples.
    /// Second, uses the SequenceShredder to find spanning tuples.
    /// Third, processes (leading) spanning tuple and if it contains at least two tuple delimiters and therefore one complete tuple,
    /// process all complete tuples and trailing spanning tuple.

    std::ostream& toString(std::ostream& os) const;

    /// Describes what a InputFormatterTask that is in the InputFormatterTaskPipeline does (interface).
    struct InputFormatterTaskConcept
    {
        virtual ~InputFormatterTaskConcept() = default;
        virtual void close() = 0;
        virtual Record readRecord(
            const std::vector<Record::RecordFieldIdentifier>& projections,
            const RecordBuffer& recordBuffer,
            nautilus::val<uint64_t>& recordIndex) const
            = 0;
        virtual void open(RecordBuffer&, ArenaRef&) = 0;
        virtual std::shared_ptr<MemoryLayout> getMemoryLayout() const = 0;
        // virtual nautilus::val<uint64_t> getNumberOfRecords(const RecordBuffer& recordBuffer) const = 0;
        virtual std::ostream& toString(std::ostream& os) const = 0;
    };

    /// Defines the concrete behavior of the InputFormatterTaskConcept, i.e., which specific functions from T to call.
    template <typename T>
    struct InputFormatterTaskModel final : InputFormatterTaskConcept
    {
        explicit InputFormatterTaskModel(T&& inputFormatterTask) : inputFormatterTask(std::move(inputFormatterTask)) { }

        // Todo: what to do with 'stopTask'? <-- close?
        void close() override { inputFormatterTask.close(); }

        void open(RecordBuffer& recordBuffer, ArenaRef& arena) override { inputFormatterTask.open(recordBuffer, arena); }
        // nautilus::val<uint64_t> getNumberOfRecords(const RecordBuffer& recordBuffer) const override{ return inputFormatterTask.getNumberOfRecords(recordBuffer); };

        std::ostream& toString(std::ostream& os) const override { return inputFormatterTask.taskToString(os); }

        Record readRecord(
            const std::vector<Record::RecordFieldIdentifier>& projections,
            const RecordBuffer& recordBuffer,
            nautilus::val<uint64_t>& recordIndex) const override
        {
            return inputFormatterTask.readRecord(projections,recordBuffer, recordIndex);
        }

        [[nodiscard]] std::shared_ptr<MemoryLayout> getMemoryLayout() const override { return inputFormatterTask.getMemoryLayout(); }

    private:
        T inputFormatterTask;
    };

    std::unique_ptr<InputFormatterTaskConcept> inputFormatterTask;
};

}

FMT_OSTREAM(NES::InputFormatterTaskPipeline);
