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

#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Formatter.hpp>
#include <ExecutablePipelineStage.hpp>
#include <PipelineExecutionContext.hpp>


#include "Nautilus/Interface/MemoryProvider/ColumnTupleBufferMemoryProvider.hpp"
#include "Nautilus/Interface/RecordBuffer.hpp"

#include "ExecutionContext.hpp"
#include "PhysicalOperator.hpp"

namespace NES::InputFormatters
{
class RawTupleBuffer;

/// Type-erased wrapper around InputFormatterTask
class InputFormatterTaskPipeline final
{
public:
    template <typename T>
    requires(not std::same_as<std::decay_t<T>, InputFormatterTaskPipeline>)
    explicit InputFormatterTaskPipeline(T&& inputFormatterTask)
        : inputFormatterTask(std::make_unique<InputFormatterTaskModel<T>>(std::forward<T>(inputFormatterTask)))
    {
    }

    ~InputFormatterTaskPipeline() = default;

    void scan(
       ExecutionContext& executionCtx,
       Nautilus::RecordBuffer& recordBuffer,
       const PhysicalOperator& child,
       const std::vector<Record::RecordFieldIdentifier>& projections,
       size_t configuredBufferSize,
       bool isFirstOperatorAfterSource,
       const std::vector<Record::RecordFieldIdentifier>& requiredFields) const;

    /// Attempts to flush out a final (spanning) tuple that ends in the last byte of the last seen raw buffer.
    void stop(PipelineExecutionContext&) const;
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
        virtual void stopTask() = 0;
        virtual void scanTask(
            ExecutionContext& executionCtx,
            Nautilus::RecordBuffer& recordBuffer,
            const PhysicalOperator& child,
            const std::vector<Record::RecordFieldIdentifier>& projections,
            size_t configuredBufferSize,
            bool isFirstOperatorAfterSource,
            const std::vector<Record::RecordFieldIdentifier>& requiredFields)
            = 0;
        virtual std::ostream& toString(std::ostream& os) const = 0;
    };

    /// Defines the concrete behavior of the InputFormatterTaskConcept, i.e., which specific functions from T to call.
    template <typename T>
    struct InputFormatterTaskModel final : InputFormatterTaskConcept
    {
        explicit InputFormatterTaskModel(T&& inputFormatterTask) : inputFormatterTask(std::move(inputFormatterTask)) { }

        void stopTask() override { inputFormatterTask.stopTask(); }

        void scanTask(
            ExecutionContext& executionCtx,
            Nautilus::RecordBuffer& recordBuffer,
            const PhysicalOperator& child,
            const std::vector<Record::RecordFieldIdentifier>& projections,
            const size_t configuredBufferSize,
            const bool isFirstOperatorAfterSource,
            const std::vector<Record::RecordFieldIdentifier>& requiredFields) override
        {
            inputFormatterTask.scanTask(
                executionCtx, recordBuffer, child, projections, configuredBufferSize, isFirstOperatorAfterSource, requiredFields);
        }

        std::ostream& toString(std::ostream& os) const override { return inputFormatterTask.taskToString(os); }

    private:
        T inputFormatterTask;
    };

    std::unique_ptr<InputFormatterTaskConcept> inputFormatterTask;
};

}

FMT_OSTREAM(NES::InputFormatters::InputFormatterTaskPipeline);
