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
#include <cstdint>
#include <memory>
#include <ostream>
#include <type_traits>

#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Formatter.hpp>
#include <Util/Logger/Logger.hpp>
#include <ExecutablePipelineStage.hpp>
#include <PipelineExecutionContext.hpp>


namespace NES::InputFormatters
{

using FieldOffsetsType = uint32_t;

/// Type-erased wrapper around InputFormatterTask
class InputFormatterTaskPipeline final : public Runtime::Execution::ExecutablePipelineStage
{
public:
    template <typename T>
    requires(!std::same_as<std::decay_t<T>, InputFormatterTaskPipeline>)
    explicit InputFormatterTaskPipeline(T&& inputFormatterTask)
        : inputFormatterTask(std::make_shared<InputFormatterTaskModel<T>>(std::forward<T>(inputFormatterTask)))
    {
    }

    ~InputFormatterTaskPipeline() override = default;

    void start(Runtime::Execution::PipelineExecutionContext&) override { this->inputFormatterTask->startTask(); }
    /// Attempts to flush out a final (spanning) tuple that ends in the last byte of the last seen raw buffer.
    void stop(Runtime::Execution::PipelineExecutionContext&) override { this->inputFormatterTask->stopTask(); }
    /// (concurrently) executes an InputFormatterTask.
    /// First, uses the concrete input formatter implementation to determine the indexes of all fields of all full tuples.
    /// Second, uses the SequenceShredder to find spanning tuples.
    /// Third, processes (leading) spanning tuple and if it contains at least two tuple delimiters and therefore one complete tuple,
    /// process all complete tuples and trailing spanning tuple.
    void execute(const Memory::TupleBuffer& rawBuffer, Runtime::Execution::PipelineExecutionContext& pec) override
    {
        /// If the buffer is empty, we simply return without submitting any unnecessary work on empty buffers.
        if (rawBuffer.getBufferSize() == 0)
        {
            NES_WARNING("Received empty buffer in InputFormatterTask.");
            return;
        }

        this->inputFormatterTask->executeTask(rawBuffer, pec);
    }

    std::ostream& toString(std::ostream& os) const override { return this->inputFormatterTask->toString(os); }

    /// Describes what a InputFormatterTask that is in the InputFormatterTaskPipeline does (interface).
    struct InputFormatterTaskConcept
    {
        virtual ~InputFormatterTaskConcept() = default;
        virtual void startTask() = 0;
        virtual void stopTask() = 0;
        virtual void executeTask(const Memory::TupleBuffer& rawBuffer, Runtime::Execution::PipelineExecutionContext& pec) = 0;
        virtual std::ostream& toString(std::ostream& os) const = 0;
    };

    /// Defines the concrete behavior of the InputFormatterTaskConcept, i.e., which specific functions from T to call.
    template <typename T>
    struct InputFormatterTaskModel final : InputFormatterTaskConcept
    {
        explicit InputFormatterTaskModel(T&& inputFormatterTask) : InputFormatterTask(std::move(inputFormatterTask)) { }
        void startTask() override { InputFormatterTask.startTask(); }
        void stopTask() override { InputFormatterTask.stopTask(); }
        void executeTask(const Memory::TupleBuffer& rawBuffer, Runtime::Execution::PipelineExecutionContext& pec) override
        {
            InputFormatterTask.executeTask(rawBuffer, pec);
        }
        std::ostream& toString(std::ostream& os) const override { return InputFormatterTask.taskToString(os); }

    private:
        T InputFormatterTask;
    };

    std::shared_ptr<InputFormatterTaskConcept> inputFormatterTask;
};


}

FMT_OSTREAM(NES::InputFormatters::InputFormatterTaskPipeline);
