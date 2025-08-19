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

namespace NES
{
class RawTupleBuffer;

/// Type-erased wrapper around InputFormatterTask
class InputFormatterTaskPipeline final : public ExecutablePipelineStage
{
public:
    template <typename T>
    requires(not std::same_as<std::decay_t<T>, InputFormatterTaskPipeline>)
    explicit InputFormatterTaskPipeline(T&& inputFormatterTask)
        : inputFormatterTask(std::make_unique<InputFormatterTaskModel<T>>(std::forward<T>(inputFormatterTask)))
    {
    }

    ~InputFormatterTaskPipeline() override = default;

    void start(PipelineExecutionContext&) override;
    /// Attempts to flush out a final (spanning) tuple that ends in the last byte of the last seen raw buffer.
    void stop(PipelineExecutionContext&) override;
    /// (concurrently) executes an InputFormatterTask.
    /// First, uses the concrete InputFormatIndexer implementation to determine the indexes of all fields of all full tuples.
    /// Second, uses the SequenceShredder to find spanning tuples.
    /// Third, processes (leading) spanning tuple and if it contains at least two tuple delimiters and therefore one complete tuple,
    /// process all complete tuples and trailing spanning tuple.
    void execute(const TupleBuffer& rawTupleBuffer, PipelineExecutionContext& pec) override;

    std::ostream& toString(std::ostream& os) const override;

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
    template <typename T>
    struct InputFormatterTaskModel final : InputFormatterTaskConcept
    {
        explicit InputFormatterTaskModel(T&& inputFormatterTask) : inputFormatterTask(std::move(inputFormatterTask)) { }

        void startTask() override { inputFormatterTask.startTask(); }

        void stopTask() override { inputFormatterTask.stopTask(); }

        void executeTask(const RawTupleBuffer& rawTupleBuffer, PipelineExecutionContext& pec) override
        {
            inputFormatterTask.executeTask(rawTupleBuffer, pec);
        }

        std::ostream& toString(std::ostream& os) const override { return inputFormatterTask.taskToString(os); }

    private:
        T inputFormatterTask;
    };

    std::unique_ptr<InputFormatterTaskConcept> inputFormatterTask;
};

}

FMT_OSTREAM(NES::InputFormatterTaskPipeline);
