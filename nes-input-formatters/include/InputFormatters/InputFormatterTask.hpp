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

#include <ostream>
#include <Identifiers/Identifiers.hpp>
#include <InputFormatters/InputFormatter.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Formatter.hpp>
#include <ExecutablePipelineStage.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES::InputFormatters
{

/// Forward referencing SequenceShredder to keep it as a private implementation detail of nes-input-formatters
class SequenceShredder;

/// Takes tuple buffers with raw bytes (TBRaw/TBR), parses the TBRs and writes the formatted data to formatted tuple buffers (TBFormatted/TBF)
class InputFormatterTask : public NES::Runtime::Execution::ExecutablePipelineStage
{
public:
    explicit InputFormatterTask(OriginId originId, std::unique_ptr<InputFormatter> inputFormatter);
    ~InputFormatterTask() override;

    InputFormatterTask(const InputFormatterTask&) = delete;
    InputFormatterTask& operator=(const InputFormatterTask&) = delete;
    InputFormatterTask(InputFormatterTask&&) = delete;
    InputFormatterTask& operator=(InputFormatterTask&&) = delete;

    void start(Runtime::Execution::PipelineExecutionContext&) override { /* noop */ }

    void stop(Runtime::Execution::PipelineExecutionContext&) override;
    void
    execute(const Memory::TupleBuffer& inputTupleBuffer, Runtime::Execution::PipelineExecutionContext& pipelineExecutionContext) override;

    std::ostream& toString(std::ostream& os) const override;

private:
    OriginId originId;
    /// Synchronizes between different InputFormatterTasks (see documentation of class for details)
    std::unique_ptr<SequenceShredder> sequenceShredder;
    /// TODO #679: realize below comment
    /// Implements the format-specific functions to determine the offsets of fields, as well as format-specific parsing functions of fields
    std::unique_ptr<InputFormatter> inputFormatter;
};


}

FMT_OSTREAM(NES::InputFormatters::InputFormatterTask);
