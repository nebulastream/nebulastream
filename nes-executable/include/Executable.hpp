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
#include <Runtime/TupleBuffer.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES::Runtime::Execution
{
class ExecutablePipelineStage
{
public:
    virtual ~ExecutablePipelineStage() = default;
    virtual uint32_t start(PipelineExecutionContext& pipelineExecutionContext) = 0;
    virtual void execute(const Memory::TupleBuffer& inputTupleBuffer, PipelineExecutionContext& pipelineExecutionContext) = 0;
    virtual uint32_t stop(PipelineExecutionContext& pipelineExecutionContext) = 0;

    friend std::ostream& operator<<(std::ostream& os, const ExecutablePipelineStage& obj) { return obj.toString(os); }

protected:
    virtual std::ostream& toString(std::ostream& os) const = 0;
};

using ExecutablePipelineStagePtr = std::unique_ptr<ExecutablePipelineStage>;
}
