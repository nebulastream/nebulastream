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
#include <string>
#include <stdint.h>
#include <Runtime/Execution/ExecutablePipelineStage.hpp>

#include <Runtime/ExecutionResult.hpp>

namespace NES
{
namespace Runtime
{
class TupleBuffer;
class WorkerContext;
} /// namespace Runtime
} /// namespace NES

namespace NES::Runtime::Execution
{
class PhysicalOperatorPipeline;
class PipelineExecutionContext;

/**
 * @brief This is an adopt to the legacy pipeline stage to the nautilus operators.
 * TODO After finishing the migration the nautilus based we can replace the interface.
 */
class NautilusExecutablePipelineStage : public ExecutablePipelineStage
{
public:
    NautilusExecutablePipelineStage(std::shared_ptr<PhysicalOperatorPipeline> physicalOperatorPipeline);
    uint32_t setup(PipelineExecutionContext& pipelineExecutionContext) override;
    uint32_t start(PipelineExecutionContext& pipelineExecutionContext) override;
    uint32_t open(PipelineExecutionContext& pipelineExecutionContext, WorkerContext& workerContext) override;
    ExecutionResult
    execute(TupleBuffer& inputTupleBuffer, PipelineExecutionContext& pipelineExecutionContext, WorkerContext& workerContext) override;
    uint32_t close(PipelineExecutionContext& pipelineExecutionContext, WorkerContext& workerContext) override;
    uint32_t stop(PipelineExecutionContext& pipelineExecutionContext) override;
    /**
     * @deprecated This method will be removed as it is not applicable to the new compilation backend.
     */
    std::string getCodeAsString() override;

protected:
    std::shared_ptr<PhysicalOperatorPipeline> physicalOperatorPipeline;
};

} /// namespace NES::Runtime::Execution
