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
#include <vector>
#include <Execution/Pipelines/PhysicalOperatorPipeline.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/PinnedBuffer.hpp>
#include <nautilus/Engine.hpp>
#include <ExecutablePipelineStage.hpp>
#include <options.hpp>

namespace NES::Runtime::Execution
{

/**
 * @brief A compiled executable pipeline stage uses nautilus-lib to compile a pipeline to a code snippet.
 */
class CompiledExecutablePipelineStage final : public ExecutablePipelineStage
{
public:
    CompiledExecutablePipelineStage(
        const std::shared_ptr<PhysicalOperatorPipeline>& physicalOperatorPipeline,
        std::vector<std::shared_ptr<OperatorHandler>> operatorHandler,
        nautilus::engine::Options options);
    void start(PipelineExecutionContext& pipelineExecutionContext) override;
    void execute(const Memory::PinnedBuffer& inputTupleBuffer, PipelineExecutionContext& pipelineExecutionContext) override;
    void stop(PipelineExecutionContext& pipelineExecutionContext) override;

protected:
    std::ostream& toString(std::ostream& os) const override;

private:
    [[nodiscard]] nautilus::engine::CallableFunction<void, PipelineExecutionContext*, const Memory::PinnedBuffer*> compilePipeline() const;
    const nautilus::engine::Options options;
    nautilus::engine::CallableFunction<void, PipelineExecutionContext*, const Memory::PinnedBuffer*> compiledPipelineFunction;
    std::vector<std::shared_ptr<OperatorHandler>> operatorHandlers;
    std::shared_ptr<PhysicalOperatorPipeline> physicalOperatorPipeline;
};

}
