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
#include <cstdint>
#include <functional>
#include <memory>
#include <ostream>
#include <utility>
#include <vector>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Pipelines/CompiledExecutablePipelineStage.hpp>
#include <Execution/Pipelines/PhysicalOperatorPipeline.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Timer.hpp>
#include <nautilus/val_ptr.hpp>
#include <Engine.hpp>
#include <options.hpp>

namespace NES::Runtime::Execution
{

CompiledExecutablePipelineStage::CompiledExecutablePipelineStage(
    const std::shared_ptr<PhysicalOperatorPipeline>& physicalOperatorPipeline,
    std::vector<std::shared_ptr<OperatorHandler>> operatorHandlers,
    nautilus::engine::Options options)
    : options(std::move(options))
    , compiledPipelineFunction(nullptr)
    , operatorHandlers(std::move(operatorHandlers))
    , physicalOperatorPipeline(physicalOperatorPipeline)
{
}

void CompiledExecutablePipelineStage::execute(
    const Memory::TupleBuffer& inputTupleBuffer, PipelineExecutionContext& pipelineExecutionContext)
{
    /// we call the compiled pipeline function with an input buffer and the execution context
    pipelineExecutionContext.setOperatorHandlers(operatorHandlers);
    Arena arena(pipelineExecutionContext.getBufferManager());
    compiledPipelineFunction(&pipelineExecutionContext, std::addressof(inputTupleBuffer), &arena);
}

nautilus::engine::CallableFunction<void, PipelineExecutionContext*, const Memory::TupleBuffer*, Arena*>
CompiledExecutablePipelineStage::compilePipeline() const
{
    Timer timer("compiler");
    timer.start();

    /// We must capture the physicalOperatorPipeline by value to ensure it is not destroyed before the function is called
    /// Additionally, we can NOT use const or const references for the parameters of the lambda function
    const std::function compiledFunction
        = [&](nautilus::val<PipelineExecutionContext*> pipelineExecutionContext, nautilus::val<const Memory::TupleBuffer*> recordBufferRef, nautilus::val<Arena*> arena)
    {
        auto ctx = ExecutionContext(pipelineExecutionContext, arena);
        RecordBuffer recordBuffer(recordBufferRef);
        physicalOperatorPipeline->getRootOperator()->open(ctx, recordBuffer);
        physicalOperatorPipeline->getRootOperator()->close(ctx, recordBuffer);
    };

    const nautilus::engine::NautilusEngine engine(options);
    auto executable = engine.registerFunction(compiledFunction);
    timer.snapshot("Compiled");
    timer.pause();
    NES_INFO("Timer: {}", fmt::streamed(timer));
    return executable;
}

void CompiledExecutablePipelineStage::stop(PipelineExecutionContext& pipelineExecutionContext)
{
    pipelineExecutionContext.setOperatorHandlers(operatorHandlers);
    pipelineExecutionContext.setOperatorHandlers(operatorHandlers);
    Arena arena(pipelineExecutionContext.getBufferManager());
    auto ctx = ExecutionContext(&pipelineExecutionContext, &arena);
    physicalOperatorPipeline->getRootOperator()->terminate(ctx);
}

std::ostream& CompiledExecutablePipelineStage::toString(std::ostream& os) const
{
    return os << "CompiledExecutablePipelineStage()";
}

void CompiledExecutablePipelineStage::start(PipelineExecutionContext& pipelineExecutionContext)
{
    pipelineExecutionContext.setOperatorHandlers(operatorHandlers);
    pipelineExecutionContext.setOperatorHandlers(operatorHandlers);
    Arena arena(pipelineExecutionContext.getBufferManager());
    auto ctx = ExecutionContext(std::addressof(pipelineExecutionContext), &arena);
    physicalOperatorPipeline->getRootOperator()->setup(ctx);
    compiledPipelineFunction = this->compilePipeline();
}

}
