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
#include <Pipelines/CompiledExecutablePipelineStage.hpp>

#include <functional>
#include <memory>
#include <ostream>
#include <unordered_map>
#include <utility>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <fmt/format.h>
#include <nautilus/val_ptr.hpp>
#include <Engine.hpp>
#include <ExecutionContext.hpp>
#include <PhysicalOperator.hpp>
#include <Pipeline.hpp>
#include <options.hpp>

namespace NES
{

CompiledExecutablePipelineStage::CompiledExecutablePipelineStage(
    std::shared_ptr<Pipeline> pipeline,
    std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>> operatorHandlers,
    nautilus::engine::Options options)
    : options(std::move(options))
    , compiledPipelineFunction(nullptr)
    , operatorHandlers(std::move(operatorHandlers))
    , pipeline(std::move(pipeline))
{
}

void CompiledExecutablePipelineStage::execute(
    const Memory::TupleBuffer& inputTupleBuffer, PipelineExecutionContext& pipelineExecutionContext)
{
    /// we call the compiled pipeline function with an input buffer and the execution context
    pipelineExecutionContext.setOperatorHandlers(operatorHandlers);
    Arena arena(pipelineExecutionContext.getBufferManager());
    compiledPipelineFunction(std::addressof(pipelineExecutionContext), std::addressof(inputTupleBuffer), std::addressof(arena));
}

nautilus::engine::CallableFunction<void, PipelineExecutionContext*, const Memory::TupleBuffer*, const Arena*>
CompiledExecutablePipelineStage::compilePipeline(const nautilus::engine::NautilusEngine& engine) const
{
    try
    {
        /// We must capture the operatorPipeline by value to ensure it is not destroyed before the function is called
        /// Additionally, we can NOT use const or const references for the parameters of the lambda function
        /// NOLINTBEGIN(performance-unnecessary-value-param)
        const std::function compiledFunction = [&](nautilus::val<PipelineExecutionContext*> pipelineExecutionContext,
                                                   nautilus::val<const Memory::TupleBuffer*> recordBufferRef,
                                                   nautilus::val<const Arena*> arenaRef)
        {
            auto ctx = ExecutionContext(pipelineExecutionContext, arenaRef);
            RecordBuffer recordBuffer(recordBufferRef);

            pipeline->getRootOperator().open(ctx, recordBuffer);
            pipeline->getRootOperator().close(ctx, recordBuffer);
        };
        /// NOLINTEND(performance-unnecessary-value-param)
        return engine.registerFunction(compiledFunction);
    }
    catch (...)
    {
        auto queryCompilerNautilusException = wrapExternalException();
        queryCompilerNautilusException.what() += fmt::format("Could not query compile pipeline: {}", *pipeline);
        throw queryCompilerNautilusException;
    }
}

void CompiledExecutablePipelineStage::stop(PipelineExecutionContext& pipelineExecutionContext)
{
    pipelineExecutionContext.setOperatorHandlers(operatorHandlers);
    Arena arena(pipelineExecutionContext.getBufferManager());
    ExecutionContext ctx(std::addressof(pipelineExecutionContext), std::addressof(arena));
    pipeline->getRootOperator().terminate(ctx);
}

std::ostream& CompiledExecutablePipelineStage::toString(std::ostream& os) const
{
    return os << "CompiledExecutablePipelineStage()";
}

void CompiledExecutablePipelineStage::start(PipelineExecutionContext& pipelineExecutionContext)
{
    pipelineExecutionContext.setOperatorHandlers(operatorHandlers);
    Arena arena(pipelineExecutionContext.getBufferManager());
    ExecutionContext ctx(std::addressof(pipelineExecutionContext), std::addressof(arena));
    const nautilus::engine::NautilusEngine engine(options);
    pipeline->getRootOperator().setup(ctx, engine);
    compiledPipelineFunction = this->compilePipeline(engine);
}

}
