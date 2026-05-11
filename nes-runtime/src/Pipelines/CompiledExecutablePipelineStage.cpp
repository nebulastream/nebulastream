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

#include <chrono>
#include <functional>
#include <memory>
#include <ostream>
#include <unordered_map>
#include <utility>
#include <Interface/CompiledHandle.hpp>
#include <Interface/RecordBuffer.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <cpptrace/from_current.hpp>
#include <fmt/format.h>
#include <nautilus/val_ptr.hpp>
#include <CompilationContext.hpp>
#include <Engine.hpp>
#include <ExecutionContext.hpp>
#include <Module.hpp>
#include <PhysicalOperator.hpp>
#include <Pipeline.hpp>
#include <function.hpp>
#include <options.hpp>

namespace NES
{

namespace
{
constexpr auto PIPELINE_FUNCTION_NAME = "pipeline_execute";
}

CompiledExecutablePipelineStage::CompiledExecutablePipelineStage(
    std::shared_ptr<Pipeline> pipeline,
    std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>> operatorHandlers,
    nautilus::engine::Options options)
    : engine(std::move(options))
    , moduleSlot(std::make_shared<CompiledModuleSlot>())
    , operatorHandlers(std::move(operatorHandlers))
    , pipeline(std::move(pipeline))
{
}

void CompiledExecutablePipelineStage::execute(const TupleBuffer& inputTupleBuffer, PipelineExecutionContext& pipelineExecutionContext)
{
    /// we call the compiled pipeline function with an input buffer and the execution context
    pipelineExecutionContext.setOperatorHandlers(operatorHandlers);
    Arena arena(pipelineExecutionContext.getBufferManager());
    (*compiledPipelineFunction)(std::addressof(pipelineExecutionContext), std::addressof(inputTupleBuffer), std::addressof(arena));
}

void CompiledExecutablePipelineStage::registerPipelineFunction(nautilus::engine::NautilusModule& module) const
{
    /// We must capture the operatorPipeline by value to ensure it is not destroyed before the function is called
    /// Additionally, we can NOT use const or const references for the parameters of the lambda function
    /// NOLINTBEGIN(performance-unnecessary-value-param)
    std::function<void(nautilus::val<PipelineExecutionContext*>, nautilus::val<const TupleBuffer*>, nautilus::val<const Arena*>)>
        compiledFunction = [this](
                               nautilus::val<PipelineExecutionContext*> pipelineExecutionContext,
                               nautilus::val<const TupleBuffer*> recordBufferRef,
                               nautilus::val<const Arena*> arenaRef)
    {
        auto ctx = ExecutionContext(pipelineExecutionContext, arenaRef);
        RecordBuffer recordBuffer(recordBufferRef);

        pipeline->getRootOperator().open(ctx, recordBuffer);
        switch (ctx.getOpenReturnState())
        {
            case OpenReturnState::CONTINUE: {
                pipeline->getRootOperator().close(ctx, recordBuffer);
                break;
            }
            case OpenReturnState::REPEAT: {
                nautilus::invoke(
                    +[](PipelineExecutionContext* pec, const TupleBuffer* buffer)
                    { pec->repeatTask(*buffer, std::chrono::milliseconds(0)); },
                    pipelineExecutionContext,
                    recordBufferRef);
                break;
            }
        }
    };
    /// NOLINTEND(performance-unnecessary-value-param)
    module.registerFunction<void(nautilus::val<PipelineExecutionContext*>, nautilus::val<const TupleBuffer*>, nautilus::val<const Arena*>)>(
        PIPELINE_FUNCTION_NAME, std::move(compiledFunction));
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

    /// Build one nautilus module per pipeline stage. Operators register helper functions
    /// (e.g. hashmap cleanup) into this module during setup() via CompilationContext, and
    /// the main pipeline lambda is registered immediately after. A single module.compile()
    /// call then produces one IR/optimization/codegen pass for the entire pipeline.
    auto moduleBuilder = engine.createModule();
    CompilationContext compilationCtx{moduleBuilder, moduleSlot};
    pipeline->getRootOperator().setup(ctx, compilationCtx);

    CPPTRACE_TRY
    {
        registerPipelineFunction(moduleBuilder);
        moduleSlot->compiled.emplace(moduleBuilder.compile());
    }
    CPPTRACE_CATCH(...)
    {
        throw wrapExternalException(fmt::format("Could not query compile pipeline: {}", *pipeline));
    }

    compiledPipelineFunction.emplace(moduleSlot, PIPELINE_FUNCTION_NAME);
}

}
