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
#include <functional>
#include <utility>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Pipelines/CompiledExecutablePipelineStage.hpp>
#include <Execution/Pipelines/PhysicalOperatorPipeline.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Util/DumpHelper.hpp>
#include <Util/Timer.hpp>
#include <nautilus/val.hpp>
#include <nautilus/val_ptr.hpp>

namespace NES::Runtime::Execution
{

CompiledExecutablePipelineStage::CompiledExecutablePipelineStage(
    const std::shared_ptr<PhysicalOperatorPipeline>& physicalOperatorPipeline, nautilus::engine::Options options)
    : options(std::move(options)), pipelineFunctionCompiled(nullptr), physicalOperatorPipeline(physicalOperatorPipeline)
{
}

ExecutionResult CompiledExecutablePipelineStage::execute(
    Memory::TupleBuffer& inputTupleBuffer, PipelineExecutionContext& pipelineExecutionContext, WorkerContext& workerContext)
{
    /// we call the compiled pipeline function with an input buffer and the execution context
    pipelineFunctionCompiled(
        reinterpret_cast<int8_t*>(&workerContext),
        reinterpret_cast<int8_t*>(&pipelineExecutionContext),
        reinterpret_cast<int8_t*>(std::addressof(inputTupleBuffer)));
    return ExecutionResult::Ok;
}

nautilus::engine::CallableFunction<void, int8_t*, int8_t*, int8_t*> CompiledExecutablePipelineStage::compilePipeline()
{
    Timer timer("compiler");
    timer.start();
    const std::function compiledFunction
        = [&](nautilus::val<int8_t*> workerContext, nautilus::val<int8_t*> pipelineExecutionContext, nautilus::val<int8_t*> recordBufferRef)
    {
        auto ctx = ExecutionContext(workerContext, pipelineExecutionContext);
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

uint32_t CompiledExecutablePipelineStage::start(PipelineExecutionContext&)
{
    /// TODO #349: Refactor/Cleanup this call here
    /// nop as we don't need this function in nautilus
    return 0;
}

uint32_t CompiledExecutablePipelineStage::open(PipelineExecutionContext&, WorkerContext&)
{
    /// TODO #349: Refactor/Cleanup this call here
    /// nop as we don't need this function in nautilus
    return 0;
}
uint32_t CompiledExecutablePipelineStage::close(PipelineExecutionContext&, WorkerContext&)
{
    /// TODO #349: Refactor/Cleanup this call here
    /// nop as we don't need this function in nautilus
    return 0;
}

uint32_t CompiledExecutablePipelineStage::stop(PipelineExecutionContext& pipelineExecutionContext)
{
    auto pipelineExecutionContextRef = nautilus::val<int8_t*>((int8_t*)&pipelineExecutionContext);
    auto workerContextRef = nautilus::val<int8_t*>((int8_t*)nullptr);
    auto ctx = ExecutionContext(workerContextRef, pipelineExecutionContextRef);
    physicalOperatorPipeline->getRootOperator()->terminate(ctx);
    return 0;
}

uint32_t CompiledExecutablePipelineStage::setup(PipelineExecutionContext& pipelineExecutionContext)
{
    const auto pipelineExecutionContextRef = nautilus::val<int8_t*>((int8_t*)&pipelineExecutionContext);
    const auto workerContextRef = nautilus::val<int8_t*>(nullptr);
    auto ctx = ExecutionContext(workerContextRef, pipelineExecutionContextRef);
    physicalOperatorPipeline->getRootOperator()->setup(ctx);
    pipelineFunctionCompiled = this->compilePipeline();
    return 0;
}

}
