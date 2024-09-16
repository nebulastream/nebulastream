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
    : options(std::move(options)), pipelineFunction(nullptr), physicalOperatorPipeline(physicalOperatorPipeline)
{
}

ExecutionResult CompiledExecutablePipelineStage::execute(
    Memory::TupleBuffer& inputTupleBuffer, PipelineExecutionContext& pipelineExecutionContext, WorkerContext& workerContext)
{
    /// we call the compiled pipeline function with an input buffer and the execution context
    pipelineFunction(
        reinterpret_cast<int8_t*>(&workerContext),
        reinterpret_cast<int8_t*>(&pipelineExecutionContext),
        reinterpret_cast<int8_t*>(std::addressof(inputTupleBuffer)));
    return ExecutionResult::Ok;
}

auto CompiledExecutablePipelineStage::compilePipeline()
{
    /// compiler after setup
    Timer timer("CompilationBasedPipelineExecutionEngine " + options.getOptionOrDefault<std::string>("engine_backend", ""));
    timer.start();

    std::function<void(nautilus::val<int8_t*>, nautilus::val<int8_t*>, nautilus::val<int8_t*>)> compiledFunction
        = [&](nautilus::val<int8_t*> workerContext, nautilus::val<int8_t*> pipelineExecutionContext, nautilus::val<int8_t*> recordBufferRef)
    {
        auto ctx = ExecutionContext(workerContext, pipelineExecutionContext);
        RecordBuffer recordBuffer(recordBufferRef);
        physicalOperatorPipeline->getRootOperator()->open(ctx, recordBuffer);
        physicalOperatorPipeline->getRootOperator()->close(ctx, recordBuffer);
    };

    engine = std::make_shared<nautilus::engine::NautilusEngine>(options);
    auto executable = engine->registerFunction(compiledFunction);
    timer.snapshot("Compilation");
    std::stringstream timerAsString;
    timerAsString << timer;
    NES_INFO("{}", timerAsString.str());
    return executable;
}

uint32_t CompiledExecutablePipelineStage::start(PipelineExecutionContext&)
{
    /// nop as we don't need this function in nautilus
    return 0;
}

uint32_t CompiledExecutablePipelineStage::open(Execution::PipelineExecutionContext&, WorkerContext&)
{
    /// nop as we don't need this function in nautilus
    return 0;
}
uint32_t CompiledExecutablePipelineStage::close(PipelineExecutionContext&, WorkerContext&)
{
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
    pipelineFunction = this->compilePipeline();
    return 0;
}

}
