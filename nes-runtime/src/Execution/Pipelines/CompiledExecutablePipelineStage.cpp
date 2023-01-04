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
#include <Execution/Pipelines/CompiledExecutablePipelineStage.hpp>
#include <Execution/Pipelines/PhysicalOperatorPipeline.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Nautilus/Backends/CompilationBackend.hpp>
#include <Nautilus/Tracing/Phases/SSACreationPhase.hpp>
#include <Nautilus/Tracing/Phases/TraceToIRConversionPhase.hpp>
#include <Nautilus/Tracing/TraceContext.hpp>
#include <Util/Timer.hpp>

namespace NES::Runtime::Execution {

CompiledExecutablePipelineStage::CompiledExecutablePipelineStage(
    std::shared_ptr<PhysicalOperatorPipeline> physicalOperatorPipeline)
    : NautilusExecutablePipelineStage(physicalOperatorPipeline) {}

ExecutionResult CompiledExecutablePipelineStage::execute(TupleBuffer& inputTupleBuffer,
                                                         PipelineExecutionContext& pipelineExecutionContext,
                                                         WorkerContext& workerContext) {
    // wait till pipeline is ready
    executablePipeline.wait();
    auto& pipeline = executablePipeline.get();
    auto function = pipeline->getInvocableMember<void (*)(void*, void*, void*)>("execute");
    function((void*) &pipelineExecutionContext, &workerContext, std::addressof(inputTupleBuffer));
    return ExecutionResult::Ok;
}

std::unique_ptr<Nautilus::Backends::Executable> CompiledExecutablePipelineStage::compilePipeline() {
    // compile after setup
    Timer timer("CompilationBasedPipelineExecutionEngine");
    timer.start();

    auto pipelineExecutionContextRef = Value<MemRef>((int8_t*) nullptr);
    pipelineExecutionContextRef.ref =
        Nautilus::Tracing::ValueRef(INT32_MAX, 0, NES::Nautilus::IR::Types::StampFactory::createAddressStamp());
    auto workerContextRef = Value<MemRef>((int8_t*) nullptr);
    workerContextRef.ref =
        Nautilus::Tracing::ValueRef(INT32_MAX, 1, NES::Nautilus::IR::Types::StampFactory::createAddressStamp());
    auto memRef = Nautilus::Value<Nautilus::MemRef>(std::make_unique<Nautilus::MemRef>(Nautilus::MemRef(0)));
    memRef.ref = Nautilus::Tracing::ValueRef(INT32_MAX, 2, NES::Nautilus::IR::Types::StampFactory::createAddressStamp());
    auto recordBuffer = RecordBuffer(memRef);

    auto rootOperator = physicalOperatorPipeline->getRootOperator();
    // generate trace
    auto executionTrace = Nautilus::Tracing::traceFunction([&]() {
        auto traceContext = Tracing::TraceContext::get();
        traceContext->addTraceArgument(pipelineExecutionContextRef.ref);
        traceContext->addTraceArgument(workerContextRef.ref);
        traceContext->addTraceArgument(recordBuffer.getReference().ref);
        auto ctx = ExecutionContext(workerContextRef, pipelineExecutionContextRef);
        rootOperator->open(ctx, recordBuffer);
        rootOperator->close(ctx, recordBuffer);
    });

    Nautilus::Tracing::SSACreationPhase ssaCreationPhase;
    NES_DEBUG(*executionTrace.get());
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    NES_DEBUG(*executionTrace.get());
    timer.snapshot("Trace Generation");

    Nautilus::Tracing::TraceToIRConversionPhase irCreationPhase;
    auto ir = irCreationPhase.apply(executionTrace);
    timer.snapshot("NESIR Generation");
    NES_DEBUG(ir->toString());

    auto& compilationBackend = Nautilus::Backends::CompilationBackendRegistry::getPlugin("MLIR");
    auto executable = compilationBackend->compile(ir);
    timer.snapshot("MLIR Compilation");
    NES_DEBUG(timer);
    return executable;
}

uint32_t CompiledExecutablePipelineStage::setup(PipelineExecutionContext& pipelineExecutionContext) {
    NautilusExecutablePipelineStage::setup(pipelineExecutionContext);
    // TODO enable async compilation #3357
    executablePipeline = std::async(std::launch::deferred, [this] {
                             return this->compilePipeline();
                         }).share();
    executablePipeline.get();
    return 0;
}

}// namespace NES::Runtime::Execution