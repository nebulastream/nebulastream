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
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Pipelines/CompiledExecutablePipelineStage.hpp>
#include <Execution/Pipelines/PhysicalOperatorPipeline.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Nautilus/Backends/CompilationBackend.hpp>
#include <Nautilus/Backends/Executable.hpp>
#include <Nautilus/IR/Phases/RemoveBrOnlyBlocksPhase.hpp>
#include <Nautilus/IR/Phases/ConstantValuePropagationPhase.hpp>
#include <Nautilus/IR/Phases/RedundantOperationRemovalPhase.hpp>
#include <Nautilus/Tracing/Phases/SSACreationPhase.hpp>
#include <Nautilus/Tracing/Phases/TraceToIRConversionPhase.hpp>
#include <Nautilus/Tracing/TraceContext.hpp>
#include <Util/DumpHelper.hpp>
#include <Util/Timer.hpp>

namespace NES::Runtime::Execution {

CompiledExecutablePipelineStage::CompiledExecutablePipelineStage(
    const std::shared_ptr<PhysicalOperatorPipeline>& physicalOperatorPipeline,
    const std::string& compilationBackend,
    const Nautilus::CompilationOptions& options)
    : NautilusExecutablePipelineStage(physicalOperatorPipeline), compilationBackend(compilationBackend), options(options),
      pipelineFunction(nullptr) {}

ExecutionResult CompiledExecutablePipelineStage::execute(TupleBuffer& inputTupleBuffer,
                                                         PipelineExecutionContext& pipelineExecutionContext,
                                                         WorkerContext& workerContext) {
    // wait till pipeline is ready
    executablePipeline.wait();

    pipelineFunction((void*) &pipelineExecutionContext, &workerContext, std::addressof(inputTupleBuffer));
    return ExecutionResult::Ok;
}

std::shared_ptr<NES::Nautilus::IR::IRGraph> CompiledExecutablePipelineStage::createIR(DumpHelper& dumpHelper, Timer<>& timer) {

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
    dumpHelper.dump("O. AfterTracing.trace", executionTrace->toString());

    Nautilus::Tracing::SSACreationPhase ssaCreationPhase;
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    dumpHelper.dump("1. AfterTracing.trace", executionTrace->toString());
    timer.snapshot("Trace Generation");

    Nautilus::Tracing::TraceToIRConversionPhase irCreationPhase;
    auto ir = irCreationPhase.apply(executionTrace);
    timer.snapshot("IR Generation");
    dumpHelper.dump("2. IR AfterGeneration.ir", ir->toString());

    // Apply IR phases:
    Nautilus::IR::RemoveBrOnlyBlocksPhase removeBrOnlyBlocksPhase;
    Nautilus::IR::ConstantValuePropagationPhase constValuePropagationPhase;
    Nautilus::IR::RedundantOperationRemovalPhase redundantOperationRemovalPhase;
    std::shared_ptr<NES::Nautilus::IR::Operations::FunctionOperation> rootOperation = ir->getRootOperation();
    removeBrOnlyBlocksPhase.apply(ir);
    constValuePropagationPhase.apply(ir);
    redundantOperationRemovalPhase.apply(ir);
    return ir;
}

std::unique_ptr<Nautilus::Backends::Executable> CompiledExecutablePipelineStage::compilePipeline() {
    // compile after setup
    auto dumpHelper = DumpHelper::create(options.getIdentifier(),
                                         options.isDumpToConsole(),
                                         options.isDumpToFile(),
                                         options.getDumpOutputPath());
    Timer timer("CompilationBasedPipelineExecutionEngine " + options.getIdentifier());
    timer.start();
    auto& compiler = Nautilus::Backends::CompilationBackendRegistry::getPlugin(compilationBackend);
    auto ir = createIR(dumpHelper, timer);
    auto executable = compiler->compile(ir, options, dumpHelper);
    timer.snapshot("Compilation");
    std::stringstream timerAsString;
    timerAsString << timer;
    NES_INFO("{}", timerAsString.str());
    return executable;
}

uint32_t CompiledExecutablePipelineStage::setup(PipelineExecutionContext& pipelineExecutionContext) {
    NautilusExecutablePipelineStage::setup(pipelineExecutionContext);
    // TODO enable async compilation #3357
    executablePipeline = std::async(std::launch::deferred, [this] {
                             return this->compilePipeline();
                         }).share();
    pipelineFunction = executablePipeline.get()->getInvocableMember<void, void*, void*, void*>("execute");
    return 0;
}

}// namespace NES::Runtime::Execution