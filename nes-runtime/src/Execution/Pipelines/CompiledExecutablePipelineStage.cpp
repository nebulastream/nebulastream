#include "Execution/RecordBuffer.hpp"
#include <Execution/Pipelines/CompiledExecutablePipelineStage.hpp>
#include <Execution/Pipelines/PhysicalOperatorPipeline.hpp>
#include <Nautilus/Backends/CompilationBackend.hpp>
#include <Nautilus/Tracing/Phases/SSACreationPhase.hpp>
#include <Nautilus/Tracing/Phases/TraceToIRConversionPhase.hpp>
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
    auto executionTrace = Nautilus::Tracing::traceFunctionSymbolically([&]() {
        Nautilus::Tracing::getThreadLocalTraceContext()->addTraceArgument(pipelineExecutionContextRef.ref);
        Nautilus::Tracing::getThreadLocalTraceContext()->addTraceArgument(workerContextRef.ref);
        Nautilus::Tracing::getThreadLocalTraceContext()->addTraceArgument(recordBuffer.getReference().ref);
        auto ctx = ExecutionContext(workerContextRef, pipelineExecutionContextRef);
        rootOperator->open(ctx, recordBuffer);
        rootOperator->close(ctx, recordBuffer);
    });

    Nautilus::Tracing::SSACreationPhase ssaCreationPhase;
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    std::cout << *executionTrace.get() << std::endl;
    timer.snapshot("TraceGeneration");

    Nautilus::Tracing::TraceToIRConversionPhase irCreationPhase;
    auto ir = irCreationPhase.apply(executionTrace);
    timer.snapshot("NESIRGeneration");
    std::cout << ir->toString() << std::endl;
    std::cout << timer << std::endl;
    //ir = loopInferencePhase.apply(ir);

    auto& compilationBackend = Nautilus::Backends::CompilationBackendRegistry::getPlugin("MLIR");
    auto executable = compilationBackend->compile(ir);
    return executable;
}

uint32_t CompiledExecutablePipelineStage::setup(PipelineExecutionContext& pipelineExecutionContext) {
    NautilusExecutablePipelineStage::setup(pipelineExecutionContext);
    executablePipeline = std::async(std::launch::async, [this] {
                             return this->compilePipeline();
                         }).share();
    return 0;
}

}// namespace NES::Runtime::Execution