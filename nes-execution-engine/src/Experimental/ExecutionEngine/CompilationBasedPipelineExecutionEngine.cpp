#include <Experimental/Interpreter/RecordBuffer.hpp>
#include <Experimental/MLIR/MLIRUtility.hpp>
#include <Experimental/ExecutionEngine/CompilationBasedPipelineExecutionEngine.hpp>
#include <Experimental/ExecutionEngine/ExecutablePipeline.hpp>
#include <Experimental/ExecutionEngine/PhysicalOperatorPipeline.hpp>
#include <Experimental/Interpreter/DataValue/Value.hpp>
#include <Experimental/Interpreter/ExecutionContext.hpp>
#include <Experimental/Runtime/RuntimeExecutionContext.hpp>
#include <Experimental/Runtime/RuntimePipelineContext.hpp>
#include <Experimental/Trace/Phases/SSACreationPhase.hpp>
#include <Experimental/Trace/Phases/TraceToIRConversionPhase.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Util/Timer.hpp>
#include <Runtime/WorkerContext.hpp>
#include <memory>

namespace NES::ExecutionEngine::Experimental {

std::shared_ptr<ExecutablePipeline>
CompilationBasedPipelineExecutionEngine::compile(std::shared_ptr<PhysicalOperatorPipeline> physicalOperatorPipeline) {
    Timer timer("CompilationBasedPipelineExecutionEngine");
    timer.start();
    Trace::SSACreationPhase ssaCreationPhase;
    Trace::TraceToIRConversionPhase irCreationPhase;
    auto pipelineContext = std::make_shared<Runtime::Execution::RuntimePipelineContext>();
    auto runtimeExecutionContext = Runtime::Execution::RuntimeExecutionContext(nullptr, pipelineContext.get());
    auto runtimeExecutionContextRef = Interpreter::Value<Interpreter::MemRef>(
        std::make_unique<Interpreter::MemRef>(Interpreter::MemRef((int8_t*) &runtimeExecutionContext)));
    runtimeExecutionContextRef.ref = Trace::ValueRef(INT32_MAX, 3, IR::Operations::INT8PTR);
    auto executionContext = Interpreter::RuntimeExecutionContext(runtimeExecutionContextRef);

    auto memRef = Interpreter::Value<Interpreter::MemRef>(std::make_unique<Interpreter::MemRef>(Interpreter::MemRef(0)));
    memRef.ref = Trace::ValueRef(INT32_MAX, 0, IR::Operations::INT8PTR);
    auto recordBuffer = Interpreter::RecordBuffer(memRef);

    auto rootOperator = physicalOperatorPipeline->getRootOperator();
    // generate trace
    auto executionTrace = Trace::traceFunctionSymbolically([&rootOperator, &executionContext, &recordBuffer]() {
        rootOperator->open(executionContext, recordBuffer);
        rootOperator->close(executionContext, recordBuffer);
    });
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    std::cout << *executionTrace.get() << std::endl;
    timer.snapshot("TraceGeneration");
    auto ir = irCreationPhase.apply(executionTrace);
    timer.snapshot("NESIRGeneration");
    //std::cout << ir->toString() << std::endl;
    //ir = loopInferencePhase.apply(ir);
    std::cout << ir->toString() << std::endl;
    auto mlirUtility = new MLIR::MLIRUtility("/home/rudi/mlir/generatedMLIR/locationTest.mlir", false);
    MLIR::MLIRUtility::DebugFlags df = {false, false, false};
    int loadedModuleSuccess = mlirUtility->loadAndProcessMLIR(ir, nullptr, false);
    timer.snapshot("MLIRGeneration");
    timer.pause();
    NES_INFO("CompilationBasedPipelineExecutionEngine TIME: " << timer);
    auto engine = mlirUtility->prepareEngine();
    auto rx = std::make_shared<ExecutablePipeline>(pipelineContext, physicalOperatorPipeline, std::move(engine));
    return rx;
}

}// namespace NES::ExecutionEngine::Experimental