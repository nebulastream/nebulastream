#include "Experimental/Interpreter/RecordBuffer.hpp"
#include "Experimental/MLIR/MLIRUtility.hpp"
#include <Experimental/ExecutionEngine/CompilationBasedPipelineExecutionEngine.hpp>
#include <Experimental/ExecutionEngine/ExecutablePipeline.hpp>
#include <Experimental/ExecutionEngine/PhysicalOperatorPipeline.hpp>
#include <Experimental/Interpreter/DataValue/Value.hpp>
#include <Experimental/Interpreter/ExecutionContext.hpp>
#include <Experimental/Runtime/ExecutionContext.hpp>
#include <Experimental/Runtime/PipelineContext.hpp>
#include <Experimental/Trace/Phases/SSACreationPhase.hpp>
#include <Experimental/Trace/Phases/TraceToIRConversionPhase.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/WorkerContext.hpp>
#include <memory>

namespace NES::ExecutionEngine::Experimental {

std::shared_ptr<ExecutablePipeline>
CompilationBasedPipelineExecutionEngine::compile(std::shared_ptr<PhysicalOperatorPipeline> physicalOperatorPipeline) {
    Trace::SSACreationPhase ssaCreationPhase;
    Trace::TraceToIRConversionPhase irCreationPhase;
    auto bm = std::make_shared<Runtime::BufferManager>(100);
    auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    schema->addField("f1", BasicType::UINT64);
    auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bm->getBufferSize());
    auto pipelineContext = std::make_shared<Runtime::Execution::PipelineContext>();
    auto runtimeExecutionContext = Runtime::Execution::ExecutionContext(nullptr, pipelineContext.get());
    auto runtimeExecutionContextRef = Interpreter::Value<Interpreter::MemRef>(
        std::make_unique<Interpreter::MemRef>(Interpreter::MemRef((int8_t*) &runtimeExecutionContext)));
    runtimeExecutionContextRef.ref = Trace::ValueRef(INT32_MAX, 3, IR::Operations::INT8PTR);
    auto executionContext = Interpreter::ExecutionContext(runtimeExecutionContextRef);

    auto memRef = Interpreter::Value<Interpreter::MemRef>(std::make_unique<Interpreter::MemRef>(Interpreter::MemRef(0)));
    memRef.ref = Trace::ValueRef(INT32_MAX, 0, IR::Operations::INT8PTR);
    auto recordBuffer = Interpreter::RecordBuffer(memoryLayout, memRef);

    auto rootOperator = physicalOperatorPipeline->getRootOperator();
    // generate trace
    auto executionTrace = Trace::traceFunctionSymbolically([&rootOperator, &executionContext, &recordBuffer]() {
        rootOperator->open(executionContext, recordBuffer);
        rootOperator->close(executionContext, recordBuffer);
    });
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    std::cout << *executionTrace.get() << std::endl;
    auto ir = irCreationPhase.apply(executionTrace);
    //std::cout << ir->toString() << std::endl;
    //ir = loopInferencePhase.apply(ir);
    std::cout << ir->toString() << std::endl;
    auto mlirUtility = new MLIR::MLIRUtility("/home/rudi/mlir/generatedMLIR/locationTest.mlir", false);
    MLIR::MLIRUtility::DebugFlags df = {false, false, false};
    int loadedModuleSuccess = mlirUtility->loadAndProcessMLIR(ir, nullptr, false);
    auto engine = mlirUtility->prepareEngine();
    auto rx = std::make_shared<ExecutablePipeline>(pipelineContext, std::move(engine));
    return rx;
}

}// namespace NES::ExecutionEngine::Experimental