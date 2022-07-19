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
#include <Experimental/NESIR/Types/StampFactory.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Util/Timer.hpp>
#include <Runtime/WorkerContext.hpp>
#include <memory>

namespace NES::ExecutionEngine::Experimental {

CompilationBasedPipelineExecutionEngine::CompilationBasedPipelineExecutionEngine(std::shared_ptr<PipelineCompilerBackend> backend): PipelineExecutionEngine(), backend(backend) {}

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
    runtimeExecutionContextRef.ref = Trace::ValueRef(INT32_MAX, 3, IR::Types::StampFactory::createAddressStamp());
    auto executionContext = Interpreter::RuntimeExecutionContext(runtimeExecutionContextRef);

    auto memRef = Interpreter::Value<Interpreter::MemRef>(std::make_unique<Interpreter::MemRef>(Interpreter::MemRef(0)));
    memRef.ref = Trace::ValueRef(INT32_MAX, 0, IR::Types::StampFactory::createAddressStamp());
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

    return backend->compile(pipelineContext, physicalOperatorPipeline, ir);
}

}// namespace NES::ExecutionEngine::Experimental