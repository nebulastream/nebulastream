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
#include <Experimental/ExecutionEngine/CompilationBasedPipelineExecutionEngine.hpp>
#include <Experimental/ExecutionEngine/ExecutablePipeline.hpp>
#include <Experimental/ExecutionEngine/PhysicalOperatorPipeline.hpp>
#include <Experimental/Interpreter/ExecutionContext.hpp>
#include <Experimental/Interpreter/RecordBuffer.hpp>
#include <Experimental/Runtime/RuntimeExecutionContext.hpp>
#include <Experimental/Runtime/RuntimePipelineContext.hpp>
#include <Nautilus/IR/Types/StampFactory.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <Nautilus/Tracing/Phases/SSACreationPhase.hpp>
#include <Nautilus/Tracing/Phases/TraceToIRConversionPhase.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Util/Timer.hpp>
#include <memory>

namespace NES::ExecutionEngine::Experimental {

CompilationBasedPipelineExecutionEngine::CompilationBasedPipelineExecutionEngine(std::shared_ptr<PipelineCompilerBackend> backend)
    : PipelineExecutionEngine(), backend(backend) {}

std::shared_ptr<ExecutablePipeline>
CompilationBasedPipelineExecutionEngine::compile(std::shared_ptr<PhysicalOperatorPipeline> physicalOperatorPipeline) {
    Timer timer("CompilationBasedPipelineExecutionEngine");
    timer.start();
    Nautilus::Tracing::SSACreationPhase ssaCreationPhase;
    Nautilus::Tracing::TraceToIRConversionPhase irCreationPhase;
    auto pipelineContext = std::make_shared<Runtime::Execution::RuntimePipelineContext>();
    auto runtimeExecutionContext = Runtime::Execution::RuntimeExecutionContext(nullptr, pipelineContext.get());
    auto runtimeExecutionContextRef = Nautilus::Value<Nautilus::MemRef>(
        std::make_unique<Nautilus::MemRef>(Nautilus::MemRef((int8_t*) &runtimeExecutionContext)));
    runtimeExecutionContextRef.ref =
        Nautilus::Tracing::ValueRef(INT32_MAX, 3, Nautilus::IR::Types::StampFactory::createAddressStamp());
    auto executionContext = Nautilus::RuntimeExecutionContext(runtimeExecutionContextRef);

    auto memRef = Nautilus::Value<Nautilus::MemRef>(std::make_unique<Nautilus::MemRef>(Nautilus::MemRef(0)));
    memRef.ref = Nautilus::Tracing::ValueRef(INT32_MAX, 0, NES::Nautilus::IR::Types::StampFactory::createAddressStamp());
    auto recordBuffer = Nautilus::RecordBuffer(memRef);

    auto rootOperator = physicalOperatorPipeline->getRootOperator();
    // generate trace
    auto executionTrace = Nautilus::Tracing::traceFunctionSymbolically([&rootOperator, &executionContext, &recordBuffer]() {
        Nautilus::Tracing::getThreadLocalTraceContext()->addTraceArgument(executionContext.getReference().ref);
        Nautilus::Tracing::getThreadLocalTraceContext()->addTraceArgument(recordBuffer.getReference().ref);
        rootOperator->open(executionContext, recordBuffer);
        rootOperator->close(executionContext, recordBuffer);
    });
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    NES_DEBUG(*executionTrace.get());
    timer.snapshot("TraceGeneration");
    auto ir = irCreationPhase.apply(executionTrace);
    timer.snapshot("NESIRGeneration");
    NES_DEBUG(ir->toString());
    NES_DEBUG(timer);
    //ir = loopInferencePhase.apply(ir);

    return backend->compile(pipelineContext, physicalOperatorPipeline, ir);
}

}// namespace NES::ExecutionEngine::Experimental