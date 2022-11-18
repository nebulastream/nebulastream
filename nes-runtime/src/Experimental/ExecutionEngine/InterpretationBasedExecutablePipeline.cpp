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

#include <Experimental/ExecutionEngine/InterpretationBasedExecutablePipeline.hpp>
#include <Experimental/ExecutionEngine/PhysicalOperatorPipeline.hpp>
#include <Experimental/Interpreter/RecordBuffer.hpp>

namespace NES::ExecutionEngine::Experimental {

InterpretationBasedExecutablePipeline::InterpretationBasedExecutablePipeline(
    std::shared_ptr<Runtime::Execution::RuntimePipelineContext> pipelineContext,
    std::shared_ptr<PhysicalOperatorPipeline> physicalOperatorPipeline)
    : ExecutablePipeline(pipelineContext, physicalOperatorPipeline) {}

void InterpretationBasedExecutablePipeline::setup() {
    auto runtimeExecutionContext = Runtime::Execution::RuntimeExecutionContext(nullptr, executionContext.get());
    auto runtimeExecutionContextRef = Nautilus::Value<Nautilus::MemRef>(
        std::make_unique<Nautilus::MemRef>(Nautilus::MemRef((int8_t*) &runtimeExecutionContext)));
    runtimeExecutionContextRef.ref =
        Nautilus::Tracing::ValueRef(INT32_MAX, 3, Nautilus::IR::Types::StampFactory::createAddressStamp());
    auto ctx = Nautilus::RuntimeExecutionContext(runtimeExecutionContextRef);
    physicalOperatorPipeline->getRootOperator()->setup(ctx);
}
void InterpretationBasedExecutablePipeline::execute(NES::Runtime::WorkerContext& workerContext,
                                                    NES::Runtime::TupleBuffer& buffer) {
    auto runtimeExecutionContext = Runtime::Execution::RuntimeExecutionContext(&workerContext, executionContext.get());
    auto runtimeExecutionContextRef = Nautilus::Value<Nautilus::MemRef>(
        std::make_unique<Nautilus::MemRef>(Nautilus::MemRef((int8_t*) &runtimeExecutionContext)));
    auto ctx = Nautilus::RuntimeExecutionContext(runtimeExecutionContextRef);
    auto bufferRef =
        Nautilus::Value<Nautilus::MemRef>(std::make_unique<Nautilus::MemRef>(Nautilus::MemRef((int8_t*) std::addressof(buffer))));
    auto rBuffer = Nautilus::RecordBuffer(bufferRef);
    physicalOperatorPipeline->getRootOperator()->open(ctx, rBuffer);
    physicalOperatorPipeline->getRootOperator()->close(ctx, rBuffer);
}
}// namespace NES::ExecutionEngine::Experimental