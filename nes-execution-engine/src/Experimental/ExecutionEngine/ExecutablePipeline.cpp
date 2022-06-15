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
#include <Experimental/ExecutionEngine/ExecutablePipeline.hpp>
#include <Experimental/ExecutionEngine/PhysicalOperatorPipeline.hpp>
#include <Experimental/Interpreter/ExecutionContext.hpp>
#include <Experimental/Runtime/RuntimeExecutionContext.hpp>
#include <Experimental/NESIR/Types/StampFactory.hpp>

namespace NES::ExecutionEngine::Experimental {

ExecutablePipeline::ExecutablePipeline(std::shared_ptr<Runtime::Execution::RuntimePipelineContext> executionContext,
                                       std::shared_ptr<PhysicalOperatorPipeline> physicalOperatorPipeline,
                                       std::unique_ptr<mlir::ExecutionEngine> engine)
    : executionContext(std::move(executionContext)), physicalOperatorPipeline(physicalOperatorPipeline),
      engine(std::move(engine)) {}

void ExecutablePipeline::execute(Runtime::WorkerContext& workerContext, Runtime::TupleBuffer& buffer) {
    auto runtimeExecutionContext = Runtime::Execution::RuntimeExecutionContext(&workerContext, executionContext.get());
    auto function = (void (*)(void*, void*)) engine->lookup("execute").get();
    function((void*) &runtimeExecutionContext, std::addressof(buffer));
}

void ExecutablePipeline::setup() {
    auto runtimeExecutionContext = Runtime::Execution::RuntimeExecutionContext(nullptr, executionContext.get());
    auto runtimeExecutionContextRef = Interpreter::Value<Interpreter::MemRef>(
        std::make_unique<Interpreter::MemRef>(Interpreter::MemRef((int8_t*) &runtimeExecutionContext)));
    runtimeExecutionContextRef.ref = Trace::ValueRef(INT32_MAX, 3, IR::Types::StampFactory::createAddressStamp());
    auto ctx = Interpreter::RuntimeExecutionContext(runtimeExecutionContextRef);
    physicalOperatorPipeline->getRootOperator()->setup(ctx);
}

std::shared_ptr<Runtime::Execution::RuntimePipelineContext> ExecutablePipeline::getExecutionContext() { return executionContext; }

}// namespace NES::ExecutionEngine::Experimental