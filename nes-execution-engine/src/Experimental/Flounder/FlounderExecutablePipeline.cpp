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

#include <Experimental/ExecutionEngine/PhysicalOperatorPipeline.hpp>
#include <Experimental/Flounder/FlounderExecutablePipeline.hpp>

namespace NES::ExecutionEngine::Experimental {

FlounderExecutablePipeline::FlounderExecutablePipeline(
    std::shared_ptr<Runtime::Execution::RuntimePipelineContext> executionContext,
    std::shared_ptr<PhysicalOperatorPipeline> physicalOperatorPipeline,
    std::unique_ptr<flounder::Executable> engine)
    : ExecutablePipeline(executionContext, physicalOperatorPipeline), engine(std::move(engine)) {}

void FlounderExecutablePipeline::setup() {
    auto runtimeExecutionContext = Runtime::Execution::RuntimeExecutionContext(nullptr, executionContext.get());
    auto runtimeExecutionContextRef = Interpreter::Value<Interpreter::MemRef>(
        std::make_unique<Interpreter::MemRef>(Interpreter::MemRef((int8_t*) &runtimeExecutionContext)));
    runtimeExecutionContextRef.ref = Trace::ValueRef(INT32_MAX, 3, IR::Types::StampFactory::createAddressStamp());
    auto ctx = Interpreter::RuntimeExecutionContext(runtimeExecutionContextRef);
    physicalOperatorPipeline->getRootOperator()->setup(ctx);
}
void FlounderExecutablePipeline::execute(NES::Runtime::WorkerContext& workerContext, NES::Runtime::TupleBuffer& buffer) {
    auto runtimeExecutionContext = Runtime::Execution::RuntimeExecutionContext(&workerContext, executionContext.get());
    engine->execute<>((void*) &runtimeExecutionContext, std::addressof(buffer));
}
}// namespace NES::ExecutionEngine::Experimental