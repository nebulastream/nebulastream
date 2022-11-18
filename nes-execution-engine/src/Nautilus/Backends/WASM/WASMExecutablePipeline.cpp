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
#include <Nautilus/Backends/WASM/WASMExecutablePipeline.hpp>
//#include <wasmtime.hh>

namespace NES::Nautilus::Backends::WASM {

WASMExecutablePipeline::WASMExecutablePipeline(std::shared_ptr<Runtime::Execution::RuntimePipelineContext> executionContext,
                           std::shared_ptr<ExecutionEngine::Experimental::PhysicalOperatorPipeline> physicalOperatorPipeline,
                           std::unique_ptr<mlir::ExecutionEngine> engine)
    : ExecutionEngine::Experimental::ExecutablePipeline(executionContext, physicalOperatorPipeline), engine(std::move(engine)) {}

void WASMExecutablePipeline::setup() {
    //wasm_engine_t *engine = wasm_engine_new();
}

void WASMExecutablePipeline::execute(Runtime::WorkerContext& workerContext, Runtime::TupleBuffer& buffer) {
    auto runtimeExecutionContext = Runtime::Execution::RuntimeExecutionContext(&workerContext, executionContext.get());
    auto function = (void (*)(void*, void*)) engine->lookup("execute").get();
    function((void*) &runtimeExecutionContext, std::addressof(buffer));
}

}// namespace NES::Nautilus::Backends::WASM