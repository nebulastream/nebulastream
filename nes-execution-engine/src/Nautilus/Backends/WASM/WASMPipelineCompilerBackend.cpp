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
#include <Nautilus/Backends/WASM/WASMPipelineCompilerBackend.hpp>
#include <Nautilus/Backends/WASM/WASMCompiler.hpp>

namespace NES::Nautilus::Backends::WASM {

std::shared_ptr<ExecutablePipeline>
WASMPipelineCompilerBackend::compile(std::shared_ptr<Runtime::Execution::RuntimePipelineContext> executionContext,
                                     std::shared_ptr<PhysicalOperatorPipeline> physicalOperatorPipeline,
                                     std::shared_ptr<IR::IRGraph> ir) {
    auto tmp = executionContext.get();
    auto tmp2 = physicalOperatorPipeline->getRootOperator();
    auto wasmCompiler = std::make_unique<WASMCompiler>();
    auto wasm = wasmCompiler->lower(ir);
    return nullptr;
}

}// namespace NES::Nautilus::Backends::WASM