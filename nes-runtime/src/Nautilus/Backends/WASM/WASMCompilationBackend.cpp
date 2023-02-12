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

#include <Nautilus/Backends/WASM/WASMCompilationBackend.hpp>
#include <Nautilus/Backends/WASM/WASMCompiler.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Timer.hpp>

namespace NES::Nautilus::Backends::WASM {

//[[maybe_unused]] static CompilationBackendRegistry::Add<WASMCompilationBackend> wasmCompilerBackend("WASM");

std::unique_ptr<WAMRExecutionEngine> WASMCompilationBackend::compile(const std::shared_ptr<IR::IRGraph>& ir) {
    //auto timer = Timer<>("CompilationBasedPipelineExecutionEngine");
    //timer.start();

    // 1. Create the WASMLoweringProvider and lower the given Nautilus IR. Return a WASM module.
    auto loweringProvider = std::make_unique<WASMCompiler>();
    auto loweringResult = loweringProvider->lower(ir);

    // 2. JIT compile LLVM IR module and return engine that provides access compiled execute function.
    auto engine = std::make_unique<WAMRExecutionEngine>();
    engine->setup(loweringResult);
    // 3. Get execution function from engine. Create and return execution context.

    //timer.snapshot("WASMGeneration");
    return engine;
}

}// namespace NES::Nautilus::Backends::WASM