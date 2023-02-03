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

// TODO consider removing this singleton
// this makes nes crash if the logger singleton is destroyed before MLIRCompilationBackend object as its dtor prints
[[maybe_unused]] static CompilationBackendRegistry::Add<WASMCompilationBackend> wasmCompilerBackend("WASM");

std::unique_ptr<Executable> WASMCompilationBackend::compile(std::shared_ptr<IR::IRGraph> ir) {
    auto timer = Timer<>("CompilationBasedPipelineExecutionEngine");
    timer.start();

    // 1. Create the WASMLoweringProvider and lower the given NESIR. Return a WASM module.
    auto loweringProvider = std::make_unique<WASMCompiler>();
    auto module = loweringProvider->generateModuleFromIR(ir);

    // 2. Take the MLIR module from the MLIRLoweringProvider and apply lowering and optimization passes.
    if (MLIR::MLIRPassManager::lowerAndOptimizeMLIRModule(module, {}, {})) {
        NES_FATAL_ERROR("Could not lower and optimize MLIR");
    }

    // 3. Lower MLIR module to LLVM IR and create LLVM IR optimization pipeline.
    auto optPipeline = MLIR::LLVMIROptimizer::getLLVMOptimizerPipeline(/*inlining*/ false);

    // 4. JIT compile LLVM IR module and return engine that provides access compiled execute function.
    auto engine = MLIR::JITCompiler::jitCompileModule(module,
                                                      optPipeline,
                                                      loweringProvider->getJitProxyFunctionSymbols(),
                                                      loweringProvider->getJitProxyTargetAddresses());

    // 5. Get execution function from engine. Create and return execution context.
    auto function = (void (*)(void*, void*)) engine->lookup("execute").get();
    timer.snapshot("MLIRGeneration");
    return std::make_unique<MLIRExecutable>(std::move(engine));
}

}// namespace NES::Nautilus::Backends::WASM