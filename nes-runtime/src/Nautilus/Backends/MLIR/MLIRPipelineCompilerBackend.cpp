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

#include <Nautilus/Backends/MLIR/MLIRLoweringProvider.hpp>
#include <Nautilus/Backends/MLIR/MLIRPassManager.hpp>
#include <Nautilus/Backends/MLIR/LLVMIROptimizer.hpp>
#include <Nautilus/Backends/MLIR/JITCompiler.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Timer.hpp>
#include <Nautilus/Backends/MLIR/MLIRExecutablePipeline.hpp>
#include <Nautilus/Backends/MLIR/MLIRPipelineCompilerBackend.hpp>
#include <mlir/IR/MLIRContext.h>

using namespace NES::ExecutionEngine::Experimental;
namespace NES::Nautilus::Backends::MLIR {

std::shared_ptr<ExecutablePipeline>
MLIRPipelineCompilerBackend::compile(std::shared_ptr<Runtime::Execution::RuntimePipelineContext> executionContext,
                                     std::shared_ptr<PhysicalOperatorPipeline> physicalOperatorPipeline,
                                     std::shared_ptr<IR::IRGraph> ir) {
    Timer timer("CompilationBasedPipelineExecutionEngine");
    timer.start();

    // 1. Create the MLIRLoweringProvider and lower the given NESIR. Return an MLIR module.
    mlir::MLIRContext context;
    auto loweringProvider = std::make_unique<MLIR::MLIRLoweringProvider>(context);
    auto module = loweringProvider->generateModuleFromIR(ir);

    // 2. Take the MLIR module from the MLIRLoweringProvider and apply lowering and optimization passes.
    if(MLIR::MLIRPassManager::lowerAndOptimizeMLIRModule(module, {}, {})) {
        NES_FATAL_ERROR("Could not lower and optimize MLIR");
    }

    // 3. Lower MLIR module to LLVM IR and create LLVM IR optimization pipeline.
    auto optPipeline =MLIR::LLVMIROptimizer::getLLVMOptimizerPipeline(/*inlining*/ false);

    // 4. JIT compile LLVM IR module and return engine that provides access compiled execute function.
    auto engine = MLIR::JITCompiler::jitCompileModule(module, optPipeline, 
        loweringProvider->getJitProxyFunctionSymbols(), loweringProvider->getJitProxyTargetAddresses());

    // 5. Get execution function from engine. Create and return execution context.
    auto function = (void (*)(void*, void*)) engine->lookup("execute").get();
    timer.snapshot("MLIRGeneration");
    auto exec = std::make_shared<MLIRExecutablePipeline>(executionContext, physicalOperatorPipeline, std::move(engine));
    timer.snapshot("MLIR compile");
    timer.pause();
    NES_INFO("MLIRPipelineCompilerBackend TIME: " << timer);
    return exec;
}
}// namespace NES::ExecutionEngine::Experimental