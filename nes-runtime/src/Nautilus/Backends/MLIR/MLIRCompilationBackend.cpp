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

#include <Nautilus/Backends/MLIR/JITCompiler.hpp>
#include <Nautilus/Backends/MLIR/LLVMIROptimizer.hpp>
#include <Nautilus/Backends/MLIR/MLIRCompilationBackend.hpp>
#include <Nautilus/Backends/MLIR/MLIRExecutable.hpp>
#include <Nautilus/Backends/MLIR/MLIRLoweringProvider.hpp>
#include <Nautilus/Backends/MLIR/MLIRPassManager.hpp>
#include <Nautilus/IR/IRGraph.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Timer.hpp>
#include <mlir/IR/MLIRContext.h>

namespace NES::Nautilus::Backends::MLIR {

//Singleton
[[maybe_unused]] static CompilationBackendRegistry::Add<MLIRCompilationBackend> mlirCompilerBackend("MLIR");

std::unique_ptr<Executable> MLIRCompilationBackend::compile(std::shared_ptr<IR::IRGraph> ir,
                                                            const CompilationOptions& options,
                                                            const DumpHelper& dumpHelper) {
    auto timer = Timer<>("CompilationBasedPipelineExecutionEngine");
    timer.start();

    // 1. Create the MLIRLoweringProvider and lower the given NESIR. Return an MLIR module.
    mlir::MLIRContext context;
    auto loweringProvider = std::make_unique<MLIR::MLIRLoweringProvider>(context);
    auto mlirModule = loweringProvider->generateModuleFromIR(ir);

    // 2.a dump MLIR to console or a file
    if (options.isDumpToConsole() || options.isDumpToFile()) {
        mlir::OpPrintingFlags flags;
        std::string result;
        auto output = llvm::raw_string_ostream(result);
        mlirModule->print(output, flags);
        dumpHelper.dump("3. MLIR.mlir", result);
    }

    // 2.b Take the MLIR module from the MLIRLoweringProvider and apply lowering and optimization passes.
    if (MLIR::MLIRPassManager::lowerAndOptimizeMLIRModule(mlirModule, {}, {})) {
        NES_FATAL_ERROR("Could not lower and optimize MLIR");
    }

    // 3. Lower MLIR module to LLVM IR and create LLVM IR optimization pipeline.
    auto optPipeline = MLIR::LLVMIROptimizer::getLLVMOptimizerPipeline(options, dumpHelper);

    // 4. JIT compile LLVM IR module and return engine that provides access compiled execute function.
    auto engine = MLIR::JITCompiler::jitCompileModule(mlirModule,
                                                      optPipeline,
                                                      loweringProvider->getJitProxyFunctionSymbols(),
                                                      loweringProvider->getJitProxyTargetAddresses(),
                                                      options,
                                                      dumpHelper);

    // 5. Get execution function from engine. Create and return execution context.
    timer.snapshot("MLIRGeneration");
    return std::make_unique<MLIRExecutable>(std::move(engine));
}

}// namespace NES::Nautilus::Backends::MLIR