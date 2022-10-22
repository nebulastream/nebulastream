#include <Nautilus/Backends/MLIR/MLIRExecutable.hpp>
#include <Nautilus/Backends/MLIR/JITCompiler.hpp>
#include <Nautilus/Backends/MLIR/LLVMIROptimizer.hpp>
#include <Nautilus/Backends/MLIR/MLIRCompilationBackend.hpp>
#include <Nautilus/Backends/MLIR/MLIRLoweringProvider.hpp>
#include <Nautilus/Backends/MLIR/MLIRPassManager.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Timer.hpp>
#include <mlir/IR/MLIRContext.h>

namespace NES::Nautilus::Backends::MLIR {

std::unique_ptr<Executable> MLIRCompilationBackend::compile(std::shared_ptr<IR::IRGraph> ir) {
    auto timer = Timer<>("CompilationBasedPipelineExecutionEngine");
    timer.start();

    // 1. Create the MLIRLoweringProvider and lower the given NESIR. Return an MLIR module.
    mlir::MLIRContext context;
    auto loweringProvider = std::make_unique<MLIR::MLIRLoweringProvider>(context);
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
}// namespace NES::Nautilus::Backends::MLIR