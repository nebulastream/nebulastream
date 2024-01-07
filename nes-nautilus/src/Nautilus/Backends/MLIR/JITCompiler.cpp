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
#include <Nautilus/Backends/MLIR/MLIRLoweringProvider.hpp>
#include <mlir/ExecutionEngine/OptUtils.h>
#include <mlir/Target/LLVMIR/Dialect/LLVMIR/LLVMToLLVMIRTranslation.h>
#include <mlir/Target/LLVMIR/Export.h>
namespace NES::Nautilus::Backends::MLIR {

void dumpLLVMIR(mlir::ModuleOp mlirModule, const CompilationOptions& compilerOptions, const DumpHelper& dumpHelper) {
    // Convert the module to LLVM IR in a new LLVM IR context.
    llvm::LLVMContext llvmContext;
    auto llvmModule = mlir::translateModuleToLLVMIR(mlirModule, llvmContext);
    if (!llvmModule) {
        llvm::errs() << "Failed to emit LLVM IR\n";
        return;
    }

    mlir::ExecutionEngine::setupTargetTriple(llvmModule.get());

    /// Optionally run an optimization pipeline over the llvm module.
    auto optPipeline = mlir::makeOptimizingTransformer(
        /*optLevel=*/compilerOptions.isOptimize() ? 3 : 0,
        /*sizeLevel=*/0,
        /*targetMachine=*/nullptr);
    if (auto err = optPipeline(llvmModule.get())) {
        llvm::errs() << "Failed to optimize LLVM IR " << err << "\n";
        return;
    }
    std::string result;
    auto resultStream = llvm::raw_string_ostream(result);
    llvmModule->print(resultStream, nullptr);
    dumpHelper.dump("4. AfterLLVMGeneration.ll", result);
}

std::unique_ptr<mlir::ExecutionEngine>
JITCompiler::jitCompileModule(mlir::OwningOpRef<mlir::ModuleOp>& mlirModule,
                              const llvm::function_ref<llvm::Error(llvm::Module*)> optPipeline,
                              const std::vector<std::string>& jitProxyFunctionSymbols,
                              const std::vector<llvm::JITTargetAddress>& jitProxyFunctionTargetAddresses,
                              const CompilationOptions& compilerOptions,
                              const DumpHelper& dumpHelper) {
    // Initialize information about the local machine in LLVM.
    LLVMInitializeNativeTarget();
    LLVMInitializeNativeAsmPrinter();
    // Register the translation from MLIR to LLVM IR, which must happen before we can JIT-compile.
    mlir::registerLLVMDialectTranslation(*mlirModule->getContext());

    if (compilerOptions.isDumpToConsole() || compilerOptions.isDumpToFile()) {
        dumpLLVMIR(mlirModule.get(), compilerOptions, dumpHelper);
    }

    // Create MLIR execution engine (wrapper around LLVM ExecutionEngine).
    mlir::ExecutionEngineOptions options;
    options.jitCodeGenOptLevel = llvm::CodeGenOpt::Level::Aggressive;
    options.transformer = optPipeline;
    auto maybeEngine = mlir::ExecutionEngine::create(*mlirModule, options);
    assert(maybeEngine && "failed to construct an execution engine");

    // TODO in issue #3710 we aim to add a proxy function catalog that contains the information on all proxy functions.
    // right now, we have to statically list all proxy functions here, and in 'ExtractFunctionsFromLLVMIR.cpp'.
    const std::unordered_set<std::string> ProxyInliningFunctions{"NES__Runtime__TupleBuffer__getNumberOfTuples",
                                                                 "NES__Runtime__TupleBuffer__setNumberOfTuples",
                                                                 "NES__Runtime__TupleBuffer__getBuffer",
                                                                 "NES__Runtime__TupleBuffer__getBufferSize",
                                                                 "NES__Runtime__TupleBuffer__getWatermark",
                                                                 "NES__Runtime__TupleBuffer__setWatermark",
                                                                 "NES__Runtime__TupleBuffer__getCreationTimestampInMS",
                                                                 "NES__Runtime__TupleBuffer__setSequenceNumber",
                                                                 "NES__Runtime__TupleBuffer__getSequenceNumber",
                                                                 "NES__Runtime__TupleBuffer__setCreationTimestampInMS"};
    // We register all external functions (symbols) that we do not inline.
    const auto runtimeSymbolMap = [&](llvm::orc::MangleAndInterner interner) {
        auto symbolMap = llvm::orc::SymbolMap();
        for (int i = 0; i < (int) jitProxyFunctionSymbols.size(); ++i) {
            if (!(compilerOptions.isProxyInlining() && ProxyInliningFunctions.contains(jitProxyFunctionSymbols.at(i)))) {
                symbolMap[interner(jitProxyFunctionSymbols.at(i))] =
                    llvm::JITEvaluatedSymbol(jitProxyFunctionTargetAddresses.at(i), llvm::JITSymbolFlags::Callable);
            }
        }
        return symbolMap;
    };
    auto& engine = maybeEngine.get();
    engine->registerSymbols(runtimeSymbolMap);
    return std::move(engine);
}
}// namespace NES::Nautilus::Backends::MLIR