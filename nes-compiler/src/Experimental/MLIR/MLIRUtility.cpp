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

#include <Experimental/MLIR/MLIRUtility.hpp>
#include <llvm/ADT/StringMap.h>
#include <llvm/ExecutionEngine/ExecutionEngine.h>
#include <llvm/ExecutionEngine/JITSymbol.h>
#include <llvm/Support/SourceMgr.h>
#include <llvm/Support/TargetSelect.h>
#include <mlir/Conversion/SCFToStandard/SCFToStandard.h>
#include <mlir/Conversion/StandardToLLVM/ConvertStandardToLLVMPass.h>
#include <mlir/Dialect/LLVMIR/LLVMDialect.h>
#include <mlir/Dialect/SCF/SCF.h>
#include <mlir/Dialect/StandardOps/IR/Ops.h>
#include <mlir/ExecutionEngine/OptUtils.h>
#include <mlir/IR/Attributes.h>
#include <mlir/IR/PatternMatch.h>
#include <mlir/Parser.h>
#include <mlir/Pass/PassManager.h>
#include <mlir/Target/LLVMIR/Dialect/LLVMIR/LLVMToLLVMIRTranslation.h>
#include <mlir/Transforms/Passes.h>
#include <string>

namespace NES::Compiler::Experimental {
int MLIRUtility::loadModuleFromString(const std::string& mlirString) {
    // Load necessary Dialects.
    context.loadDialect<mlir::StandardOpsDialect>();
    context.loadDialect<mlir::LLVM::LLVMDialect>();
    context.loadDialect<mlir::scf::SCFDialect>();

    // Create Module from string.
    module = parseSourceString(mlirString, &context);
    if (!module) {
        llvm::errs() << "NESMLIRGenerator::loadMLIR: Could not load MLIR module" << '\n';
        return 1;
    }

    // Apply MLIR optimization and lowering passes. Lowers To LLVM Dialect, which can be lowered to LLVM IR later.
    mlir::PassManager passManager(&context);
    applyPassManagerCLOptions(passManager);
    passManager.addPass(mlir::createInlinerPass());
    passManager.addPass(mlir::createLowerToCFGPass());
    passManager.addPass(mlir::createLowerToLLVMPass());
    if (mlir::failed(passManager.run(*module))) {
        return 1;
    }
    return 0;
}

int MLIRUtility::runJit(const std::vector<std::string>& symbols, const std::vector<llvm::JITTargetAddress>& jitAddresses) {
    // Initialize LLVM targets.
    llvm::InitializeNativeTarget();
    llvm::InitializeNativeTargetAsmPrinter();
    // Register the translation from MLIR to LLVM IR, which must happen before we can JIT-compile.
    mlir::registerLLVMDialectTranslation(*module->getContext());

    // Link proxyFunctions into MLIR module. Optimize MLIR module.
    llvm::function_ref<llvm::Error(llvm::Module*)> optimizingTransformer;
    optimizingTransformer = [](llvm::Module* llvmIRModule) {
        auto optPipeline = mlir::makeOptimizingTransformer(3, 3, nullptr);
        auto optimizedModule = optPipeline(llvmIRModule);
        llvmIRModule->print(llvm::errs(), nullptr);
        return optimizedModule;
    };

    // Create an MLIR execution engine. The execution engine eagerly JIT-compiles the module.
    auto maybeEngine =
        mlir::ExecutionEngine::create(*module, nullptr, optimizingTransformer, llvm::CodeGenOpt::Level::Aggressive);
    assert(maybeEngine && "failed to construct an execution engine");
    auto& engine = maybeEngine.get();

    // SymbolMap lambda function used to register symbols with the JIT engine.
    auto runtimeSymbolMap = [&](llvm::orc::MangleAndInterner interner) {
        auto symbolMap = llvm::orc::SymbolMap();
        for (int i = 0; i < (int) symbols.size(); ++i) {
            symbolMap[interner(symbols.at(i))] = llvm::JITEvaluatedSymbol(jitAddresses.at(i), llvm::JITSymbolFlags::Callable);
        }
        return symbolMap;
    };
    engine->registerSymbols(runtimeSymbolMap);

    // Invoke the JIT-compiled function.
    int result = 0;
    auto invocationResult = engine->invoke("execute", 42, mlir::ExecutionEngine::Result<int>(result));
    printf("Result: %d\n", result);
    if (invocationResult) {
        llvm::errs() << "JIT invocation failed\n";
        return -1;
    }

    return 0;
}
}// namespace NES::Compiler::Experimental