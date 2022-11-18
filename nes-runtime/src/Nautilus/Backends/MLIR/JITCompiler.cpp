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

#include <llvm/ExecutionEngine/ExecutionEngine.h>
#include <llvm/ExecutionEngine/JITSymbol.h>
#include <llvm/ExecutionEngine/Orc/Mangling.h>
#include <llvm/IRReader/IRReader.h>
#include <llvm/Linker/Linker.h>
#include <llvm/Support/CommandLine.h>
#include <llvm/Support/FileCollector.h>
#include <llvm/Support/SourceMgr.h>
#include <llvm/Support/TargetSelect.h>
#include <llvm/Transforms/IPO/SCCP.h>
#include <mlir/ExecutionEngine/OptUtils.h>
#include <mlir/Target/LLVMIR/Dialect/LLVMIR/LLVMToLLVMIRTranslation.h>
#include <mlir/Target/LLVMIR/LLVMTranslationInterface.h>

namespace NES::Nautilus::Backends::MLIR {

std::unique_ptr<mlir::ExecutionEngine>
JITCompiler::jitCompileModule(mlir::OwningOpRef<mlir::ModuleOp>& module,
                              llvm::function_ref<llvm::Error(llvm::Module*)> optPipeline,
                              const std::vector<std::string>& jitProxyFunctionSymbols,
                              const std::vector<llvm::JITTargetAddress>& jitProxyFunctionTargetAddresses) {

    // Initialize information about the local machine in LLVM.
    llvm::InitializeNativeTarget();
    llvm::InitializeNativeTargetAsmPrinter();

    // Register the translation from MLIR to LLVM IR, which must happen before we can JIT-compile.
    mlir::registerLLVMDialectTranslation(*module->getContext());

    // Create MLIR execution engine (wrapper around LLVM ExecutionEngine).
    auto maybeEngine = mlir::ExecutionEngine::create(*module, nullptr, optPipeline, llvm::CodeGenOpt::Level::Aggressive);

    assert(maybeEngine && "failed to construct an execution engine");
    auto& engine = maybeEngine.get();

    auto runtimeSymbolMap = [&](llvm::orc::MangleAndInterner interner) {
        auto symbolMap = llvm::orc::SymbolMap();
        for (int i = 0; i < (int) jitProxyFunctionSymbols.size(); ++i) {
            symbolMap[interner(jitProxyFunctionSymbols.at(i))] =
                llvm::JITEvaluatedSymbol(jitProxyFunctionTargetAddresses.at(i), llvm::JITSymbolFlags::Callable);
        }
        return symbolMap;
    };
    engine->registerSymbols(runtimeSymbolMap);

    return std::move(engine);
}
}// namespace NES::Nautilus::Backends::MLIR