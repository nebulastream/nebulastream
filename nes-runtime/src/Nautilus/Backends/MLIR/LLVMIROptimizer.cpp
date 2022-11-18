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

#include <Nautilus/Backends/MLIR/LLVMIROptimizer.hpp>
#include <iostream>
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

namespace NES::Nautilus::Backends::MLIR {

llvm::function_ref<llvm::Error(llvm::Module*)> LLVMIROptimizer::getLLVMOptimizerPipeline(bool linkProxyFunctions) {
    // Return LLVM optimizer pipeline.
    if (linkProxyFunctions) {
        return [](llvm::Module* llvmIRModule) mutable {
            llvm::SMDiagnostic Err;
            auto proxyFunctionsIR = llvm::parseIRFile(std::string(PROXY_FUNCTIONS_RESULT_DIR), Err, llvmIRModule->getContext());
            llvm::Linker::linkModules(*llvmIRModule, std::move(proxyFunctionsIR));

            auto optPipeline = mlir::makeOptimizingTransformer(3, 3, nullptr);
            auto optimizedModule = optPipeline(llvmIRModule);

            std::string llvmIRString;
            llvm::raw_string_ostream llvmStringStream(llvmIRString);
            llvmIRModule->print(llvmStringStream, nullptr);

            auto* basicError = new std::error_code();
            llvm::raw_fd_ostream fileStream("../../../../llvm-ir/nes-runtime_opt/generated.ll", *basicError);
            fileStream.write(llvmIRString.c_str(), llvmIRString.length());
            return optimizedModule;
        };
    } else {
        return [](llvm::Module* llvmIRModule) mutable {
            auto optPipeline = mlir::makeOptimizingTransformer(0, 0, nullptr);
            auto optimizedModule = optPipeline(llvmIRModule);
            llvmIRModule->print(llvm::outs(), nullptr);
            return optimizedModule;
        };
    }
}
}// namespace NES::Nautilus::Backends::MLIR