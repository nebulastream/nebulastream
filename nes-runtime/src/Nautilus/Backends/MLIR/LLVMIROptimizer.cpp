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
#include <llvm/IR/Attributes.h>
#include <llvm/IRReader/IRReader.h>
#include <llvm/Linker/Linker.h>
#include <llvm/Support/FileCollector.h>
#include <llvm/ExecutionEngine/Orc/JITTargetMachineBuilder.h>
#include <mlir/ExecutionEngine/OptUtils.h>
namespace NES::Nautilus::Backends::MLIR {

llvm::function_ref<llvm::Error(llvm::Module*)> LLVMIROptimizer::getLLVMOptimizerPipeline(const bool linkProxyFunctions,
                                                                                         const CompilationOptions&) {
    // Return LLVM optimizer pipeline.
    // Todo in issue #3709 we aim to: 
    // - make inlining configurable inside of the lambda function, which gets rid of `if (linkProxyFunctions) { ... }`
    // - remove the dependency on PROXY_FUNCTIONS_RESULT_DIR and replace it with a
    // - make dumping the generated code optional.
    // - make optimization configurable
    // Write the linked and optimized module to an output file.
    if (linkProxyFunctions) {
        return [](llvm::Module* llvmIRModule) mutable {
            auto tmBuilderOrError = llvm::orc::JITTargetMachineBuilder::detectHost();
            if (!tmBuilderOrError) {
                llvm::errs() << "Failed to create a JITTargetMachineBuilder for the host\n";
            }
            auto targetMachine = tmBuilderOrError->createTargetMachine();
            llvm::TargetMachine *targetMachinePtr = targetMachine->get();
            targetMachinePtr->setOptLevel(llvm::CodeGenOpt::Level::Aggressive);

            // Add target-specific attributes to the 'execute' function.
            llvmIRModule->getFunction("execute")->addAttributeAtIndex(~0, 
                llvm::Attribute::get(llvmIRModule->getContext(), "target-cpu", targetMachinePtr->getTargetCPU())
                );
            llvmIRModule->getFunction("execute")->addAttributeAtIndex(~0, 
                llvm::Attribute::get(llvmIRModule->getContext(), "target-features", targetMachinePtr->getTargetFeatureString())
                );
            llvmIRModule->getFunction("execute")->addAttributeAtIndex(~0, 
                llvm::Attribute::get(llvmIRModule->getContext(), "tune-cpu", targetMachinePtr->getTargetCPU())
                );
            llvm::SMDiagnostic Err;
            auto proxyFunctionsIR = llvm::parseIRFile(
                std::string(PROXY_FUNCTIONS_RESULT_DIR) + "proxiesReduced.ll", Err, llvmIRModule->getContext());
            // Link the module with our generated LLVM IR module and optimize the linked LLVM IR module (inlining happens during optimization).
            llvm::Linker::linkModules(*llvmIRModule, std::move(proxyFunctionsIR), llvm::Linker::Flags::OverrideFromSrc);
            auto optPipeline = mlir::makeOptimizingTransformer(3, 0, targetMachinePtr);
            auto optimizedModule = optPipeline(llvmIRModule);

            // Todo issue #3709: make dumping configurable
            std::string llvmIRString;
            llvm::raw_string_ostream llvmStringStream(llvmIRString);
            llvmIRModule->print(llvmStringStream, nullptr);
            auto* basicError = new std::error_code();
            llvm::raw_fd_ostream fileStream(std::string(PROXY_FUNCTIONS_RESULT_DIR)+ "generated.ll", *basicError);
            fileStream.write(llvmIRString.c_str(), llvmIRString.length());
            return optimizedModule;
        };
    } else {
        return [](llvm::Module* llvmIRModule) {
            // Todo issue #3709: make optimization configurable
            auto optPipeline = mlir::makeOptimizingTransformer(1, 0, nullptr);
            auto optimizedModule = optPipeline(llvmIRModule);
            return optimizedModule;
        };
    }
}
}// namespace NES::Nautilus::Backends::MLIR