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
#include <Nautilus/Util/CompilationOptions.hpp>
#include <Util/DumpHelper.hpp>
#include <Util/Logger/Logger.hpp>
#include <filesystem>
#include <fstream>
#include <llvm/ExecutionEngine/Orc/JITTargetMachineBuilder.h>
#include <llvm/IR/Attributes.h>
#include <llvm/IRReader/IRReader.h>
#include <llvm/Linker/Linker.h>
#include <llvm/Support/FileCollector.h>
#include <memory>
#include <mlir/ExecutionEngine/OptUtils.h>
namespace NES::Nautilus::Backends::MLIR {

std::function<llvm::Error(llvm::Module*)> LLVMIROptimizer::getLLVMOptimizerPipeline(const CompilationOptions& options,
                                                                                    const DumpHelper& dumpHelper) {
    // Return LLVM optimizer pipeline.
    return [options, dumpHelper](llvm::Module* llvmIRModule) {
        // Currently, we do not increase the sizeLevel requirement of the optimizingTransformer beyond 0.
        constexpr int SIZE_LEVEL = 0;
        // Create A target-specific target machine for the host
        auto tmBuilderOrError = llvm::orc::JITTargetMachineBuilder::detectHost();
        NES_ASSERT2_FMT(tmBuilderOrError, "Failed to create a JITTargetMachineBuilder for the host");
        auto targetMachine = tmBuilderOrError->createTargetMachine();
        llvm::TargetMachine* targetMachinePtr = targetMachine->get();
        targetMachinePtr->setOptLevel(llvm::CodeGenOpt::Level::Aggressive);

        // Add target-specific attributes to the 'execute' function.
        llvmIRModule->getFunction("execute")->addAttributeAtIndex(
            ~0,
            llvm::Attribute::get(llvmIRModule->getContext(), "target-cpu", targetMachinePtr->getTargetCPU()));
        llvmIRModule->getFunction("execute")->addAttributeAtIndex(
            ~0,
            llvm::Attribute::get(llvmIRModule->getContext(), "target-features", targetMachinePtr->getTargetFeatureString()));
        llvmIRModule->getFunction("execute")->addAttributeAtIndex(
            ~0,
            llvm::Attribute::get(llvmIRModule->getContext(), "tune-cpu", targetMachinePtr->getTargetCPU()));
        llvm::SMDiagnostic Err;

        // Load LLVM IR module from proxy inlining input path (We assert that it exists in CompilationOptions).
        if (options.isProxyInlining()) {
            auto proxyFunctionsIR = llvm::parseIRFile(options.getProxyInliningInputPath(), Err, llvmIRModule->getContext());
            // Link the module with our generated LLVM IR module and optimize the linked LLVM IR module (inlining happens during optimization).
            llvm::Linker::linkModules(*llvmIRModule, std::move(proxyFunctionsIR), llvm::Linker::Flags::OverrideFromSrc);
        }
        auto optPipeline = mlir::makeOptimizingTransformer(options.getOptimizationLevel(), SIZE_LEVEL, targetMachinePtr);
        auto optimizedModule = optPipeline(llvmIRModule);

        // Print debug information to file/console if set in options.
        if (options.isDumpToConsole() || options.isDumpToFile()) {
            // Write the llvmIRModule to a string.
            std::string llvmIRString;
            llvm::raw_string_ostream llvmStringStream(llvmIRString);
            llvmIRModule->print(llvmStringStream, nullptr);
            auto* basicError = new std::error_code();

            // Dump the generated llvmIRModule.
            dumpHelper.dump(options.getIdentifier(), llvmIRString);
        }
        return optimizedModule;
    };
}
}// namespace NES::Nautilus::Backends::MLIR