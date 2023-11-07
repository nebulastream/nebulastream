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

#include "UnikernelExport.h"
#include "LLVMImporter.h"
#include "LLVMModuleStripper.h"
#include "QueryCompiler.h"
#include "UnikernelPipelineExport.h"
#include <llvm/ADT/None.h>
#include <llvm/IR/IRBuilder.h>
#include <llvm/IR/LegacyPassManager.h>
#include <llvm/MC/TargetRegistry.h>
#include <llvm/Support/FileSystem.h>
#include <llvm/Support/TargetSelect.h>
#include <llvm/Target/TargetMachine.h>
#include <llvm/TargetParser/Host.h>

void exportToObjectFile(llvm::Module* module, llvm::raw_fd_ostream& output) {
    llvm::InitializeNativeTarget();
    llvm::InitializeNativeTargetAsmPrinter();
    LLVMInitializeX86AsmParser();

    // Add code to populate your LLVM module here.

    // Specify the target machine and options.
    llvm::Triple triple(llvm::sys::getDefaultTargetTriple());
    std::string error;
    const llvm::Target* target = llvm::TargetRegistry::lookupTarget(triple.getTriple(), error);
    llvm::TargetOptions options;

    // Create a target machine.
    std::unique_ptr<llvm::TargetMachine> targetMachine(target->createTargetMachine(triple.getTriple(),
                                                                                   llvm::sys::getHostCPUName(),
                                                                                   "",
                                                                                   options,
                                                                                   llvm::Reloc::Model::PIC_,
                                                                                   llvm::None));

    // Generate object code from the LLVM module.
    llvm::legacy::PassManager passManager;
    if (targetMachine->addPassesToEmitFile(passManager, output, nullptr, llvm::CGFT_ObjectFile)) {
        llvm::errs() << "Target machine can't emit a file of this type";
        NES_THROW_RUNTIME_ERROR("Target machine can't emit a file of this type");
    }

    passManager.run(*module);
}

void UnikernelExport::exportPipelineStageToObjectFile(
    std::string outputPath,
    uint64_t pipelineId,
    QuerySubPlanId subPlanId,
    std::vector<EitherSharedOrLocal> handlers,
    std::unique_ptr<NES::Runtime::Execution::CompiledExecutablePipelineStage>&& stage) const {
    NES::Unikernel::UnikernelPipelineExport pipelineExport;
    llvm::LLVMContext context;
    LLVMImporter importer;
    LLVMModuleStripper stripper;

    auto generatedSource = NES::Runtime::Unikernel::OperatorHandlerTracer::generateFile(handlers, pipelineId, subPlanId);
    NES_DEBUG("Generated Source for Pipeline {}:\n{}", pipelineId, generatedSource);
    auto llvmIr = ci.compile(generatedSource);

    if (llvmIr.has_error()) {
        std::cerr << llvmIr.assume_error().message << std::endl;
        NES_THROW_RUNTIME_ERROR("Could not Compile Pipeline");
    }

    auto module = importer.importModule(llvmIr.assume_value());
    pipelineExport.exportPipelineIntoModule(*module, pipelineId, stage.get());
    std::error_code EC;
    llvm::raw_fd_ostream dest(outputPath, EC, llvm::sys::fs::OF_None);
    exportToObjectFile(module.get(), dest);
}

void UnikernelExport::exportSharedOperatorHandlersToObjectFile(
    std::string filePath,
    QuerySubPlanId subPlanId,
    const std::vector<NES::Runtime::Unikernel::OperatorHandlerDescriptor>& sharedHandlers) {

    auto generatedSource = NES::Runtime::Unikernel::OperatorHandlerTracer::generateSharedHandlerFile(sharedHandlers, subPlanId);
    NES_DEBUG("Generated Source for Shared Handlers:\n{}", generatedSource);
    auto llvmIr = ci.compileToObject(generatedSource, filePath);

    if (llvmIr.has_error()) {
        std::cerr << llvmIr.assume_error().message << std::endl;
        NES_THROW_RUNTIME_ERROR("Could not Compile Pipeline");
    }
    NES_INFO("Compiled Shared Handlers! Object File: {}", filePath);
}
