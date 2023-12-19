//
// Created by ls on 25.09.23.
//

#include "LLVMImporter.h"
#include "llvm/IRReader/IRReader.h"
#include "llvm/Support/MemoryBuffer.h"
#include "llvm/Support/SourceMgr.h"

std::unique_ptr<llvm::Module> LLVMImporter::importModule(const std::string& ir) {
    using namespace llvm;
    SMDiagnostic error;
    auto buffer = MemoryBuffer::getMemBufferCopy(ir, "module");
    auto llvmModule = parseIR(*buffer, error, context);
    if (!llvmModule) {
        error.print("LLVMImporter", errs());
        exit(1);
    }
    return llvmModule;
}