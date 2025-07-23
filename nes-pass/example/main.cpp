#include <ostream>
#include <iostream>

#include "FunctionRegistry.h"
#include "nautlilus.h"
#include "lib.h"
#include <llvm/ADT/StringRef.h>
#include <llvm/Bitcode/BitcodeReader.h>
#include <llvm/Transforms/Utils/Cloning.h>
#include "llvm/Support/MemoryBuffer.h"
#include "llvm/IR/Module.h"
#include "llvm/Bitcode/BitcodeWriter.h"
#include "llvm/IR/Function.h"
#include "llvm/IR/Module.h"
#include "llvm/IR/PassManager.h"
#include "llvm/IR/Type.h"
#include "llvm/IRReader/IRReader.h"
#include "llvm/Passes/PassBuilder.h"
#include "llvm/Support/raw_ostream.h"
#include "llvm/Transforms/Utils/Cloning.h"
#include "llvm/Transforms/Utils/ModuleUtils.h"
#include "llvm/Passes/PassPlugin.h"

NAUT_INLINE int target(int i)
{
    return i+1;
}


std::unique_ptr<llvm::Module> loadBitcodeFromString(const std::string& bitcodeStr) {
    std::size_t hashValue = std::hash<std::string>{}(bitcodeStr);

    std::cout << "Hash: " << hashValue << "\n";
    llvm::LLVMContext context;
    auto buffer = llvm::MemoryBuffer::getMemBuffer(
        llvm::StringRef(bitcodeStr.data(), bitcodeStr.size()), "", false);

    std::cout << "Starting to load bitcode"<< std::endl;

    llvm::Expected<std::unique_ptr<llvm::Module>> moduleOrErr = llvm::parseBitcodeFile(buffer->getMemBufferRef(), context);
    if (!moduleOrErr) {
        logAllUnhandledErrors(moduleOrErr.takeError(), llvm::errs(), "Bitcode parse error: ");
        return nullptr;
    }

    return std::move(*moduleOrErr);
}


int main()
{
    invoke(target);
    //invoke(libTargetInline);
    invoke(libTargetNoInline);
    loadBitcodeFromString(InlineFunctionRegistry::instance().getMetadata((void *) target));
}


