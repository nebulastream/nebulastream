//
// Created by ls on 25.09.23.
//

#ifndef NES_LLVMIMPORTER_H
#define NES_LLVMIMPORTER_H

#include <llvm/IR/LLVMContext.h>
#include <llvm/IR/Module.h>
#include <istream>

class LLVMImporter {
    llvm::LLVMContext context;
  public:
    std::unique_ptr<llvm::Module> importModule(const std::string &ir);
};

#endif//NES_LLVMIMPORTER_H
