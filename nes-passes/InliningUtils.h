#ifndef NAUTILUS_INLININGUTILS_H
#define NAUTILUS_INLININGUTILS_H

#include "llvm/IR/Module.h"
#include "llvm/Passes/PassBuilder.h"


using namespace llvm;

struct SymbolInfo {
	std::string name;
	llvm::GlobalValue *ptr;
};


std::tuple<std::string, std::vector<SymbolInfo>> serializeFunctionWithDependencySymbols(Function &inlineFunction);

void insertBitcodeRegistryCall(std::shared_ptr<IRBuilder<>> builder, Function *bitcodeRegistrationFunction, Function &targetFunction, std::string &bitcodeStr);

void insertSymbolRegistryCalls(std::shared_ptr<IRBuilder<>> builder, Function *symbolRegistrationFunction, std::vector<SymbolInfo> &symbols);


#endif
