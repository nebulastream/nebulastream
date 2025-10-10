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

#pragma once

#include "CloningUtils.h"
#include "llvm/Bitcode/BitcodeWriter.h"
#include "llvm/IR/Constants.h"
#include "llvm/IR/DIBuilder.h"
#include "llvm/IR/DebugInfo.h"
#include "llvm/IR/Function.h"
#include "llvm/IR/Instructions.h"
#include "llvm/IR/Module.h"
#include "llvm/IR/Value.h"
#include "llvm/IR/Verifier.h"
#include "llvm/Passes/PassBuilder.h"
#include "llvm/Support/raw_ostream.h"
#include "llvm/Transforms/Utils/Cloning.h"
#include <memory>
#include <string>
#include <tuple>
#include <vector>
using namespace llvm;

static std::optional<std::tuple<std::string, std::vector<SymbolInfo>>>
serializeFunctionWithDependencySymbols(Function& inlineFunction);

static void insertBitcodeRegistryCall(std::shared_ptr<IRBuilder<>> builder, Function* bitcodeRegistrationFunction,
                                      Function& targetFunction, std::string& bitcodeStr);

static void insertSymbolRegistryCalls(std::shared_ptr<IRBuilder<>> builder, Function* symbolRegistrationFunction,
                                      std::vector<SymbolInfo>& symbols);

std::optional<std::tuple<std::string, std::vector<SymbolInfo>>>
serializeFunctionWithDependencySymbols(Function& inlineFunction) {
	std::vector<SymbolInfo> symbols;
	// Create module holding the extracted function and its dependencies
	Module wrapperModule = Module("func_module", inlineFunction.getContext());
    wrapperModule.setDataLayout(inlineFunction.getParent()->getDataLayout());
    wrapperModule.setTargetTriple(inlineFunction.getParent()->getTargetTriple());

	// Clone inline function to the wrapper module
	Function* clonedFunc = Function::Create(inlineFunction.getFunctionType(), inlineFunction.getLinkage(),
	                                        inlineFunction.getName(), &wrapperModule);
	ValueToValueMapTy v2vMap;
	for (auto originalIt = inlineFunction.arg_begin(), cloneIt = clonedFunc->arg_begin();
	     originalIt != inlineFunction.arg_end(); ++originalIt, ++cloneIt)
		v2vMap[&*originalIt] = &*cloneIt;
	SmallVector<ReturnInst*, 8> returnInst;

	// Shallow copy target function
	CloneFunctionInto(clonedFunc, &inlineFunction, v2vMap, CloneFunctionChangeType::DifferentModule, returnInst);
	clonedFunc->addFnAttr("is_target");
	clonedFunc->setLinkage(GlobalValue::ExternalLinkage);


	// Clone all values used by the target function and collect symbol dependencies that need to be linked during query
	// compilation
	if (!cloneValuesAndCollectDependencySymbols(inlineFunction, wrapperModule, v2vMap, symbols)) {
		clonedFunc->dropAllReferences();
		clonedFunc->deleteBody();
		return std::nullopt;
	}

	// clone personality function of the inline function if it exists (otherwise some functions cant be properly
	// inlined)
	if (inlineFunction.hasPersonalityFn()) {
	    errs() << "\n Failed to serialize inline function (IR verification failed; possibly due to use of global "
          "variables). To get rid of this warning, remove the NAUT_INLINE tag from this function: "
       << inlineFunction.getName() << "\n";
		Constant* srcPersonalityConst = inlineFunction.getPersonalityFn();
		if (auto* srcPersonality = dyn_cast<Function>(srcPersonalityConst)) {
			auto* clonedPersonality = Function::Create(srcPersonality->getFunctionType(), srcPersonality->getLinkage(),
			                                           srcPersonality->getName(), &wrapperModule);
		    clonedPersonality->copyAttributesFrom(srcPersonality);
			for (auto originalIt = srcPersonality->arg_begin(), cloneIt = clonedPersonality->arg_begin();
			     originalIt != srcPersonality->arg_end(); ++originalIt, ++cloneIt)
				v2vMap[&*originalIt] = &*cloneIt;

		    if (srcPersonality->isDeclaration())
		    {
		        clonedFunc->setLinkage(GlobalValue::ExternalLinkage);
		        symbols.push_back({clonedPersonality->getName().str(), srcPersonality});
		    } else
		    {
		        SmallVector<ReturnInst*, 8> personalityReturnInst;
		        CloneFunctionInto(clonedPersonality, srcPersonality, v2vMap, CloneFunctionChangeType::DifferentModule,
                                  personalityReturnInst);
		    }


			clonedFunc->setPersonalityFn(clonedPersonality);
			v2vMap[srcPersonality] = clonedPersonality;
		}
	}
	RemapFunction(*clonedFunc, v2vMap, RF_None);

	StripDebugInfo(wrapperModule); // to suppress some llvm warning messages for invalid debug info

	// Verify integrity of the new module
	// will automatically print the precise reasons if the verification fails, which is key for debugging functions that
	// cant be inlined yet)
	if (verifyModule(wrapperModule, &errs())) {
		errs() << "\n Failed to serialize inline function (IR verification failed; possibly due to use of global "
		          "variables). To get rid of this warning, remove the NAUT_INLINE tag from this function: "
		       << inlineFunction.getName() << "\n";
		return std::nullopt;
	}

    #if IS_ARM
    if (inlineFunction.getName().contains("etNLJ"))
    {
        llvm::outs() << wrapperModule;
    }
    #endif
	// serialize wrapperModule to bitcode string
	SmallVector<char, 0> buffer;
	raw_svector_ostream OS(buffer);
	WriteBitcodeToFile(wrapperModule, OS);
	std::string bitcodeStr(buffer.begin(), buffer.end());
	return std::make_optional(std::make_tuple(std::move(bitcodeStr), std::move(symbols)));
}

// inserts call to the bitcode registry to associate the IR bitstring with the target function
void insertBitcodeRegistryCall(std::shared_ptr<IRBuilder<>> builder, Function* bitcodeRegistrationFunction,
                               Function& targetFunction, std::string& bitcodeStr) {
	auto& ctx = builder->getContext();
	ArrayRef<uint8_t> bitcode((const uint8_t*) bitcodeStr.data(), bitcodeStr.size());

	// Create types
	auto* int8Ty = IntegerType::get(ctx, 8);
	auto* int8PtrTy = PointerType::get(int8Ty, 0);

	// Create LLVM constant that holds the bitcode string
	auto* bitcodeConstant = ConstantDataArray::get(ctx, bitcode);
	auto* bitcodeGV =
	    new GlobalVariable(*targetFunction.getParent(), bitcodeConstant->getType(), true, GlobalValue::PrivateLinkage,
	                       bitcodeConstant, targetFunction.getName() + ".bitcode");
	bitcodeGV->setUnnamedAddr(GlobalValue::UnnamedAddr::Global);
	auto* bitcodePtr =
	    builder->CreateBitCast(builder->CreateConstGEP2_32(bitcodeConstant->getType(), bitcodeGV, 0, 0), int8PtrTy);
	auto* bitcodeLen = ConstantInt::get(Type::getInt64Ty(ctx), bitcode.size());

	// Insert call to registration function
	auto* funcPtr = ConstantExpr::getBitCast(&targetFunction, int8PtrTy);
	builder->CreateCall(bitcodeRegistrationFunction, {funcPtr, bitcodePtr, bitcodeLen});
}

// inserts calls to the symbol registry to associate symbol names with function pointers
void insertSymbolRegistryCalls(std::shared_ptr<IRBuilder<>> builder, Function* symbolRegistrationFunction,
                               std::vector<SymbolInfo>& symbols) {
	LLVMContext& ctx = builder->getContext();

	// Create types
	auto* int8Ty = IntegerType::get(ctx, 8);
	auto* int8PtrTy = PointerType::get(int8Ty, 0);

	for (auto& symbol : symbols) {
		ArrayRef<uint8_t> symbolName((const uint8_t*) symbol.name.data(), symbol.name.size());

		// Create LLVM values holding the symbol names and pointers
		auto* symbolNameConstant = ConstantDataArray::get(ctx, symbolName);
		auto* symbolNameGV = new GlobalVariable(*symbolRegistrationFunction->getParent(), symbolNameConstant->getType(),
		                                        true, GlobalValue::PrivateLinkage, symbolNameConstant, symbol.name);
		symbolNameGV->setUnnamedAddr(GlobalValue::UnnamedAddr::Global);
		auto* symbolNamePtr = builder->CreateBitCast(
		    builder->CreateConstGEP2_32(symbolNameConstant->getType(), symbolNameGV, 0, 0), int8PtrTy);
		auto* symbolNameLen = ConstantInt::get(Type::getInt64Ty(ctx), symbol.name.size());

		// Insert call to registration function
		auto* symbolPtr = builder->CreateBitCast(symbol.ptr, int8PtrTy);
		builder->CreateCall(symbolRegistrationFunction, {symbolNamePtr, symbolNameLen, symbolPtr});
	}
}
