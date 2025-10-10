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

#include "InliningUtils.h"
#include "llvm/Bitcode/BitcodeWriter.h"
#include "llvm/IR/DIBuilder.h"
#include "llvm/IR/Function.h"
#include "llvm/IR/Module.h"
#include "llvm/IR/PassManager.h"
#include "llvm/IR/Type.h"
#include "llvm/Passes/PassBuilder.h"
#include "llvm/Passes/PassPlugin.h"
#include "llvm/Support/CrashRecoveryContext.h"
#include "llvm/Transforms/Utils/Cloning.h"
#include "llvm/Transforms/Utils/ModuleUtils.h"

using namespace llvm;

class NautilusInlineRegistrationPass : public PassInfoMixin<NautilusInlineRegistrationPass> {
public:
	static inline PreservedAnalyses run(Module& M, ModuleAnalysisManager& MAM);
};

static void annotationsToAttributes(Module& M);
static Function* findBitcodeRegistrationFunction(Module& M);
static Function* findSymbolRegistrationFunction(Module& M);

inline PreservedAnalyses NautilusInlineRegistrationPass::run(Module& M, ModuleAnalysisManager& MAM) {
	auto& ctx = M.getContext();

	// Find registration functions
	auto* bitcodeRegistrationFunction = findBitcodeRegistrationFunction(M);
	auto* symbolRegistrationFunction = findSymbolRegistrationFunction(M);
	if (bitcodeRegistrationFunction == nullptr || symbolRegistrationFunction == nullptr) {
		return PreservedAnalyses::all();
	}

	// Convert clang annotations to function attributes that are directly connected to a function
	annotationsToAttributes(M);

	// create a copy of the original module to not break compilation in edge cases that cant be handled yet
	std::unique_ptr<Module> clonedM;

	Function* ctor = nullptr;
	std::shared_ptr<IRBuilder<>> ctorBuilder;
	for (auto& F : M) {
		if (!F.isDeclaration()) {

			// Check whether a function has the inline tag
			bool toInline = false;
			for (auto& attr : F.getAttributes().getFnAttrs()) {
				if (attr.getKindAsString().starts_with("naut_inline")) {
					toInline = true;
				}
			}
			if (toInline) {
				if (!clonedM) {
					clonedM = CloneModule(M);
				}
				// catch errors so that non-inlinable functions dont completely break compilation
				CrashRecoveryContext CRC;
				bool success = CRC.RunSafely([&] {
					// Serialize function to LLVM IR bitcode
					auto result = serializeFunctionWithDependencySymbols(F);
					if (!result.has_value()) {
						CRC.HandleExit(1);
					}

					// Create initializer function with single basic block
					// This will be executed when the program starts
					if (!ctor) {
						ctor = Function::Create(FunctionType::get(Type::getVoidTy(ctx), false),
						                        GlobalValue::InternalLinkage, M.getName() + ".ir_ctor", &M);
						auto* BB = BasicBlock::Create(ctx, "entry", ctor);
						ctorBuilder = std::make_shared<IRBuilder<>>(BB);
					}

					// map back symbol pointers from cloned module to original module
					for (auto [name, ptr] : std::get<1>(*result)) {
						ptr = M.getFunction(ptr->getName());
					}

					// Insert registry calls into the initializer function to populate them at runtime
					insertBitcodeRegistryCall(ctorBuilder, bitcodeRegistrationFunction, F, std::get<0>(*result));
					insertSymbolRegistryCalls(ctorBuilder, symbolRegistrationFunction, std::get<1>(*result));

					success = true;
				});
				if (!success) {
					errs() << "Failed to serialize inline function. To get rid of this warning, remove the NAUT_INLINE "
					          "tag from this function: "
					       << F.getName() << "\n";
				}
			}
		}
	}
	// finish initializer function
	if (ctor) {
		ctorBuilder->CreateRetVoid();
		appendToGlobalCtors(M, ctor, 65535);
	}

	// Safely call destructor of the cloned module
	CrashRecoveryContext CRC;
	bool success = CRC.RunSafely([&] { clonedM = std::make_unique<Module>("", M.getContext()); });
	if (!success) {
		errs() << "Unknown error in inlining llvm pass. There is likely a problematic function in source file "
		       << M.getName()
		       << ". Look for error messages above, or consider removing the NAUT_INLINE tag from functions that look "
		          "suspicious. When in doubt, one can safely remove the tag from all functions in this source file.\n";
	}

	return PreservedAnalyses::all();
}

// Helper functions

inline void annotationsToAttributes(Module& M) {
	auto global_annos = M.getNamedGlobal("llvm.global.annotations");
	if (global_annos) {
		if (auto* ca = dyn_cast<ConstantArray>(global_annos->getOperand(0))) {
			for (unsigned i = 0; i < ca->getNumOperands(); i++) {
				if (auto* cs = dyn_cast<ConstantStruct>(ca->getOperand(i))) {
					if (auto* fn = dyn_cast<Function>(cs->getOperand(0)->stripPointerCasts())) {
						if (auto* annoGV = dyn_cast<GlobalVariable>(cs->getOperand(1)->stripPointerCasts())) {
							if (auto* cda = dyn_cast<ConstantDataArray>(annoGV->getInitializer())) {
								auto annoStr = cda->getAsCString();
								fn->addFnAttr(annoStr);
							}
						}
					}
				}
			}
		}
	}
}

Function* findBitcodeRegistrationFunction(Module& M) {
	for (auto& F : M) {
		if (F.getName().contains("registerBitcodePleaseIgnoreThisThanks")) {
			return &F;
		}
	}
	return nullptr;
}

Function* findSymbolRegistrationFunction(Module& M) {
	for (auto& F : M) {
		if (F.getName().contains("registerSymbolPleaseIgnoreThisThanks")) {
			return &F;
		}
	}
	return nullptr;
}
