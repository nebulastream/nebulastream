#include "InliningUtils.h"
#include "llvm/Bitcode/BitcodeWriter.h"
#include "llvm/IR/DIBuilder.h"
#include "llvm/IR/Function.h"
#include "llvm/IR/Module.h"
#include "llvm/IR/PassManager.h"
#include "llvm/IR/Type.h"
#include "llvm/Passes/PassBuilder.h"
#include "llvm/Passes/PassPlugin.h"
#include "llvm/Support/raw_ostream.h"
#include "llvm/Transforms/Utils/Cloning.h"
#include "llvm/Transforms/Utils/ModuleUtils.h"
#include <iostream>

using namespace llvm;

class NautilusInlineRegistrationPass : public llvm::PassInfoMixin<NautilusInlineRegistrationPass> {
public:
	llvm::PreservedAnalyses run(llvm::Module &M, llvm::ModuleAnalysisManager &MAM);
};


void annotationsToAttributes(Module &M);
Function *findBitcodeRegistrationFunction(Module &M);
Function *findSymbolRegistrationFunction(Module &M);


PreservedAnalyses NautilusInlineRegistrationPass::run(Module &M, ModuleAnalysisManager &MAM) {
    auto &ctx = M.getContext();

    // Find registration functions
    auto *bitcodeRegistrationFunction = findBitcodeRegistrationFunction(M);
    auto *symbolRegistrationFunction = findSymbolRegistrationFunction(M);
    if (bitcodeRegistrationFunction == nullptr || symbolRegistrationFunction == nullptr)
    {
        return PreservedAnalyses::all();
    }

    // Convert clang annotations to function attributes that are directly connected to a function
    annotationsToAttributes(M);

    Function *ctor = nullptr;
    std::shared_ptr<IRBuilder<>> ctorBuilder;
    for (auto &F : M) {
        if (!F.isDeclaration()) {

            // Check whether a function has the inline tag
            bool toInline = false;
            for (auto &attr : F.getAttributes().getFnAttrs()) {
                if (attr.getKindAsString().starts_with("naut_inline"))
                {
                    toInline = true;
                }
            }
            if (toInline)
            {
                // Create initializer function with single basic block
                // This will be executed when the program starts
                if (!ctor) {
                	ctor = Function::Create(
						FunctionType::get(Type::getVoidTy(ctx), false),
						GlobalValue::InternalLinkage,
						M.getName() + ".ir_ctor",
						&M);
                	auto *BB = BasicBlock::Create(ctx, "entry", ctor);
                	ctorBuilder = std::make_shared<IRBuilder<>>(BB);
                }

                // Serialize function to LLVM IR bitcode
                auto result = serializeFunctionWithDependencySymbols(F);

                // Insert registry calls into the initializer function to populate them at runtime
				insertBitcodeRegistryCall(ctorBuilder, bitcodeRegistrationFunction, F, std::get<0>(result));
                insertSymbolRegistryCalls(ctorBuilder, symbolRegistrationFunction, std::get<1>(result));
            }
        }
    }
    // finish initializer function
    if (ctor) {
    	ctorBuilder->CreateRetVoid();
    	appendToGlobalCtors(M, ctor, 65535);
    }
    return PreservedAnalyses::all();
}

// Helper functions

void annotationsToAttributes(Module &M)
{
    auto global_annos = M.getNamedGlobal("llvm.global.annotations");
    if (global_annos) {
        if (auto *ca = dyn_cast<ConstantArray>(global_annos->getOperand(0))) {
            for (unsigned i = 0; i < ca->getNumOperands(); i++) {
                if (auto *cs = dyn_cast<ConstantStruct>(ca->getOperand(i))) {
                    if (auto *fn = dyn_cast<Function>(cs->getOperand(0)->stripPointerCasts())) {
                        if (auto *annoGV = dyn_cast<GlobalVariable>(cs->getOperand(1)->stripPointerCasts())) {
                            if (auto *cda = dyn_cast<ConstantDataArray>(annoGV->getInitializer())) {
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



Function *findBitcodeRegistrationFunction(Module &M) {
    for (auto &F : M) {
        if (F.getName().contains("registerBitcodePleaseIgnoreThisThanks")) {
            return &F;
        }
    }
    return nullptr;
}

Function *findSymbolRegistrationFunction(Module &M) {
	for (auto &F : M) {
		if (F.getName().contains("registerSymbolPleaseIgnoreThisThanks")) {
			return &F;
		}
	}
	return nullptr;
}

extern "C" LLVM_ATTRIBUTE_WEAK LLVM_ATTRIBUTE_VISIBILITY_DEFAULT
PassPluginLibraryInfo llvmGetPassPluginInfo() {
    return {
        LLVM_PLUGIN_API_VERSION, "NautilusInlineRegistrationPass", "0.0.1",
        [](PassBuilder &PB) {
            PB.registerPipelineStartEPCallback(
                [](ModulePassManager &MPM, auto) {
                    MPM.addPass(NautilusInlineRegistrationPass{});
                });
        }
    };
}




