#include <iostream>
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
#include <llvm/ADT/StringRef.h>
#include <llvm/Bitcode/BitcodeReader.h>
#include <llvm/Transforms/Utils/Cloning.h>
#include "llvm/Support/MemoryBuffer.h"
#include "llvm/IR/Verifier.h"

using namespace llvm;

void annotationsToAttributes(Module &M);
std::string serialize(Function &F, LLVMContext &ctx);
Function *findRegistrationFunction(Module &M);


class NautilusInlineRegistrationPass : public PassInfoMixin<NautilusInlineRegistrationPass> {
public:
    PreservedAnalyses run(Module &M, ModuleAnalysisManager &MAM) {
        LLVMContext &ctx = M.getContext();

        Function *registrationFunction = findRegistrationFunction(M);
        if (registrationFunction == nullptr)
        {
            return PreservedAnalyses::all();
        }

        annotationsToAttributes(M);

        llvm::outs() << "Running Inline Pass\n";

        for (Function &F : M) {
            if (!F.isDeclaration()) {
                bool toInline = false;
                for (const auto &Attr : F.getAttributes().getFnAttrs()) {
                    if (Attr.getKindAsString() == "naut_inline")
                    {
                        toInline = true;
                        llvm::outs() << "Found inline function: " << F.getName() << "\n";
                    }
                }
                if (toInline)
                {
                    // Create initializer function with single basic block
                    Function *ctor = Function::Create(
                    FunctionType::get(Type::getVoidTy(ctx), false),
                    GlobalValue::InternalLinkage,
                    F.getName() + ".ir_ctor",
                    &M);
                    BasicBlock *BB = BasicBlock::Create(ctx, "entry", ctor);
                    IRBuilder<> B(BB);

                    // Serialize function to LLVM IR bitcode
                    auto bitcodeStr = serialize(F, ctx);
                    ArrayRef<uint8_t> bitcode((const uint8_t *)bitcodeStr.data(), bitcodeStr.size());

                    // Create types
                    Type *int8Ty = IntegerType::get(ctx, 8);
                    PointerType *int8PtrTy = PointerType::get(int8Ty, 0);

                    // Create LLVM constant that holds the bitcode string
                    Constant *bitcodeConstant = ConstantDataArray::get(ctx, bitcode);
                    auto *bitcodeGV = new GlobalVariable(
                        M, bitcodeConstant->getType(), true, GlobalValue::PrivateLinkage,
                        bitcodeConstant, F.getName() + ".bitcode");
                    bitcodeGV->setUnnamedAddr(GlobalValue::UnnamedAddr::Global);
                    Value *bitcodePtr = B.CreateBitCast(B.CreateConstGEP2_32(bitcodeConstant->getType(), bitcodeGV, 0, 0), int8PtrTy);
                    Constant *bitcodeLen = ConstantInt::get(Type::getInt64Ty(ctx), bitcode.size());

                    // Insert call to registration function into initializer
                    Constant *funcPtr = ConstantExpr::getBitCast(&F, int8PtrTy);
                    B.CreateCall(registrationFunction, {funcPtr, bitcodePtr, bitcodeLen});

                    // finish initializer
                    B.CreateRetVoid();
                    appendToGlobalCtors(M, ctor, 65535);
                }
            }
        }


        return PreservedAnalyses::all();
    }
};

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

std::string serialize(Function &F, LLVMContext &ctx)
{
    llvm::LLVMContext newCtx;

    Module wrapperModule = Module("func_module", ctx);

    Function *clonedFunc = Function::Create(F.getFunctionType(), F.getLinkage(), F.getName(), &wrapperModule);
    ValueToValueMapTy v2vMap;
    for (auto originalIt = F.arg_begin(), cloneIt = clonedFunc->arg_begin(); originalIt != F.arg_end(); ++originalIt, ++cloneIt)
        v2vMap[&*originalIt] = &*cloneIt;

    SmallVector<ReturnInst *, 8> returnInst;
    CloneFunctionInto(clonedFunc, &F, v2vMap, CloneFunctionChangeType::LocalChangesOnly, returnInst);
    llvm::errs() << "\nChecking Module\n";

    if (verifyModule(wrapperModule, &llvm::errs())) {
        llvm::errs() << "\nCloned module is invalid!\n";
        int* p = nullptr;
        *p = 42;
    }

    SmallVector<char, 0> buffer;
    raw_svector_ostream OS(buffer);
    WriteBitcodeToFile(wrapperModule, OS);

    std::string bitcodeStr(buffer.begin(), buffer.end());

    auto buffer2 = llvm::MemoryBuffer::getMemBuffer(
    llvm::StringRef(bitcodeStr.data(), bitcodeStr.size()), "", false);
    llvm::Expected<std::unique_ptr<llvm::Module>> moduleOrErr = llvm::parseBitcodeFile(buffer2->getMemBufferRef(), newCtx);
    if (!moduleOrErr) {
        logAllUnhandledErrors(moduleOrErr.takeError(), llvm::errs(), "Bitcode parse error: ");
        int* p = nullptr;
        *p = 42;
    }
    std::size_t hashValue = std::hash<std::string>{}(bitcodeStr);

    std::cout << "Hash: " << hashValue << "\n";
    return bitcodeStr;
}

Function *findRegistrationFunction(Module &M) {
    for (Function &F : M) {
        if (F.getName().contains("registerFunction")) { //TODO more unique name
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
            PB.registerOptimizerLastEPCallback(
                [](ModulePassManager &MPM, auto, auto) {
                    MPM.addPass(NautilusInlineRegistrationPass{});
                });
        }
    };
}




