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

#include <exception>
#include <filesystem>
#include <llvm/ADT/SetVector.h>
#include <llvm/ADT/SmallPtrSet.h>
#include <llvm/Bitcode/BitcodeWriter.h>
#include <llvm/Bitcode/BitcodeWriterPass.h>
#include <llvm/Demangle/Demangle.h>
#include <llvm/ExecutionEngine/Orc/Mangling.h>
#include <llvm/IR/DataLayout.h>
#include <llvm/IR/DebugInfo.h>
#include <llvm/IR/GlobalValue.h>
#include <llvm/IR/IRPrintingPasses.h>
#include <llvm/IR/Instructions.h>
#include <llvm/IR/LLVMContext.h>
#include <llvm/IR/LegacyPassManager.h>
#include <llvm/IR/Mangler.h>
#include <llvm/IR/Module.h>
#include <llvm/IR/PassManager.h>
#include <llvm/IRReader/IRReader.h>
#include <llvm/Transforms/IPO/ExtractGV.h>

#include <cstddef>
#include <fstream>
#include <iostream>
#include <llvm/Pass.h>
#include <llvm/Support/CommandLine.h>
#include <llvm/Support/Error.h>
#include <llvm/Support/FileSystem.h>
#include <llvm/Support/InitLLVM.h>
#include <llvm/Support/Regex.h>
#include <llvm/Support/SourceMgr.h>
#include <llvm/Support/SystemUtils.h>
#include <llvm/Support/ToolOutputFile.h>
#include <llvm/Transforms/IPO.h>
#include <llvm/Transforms/Utils/Cloning.h>
#include <memory>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>

using namespace llvm;

std::string demangleToBaseName(const std::string& functionName) {
    (void) functionName;
    const char* mangled = functionName.c_str();
    size_t Size = functionName.size();
    char* Buf = static_cast<char*>(std::malloc(Size));

    llvm::ItaniumPartialDemangler Mangler;
    if (Mangler.partialDemangle(mangled)) {
        return "";
    }
    char* Result = Mangler.getFunctionBaseName(Buf, &Size);
    if (!Result) {
        std::cout << "Could not demangle function with name: " << functionName << '\n';
        return "";
    } else {
        return std::string(Result);
    }
}

static void makeVisible(GlobalValue& GV, bool Delete) {

    bool Local = GV.hasLocalLinkage();
    if (Local || Delete) {
        GV.setLinkage(GlobalValue::ExternalLinkage);
        if (Local) {
            //  All relevant strings pass through here, but also irrelevant strings.
            GV.setVisibility(GlobalValue::HiddenVisibility);
        }
        return;
    }

    if (!GV.hasLinkOnceLinkage()) {
        assert(!GV.isDiscardableIfUnused());
        return;
    }

    // Map linkonce* to weak* so that llvm doesn't drop this GV.
    switch (GV.getLinkage()) {
        default: llvm_unreachable("Unexpected linkage");
        case GlobalValue::LinkOnceAnyLinkage: GV.setLinkage(GlobalValue::WeakAnyLinkage); return;
        case GlobalValue::LinkOnceODRLinkage: GV.setLinkage(GlobalValue::WeakODRLinkage); return;
    }
}

void extractPass(Module& M, std::vector<GlobalValue*>& GVs, bool deleteStuff, bool keepConstInit) {
    // Visit the global inline asm.
    if (!deleteStuff) {
        M.setModuleInlineAsm("");
    }
    SetVector<GlobalValue*> Named(GVs.begin(), GVs.end());

    for (GlobalVariable& GV : M.globals()) {
        bool specialException = false;

        bool Delete = deleteStuff == (bool) Named.count(&GV) && !GV.isDeclaration() && (!GV.isConstant() || !keepConstInit);
        if (!Delete) {
            if (GV.hasAvailableExternallyLinkage())
                continue;
            if (GV.getName() == "llvm.global_ctors")
                continue;
        }

        makeVisible(GV, Delete);

        if (Delete) {
            // By removing the initializer, we loose important information on GVs that we want to keep.
            // On the other hand, it is required to remove all information that we do not need.
            GV.setInitializer(nullptr);
            GV.setComdat(nullptr);
        }
    }

    // Visit the Functions.
    for (Function& F : M) {
        bool Delete = deleteStuff == (bool) Named.count(&F) && !F.isDeclaration();
        if (!Delete) {
            if (F.hasAvailableExternallyLinkage())
                continue;
        }

        makeVisible(F, Delete);

        if (Delete) {
            // Make this a declaration and drop it's comdat.
            F.deleteBody();
            F.setComdat(nullptr);
        }
    }

    // Visit the Aliases.
    for (GlobalAlias& GA : llvm::make_early_inc_range(M.aliases())) {
        bool Delete = deleteStuff == (bool) Named.count(&GA);
        makeVisible(GA, Delete);

        if (Delete) {
            Type* Ty = GA.getValueType();

            GA.removeFromParent();
            llvm::Value* Declaration;
            if (FunctionType* FTy = dyn_cast<FunctionType>(Ty)) {
                Declaration = Function::Create(FTy, GlobalValue::ExternalLinkage, GA.getAddressSpace(), GA.getName(), &M);

            } else {
                Declaration = new GlobalVariable(M, Ty, false, GlobalValue::ExternalLinkage, nullptr, GA.getName());
            }
            GA.replaceAllUsesWith(Declaration);
            delete &GA;
        }
    }

    // Visit the IFuncs.
    for (GlobalIFunc& IF : llvm::make_early_inc_range(M.ifuncs())) {
        bool Delete = deleteStuff == (bool) Named.count(&IF);
        makeVisible(IF, Delete);

        if (!Delete)
            continue;

        auto* FuncType = dyn_cast<FunctionType>(IF.getValueType());
        IF.removeFromParent();
        llvm::Value* Declaration =
            Function::Create(FuncType, GlobalValue::ExternalLinkage, IF.getAddressSpace(), IF.getName(), &M);
        IF.replaceAllUsesWith(Declaration);
        delete &IF;
    }
}

void reinitializePass(Module& M, Module& M2) {
    for (GlobalVariable& GV : M.globals()) {
        for (GlobalVariable& GV2 : M2.globals()) {
            if (GV.getName() == GV2.getName()) {
                GV.setInitializer(GV2.getInitializer());
                GV.setComdat(GV2.getComdat());
            }
        }
    }
}

int main(int argc, char** argv) {
    bool Recursive = true;
    LLVMContext Context;

    // Use lazy loading, since we only care about selected global values.
    SMDiagnostic Err;
    std::unique_ptr<Module> LLVMModule =
        getLazyIRFileModule(std::string(IR_FILE_DIR) + "/" + std::string(IR_FILE_FILE), Err, Context);

    if (!LLVMModule.get()) {
        if (argc > 0) {
            Err.print(argv[0], errs());
        }
        return 1;
    }

    // Use SetVector to avoid duplicates.
    SetVector<GlobalValue*> ProxyFunctionValues;

    // Get unmangled function names from LLVM IR module.
    std::unordered_map<std::string, std::string> unmangledToMangledNamesMap;
    for (auto func = LLVMModule->getFunctionList().begin(); func != LLVMModule->getFunctionList().end(); func++) {
        std::string functionName = func->getName().str();
        std::string demangledName = "";
        // The hashValue function is a templated function that is created. During build time, several versions of the
        // function are created (one for each allowed data type). Currently, we only need the i32 version.
        if (functionName == "_ZN3NES8Nautilus9Interface9hashValueIiEEmmT_") {
            func->setName("hashValueI32");
        } else {
            demangledName = demangleToBaseName(functionName);
        }
        if (!demangledName.empty()) {
            unmangledToMangledNamesMap.emplace(std::pair<std::string, std::string>{demangledName, functionName});
        }
    }

    // Get functions from LLVM IR module. Use mapping to mangled function names, if needed.
    std::vector<std::string> ExtractFuncs{
        "NES__Runtime__TupleBuffer__getNumberOfTuples",
        "NES__Runtime__TupleBuffer__setNumberOfTuples",
        "NES__Runtime__TupleBuffer__getBuffer",
        "NES__Runtime__TupleBuffer__getBufferSize",
        "NES__Runtime__TupleBuffer__getWatermark",// Does this cover NES__Runtime__TupleBuffer__Watermark? -> Why does it have a different name in 'RecordBuffer'?
        "NES__Runtime__TupleBuffer__setWatermark",
        "NES__Runtime__TupleBuffer__getCreationTimestampInMS",
        "NES__Runtime__TupleBuffer__setSequenceNumber",
        "NES__Runtime__TupleBuffer__getSequenceNumber",
        "NES__Runtime__TupleBuffer__setCreationTimestampInMS",
        "NES__Runtime__TupleBuffer__getOriginId",
        "NES__Runtime__TupleBuffer__setOriginId",
        "getProbeHashMapProxy",
        "findChainProxy",
        "insertProxy",
        "_ZN3NES8Nautilus9Interface12hashValueCRCIaEEmmT_",
        "_ZN3NES8Nautilus9Interface12hashValueCRCIdEEmmT_",
        "_ZN3NES8Nautilus9Interface12hashValueCRCIfEEmmT_",
        "_ZN3NES8Nautilus9Interface12hashValueCRCIhEEmmT_",
        "_ZN3NES8Nautilus9Interface12hashValueCRCIiEEmmT_",
        "_ZN3NES8Nautilus9Interface12hashValueCRCIjEEmmT_",
        "_ZN3NES8Nautilus9Interface12hashValueCRCIlEEmmT_",
        "_ZN3NES8Nautilus9Interface12hashValueCRCImEEmmT_",
        "_ZN3NES8Nautilus9Interface12hashValueCRCIsEEmmT_",
        "_ZN3NES8Nautilus9Interface12hashValueCRCItEEmmT_",
        "hashValueI32",
        "getWorkerIdProxy",
        "getPagedVectorProxy",
        "allocateNewPageProxy",
        "getKeyedStateProxy",
        //  "getGlobalOperatorHandlerProxy"
    };

    for (size_t i = 0; i != ExtractFuncs.size(); ++i) {
        GlobalValue* GV = LLVMModule->getFunction(ExtractFuncs[i]);
        if (!GV) {
            GV = LLVMModule->getFunction(unmangledToMangledNamesMap[ExtractFuncs[i]]);
            if (!GV) {
                errs() << "Program doesn't contain function named: '" << ExtractFuncs[i] << "'!\n";
                return 1;
            }
        }
        ProxyFunctionValues.insert(GV);
    }

    // Recursively add functions called by proxy functions to proxy function set.
    ExitOnError ExitOnErr("error reading input");
    if (Recursive) {
        std::vector<llvm::Function*> Workqueue;
        for (GlobalValue* ProxyFunction : ProxyFunctionValues) {
            if (auto* F = dyn_cast<Function>(ProxyFunction)) {
                Workqueue.push_back(F);
            }
        }
        while (!Workqueue.empty()) {
            Function* function = &*Workqueue.back();
            Workqueue.pop_back();
            ExitOnErr(function->materialize());
            // Iterate over BasicBlocks (BB) of function. If BB contains CallBase, process called function.
            for (auto& BB : *function) {
                for (auto& I : BB) {
                    CallBase* callBase = dyn_cast<CallBase>(&I);
                    if (!callBase)
                        continue;
                    Function* calledFunction = callBase->getCalledFunction();
                    if (!calledFunction)
                        continue;
                    if (calledFunction->isDeclaration() || ProxyFunctionValues.count(calledFunction))
                        continue;
                    ProxyFunctionValues.insert(calledFunction);
                    Workqueue.push_back(calledFunction);
                }
            }
        }
    }
    // Unfortunately, the extractPass removes information from relevant GlobalValues that leads to compilation errors.
    // To counteract, we preserve that information, and enrich the GlobalValues with that information at the end again.

    // Copy the original module, to preserve GlobalValue information.
    auto copiedModule = CloneModule(*LLVMModule);
    // We create a vector of GlobValues that represent the functions that we do NOT want to extract.
    std::vector<GlobalValue*> FunctionsToKeep(ProxyFunctionValues.begin(), ProxyFunctionValues.end());
    // We then run the GlobalValue (GV) extraction pass and remove all functions that we want to extract.
    extractPass(*LLVMModule, FunctionsToKeep, false, false);

    // Now that we only have the GVs that we need left, mark the module as fully materialized.
    ExitOnErr(LLVMModule->materializeAll());

    // Remove Debug Info
    assert(llvm::StripDebugInfo(*LLVMModule));
    // Apply Passes to remove unreachable globals, dead function declarations, and debug info.
    legacy::PassManager Passes;
    Passes.add(createGlobalDCEPass());          // Delete unreachable globals
    Passes.add(createStripDeadDebugInfoPass()); // Remove dead debug info
    Passes.add(createStripDeadPrototypesPass());// Remove dead func decls
    Passes.run(*LLVMModule.get());

    // The module is stripped of all relevant information, and, unfortunately, also of important information on GVs.
    // We use the preserver GV information from the copiedModule to enrich the GVs in the stripped module.
    reinitializePass(*LLVMModule, *copiedModule);

    // Print
    legacy::PassManager PrintPass;
    std::error_code EC;
    // We statically write proxy functions to /tmp/proxiesReduced.ll (we might change this behavior in #3710)
    const std::string filename = std::filesystem::temp_directory_path().string() + "/proxiesReduced.ll";
    ToolOutputFile Out(filename, EC, sys::fs::OF_None);
    if (EC) {
        errs() << EC.message() << '\n';
        return 1;
    }
    PrintPass.add(createPrintModulePass(Out.os(), "", false));
    PrintPass.run(*LLVMModule.get());

    // Declare success.
    Out.keep();

    return 0;
}