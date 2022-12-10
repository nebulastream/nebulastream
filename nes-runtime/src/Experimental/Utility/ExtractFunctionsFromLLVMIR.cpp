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

#include <llvm/ADT/SetVector.h>
#include <llvm/ADT/SmallPtrSet.h>
#include <llvm/Bitcode/BitcodeWriter.h>
#include <llvm/Bitcode/BitcodeWriterPass.h>
#include <llvm/Demangle/Demangle.h>
#include <llvm/ExecutionEngine/Orc/Mangling.h>
#include <llvm/IR/DataLayout.h>
#include <llvm/IR/DebugInfo.h>
#include <llvm/IR/IRPrintingPasses.h>
#include <llvm/IR/Instructions.h>
#include <llvm/IR/LLVMContext.h>
#include <llvm/IR/LegacyPassManager.h>
#include <llvm/IR/Mangler.h>
#include <llvm/IR/Module.h>
#include <llvm/IRReader/IRReader.h>

#include <cstddef>
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
#include <memory>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
using namespace llvm;

std::string demangleToBaseName(const std::string& functionName) {
    const char* mangled = functionName.c_str();
    size_t Size = functionName.size();
    char* Buf = static_cast<char*>(std::malloc(Size));

    llvm::ItaniumPartialDemangler Mangler;
    if (Mangler.partialDemangle(mangled)) {
        return "";
    }
    char* Result = Mangler.getFunctionBaseName(Buf, &Size);
    return std::string(Result);
}

int main(int argc, char** argv) {
    InitLLVM X(argc, argv);
    bool Recursive = true;
    NES_DEBUG("Start Extraction proxy functions from: " << IR_FILE_DIR << "/" << IR_FILE_FILE);
    LLVMContext Context;

    // Use lazy loading, since we only care about selected global values.
    SMDiagnostic Err;
    std::unique_ptr<Module> LLVMModule =
        getLazyIRFileModule(std::string(IR_FILE_DIR) + "/" + std::string(IR_FILE_FILE), Err, Context);

    if (!LLVMModule.get()) {
        Err.print(argv[0], errs());
        return 1;
    }

    // Use SetVector to avoid duplicates.
    SetVector<GlobalValue*> ProxyFunctionValues;

    // Get unmangled function names from LLVM IR module.
    std::unordered_map<std::string, std::string> unmangledToMangledNamesMap;
    for (auto func = LLVMModule->getFunctionList().begin(); func != LLVMModule->getFunctionList().end(); func++) {
        unmangledToMangledNamesMap.emplace(
            std::pair<std::string, std::string>{demangleToBaseName(func->getName().str()), func->getName().str()});
    }

    // Get functions from LLVM IR module. Use mapping to mangled function names, if needed.
    std::vector<std::string> ExtractFuncs{"NES__QueryCompiler__PipelineContext__getGlobalOperatorStateProxy",
                                          "NES__Runtime__TupleBuffer__getNumberOfTuples",
                                          "NES__Runtime__TupleBuffer__setNumberOfTuples",
                                          "NES__Runtime__TupleBuffer__getBuffer",
                                          // "NES__QueryCompiler__PipelineContext__emitBufferProxy",
                                          "NES__Runtime__TupleBuffer__getBufferSize",
                                          "NES__Runtime__TupleBuffer__getWatermark",
                                          "NES__Runtime__TupleBuffer__setWatermark",
                                          "NES__Runtime__TupleBuffer__getCreationTimestamp",
                                          "NES__Runtime__TupleBuffer__setSequenceNumber",
                                          "NES__Runtime__TupleBuffer__getSequenceNumber",
                                          "NES__Runtime__TupleBuffer__setCreationTimestamp"};

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

    legacy::PassManager Extract;
    std::vector<GlobalValue*> FunctionsToKeep(ProxyFunctionValues.begin(), ProxyFunctionValues.end());
    Extract.add(createGVExtractionPass(FunctionsToKeep, /* false: keep functions */ false, false));
    Extract.run(*LLVMModule);

    // Now that we only have the GVs that we need left, mark the module as fully materialized.
    ExitOnErr(LLVMModule->materializeAll());

    // Remove Debug Info
    assert(llvm::StripDebugInfo(*LLVMModule));
    // Apply Passes to remove unreachable globals, dead function declarations, and debug info.
    legacy::PassManager Passes;
    Passes.add(createGlobalDCEPass());          // Delete unreachable globals
    Passes.add(createStripDeadDebugInfoPass()); // Remove dead debug info
    Passes.add(createStripDeadPrototypesPass());// Remove dead func decls
    // Passes.add(createFunctionInliningPass());    // Inline function definitions

    NES_DEBUG("Generate proxy functions file at : " << PROXY_FUNCTIONS_RESULT_DIR);
    std::error_code EC;
    ToolOutputFile Out(std::string(PROXY_FUNCTIONS_RESULT_DIR), EC, sys::fs::OF_None);
    if (EC) {
        errs() << EC.message() << '\n';
        return 1;
    }

    Passes.add(createPrintModulePass(Out.os(), "", false));
    Passes.run(*LLVMModule.get());

    // Below call can lead to errors.
    // system("/home/rudi/CLionProjects/GenerateProxyFunctionsIR/cmake-build-debug/mlir-translate --import-llvm ./llvm-ir/nes-runtime_opt/proxiesReduced.ll -o ./llvm-ir/nes-runtime_opt/proxiesFinal.mlir");

    // Declare success.
    Out.keep();

    return 0;
}
