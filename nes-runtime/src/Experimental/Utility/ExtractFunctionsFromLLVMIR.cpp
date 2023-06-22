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
#include <llvm/IR/IRPrintingPasses.h>
#include <llvm/IR/Instructions.h>
#include <llvm/IR/LLVMContext.h>
#include <llvm/IR/LegacyPassManager.h>
#include <llvm/IR/Mangler.h>
#include <llvm/IR/Module.h>
#include <llvm/IRReader/IRReader.h>
#include <llvm/Transforms/IPO/ExtractGV.h>

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
#include <fstream>

using namespace llvm;

/**
 * @brief Tries to find a substring a line. If it finds one, it replaces a substring with another one.
 * 
 * @param line: The line in the LLVM IR that we want to replace a substring in.
 * @param toReplace: The substring that we want to replace.
 * @param replacement: The string that we want to replace 'toReplace' with.
 */
bool replaceString(std::string& line, const std::string toReplace, const std::string replacement) {
    auto toReplacePosition = line.find(toReplace);
    if (toReplacePosition != std::string::npos) {
        line.replace(toReplacePosition, toReplace.length(), replacement);
        return true;
    }
    return false;
}

/**
 * @brief A custom pass that iterates over the generated proxy function file and adds information that gets lost
 *        during function extraction. A clean solution would be to use the LLVM passes correctly. We use this custom
 *        pass for now, because finding the correct LLVM approach could take very long.
 * 
 * @param filename: The name of the file that we want to fix @.str nad @__PRETTY_FUNCTION lines for.
 */
bool strAndPrettyFunctionFixPass(const std::string& filename) {
    std::ifstream inputFile(filename);

    if (!inputFile) {
        std::cout << "Failed to open input file." << std::endl;
        return false;
    }

    std::stringstream modifiedContent; // Store modified content in a stringstream
    std::string line;

    while (std::getline(inputFile, line)) {
        // if (line.substr(0, 13) == "@.str.80.151") {
        if (line.substr(0, 5) == "@.str" || line.substr(0, 20) == "@__PRETTY_FUNCTION__") {
            // Replace "external" with "private" and add "unnamed_addr" after "private"
            const std::string toReplaceString = "external hidden";
            size_t position = line.find(toReplaceString);
            if (position != std::string::npos) {
                line.replace(position, toReplaceString.length(), "private");
            }
            // todo adapt file path
            if(!replaceString(line, "[110 x i8], align 1", "[110 x i8] c\"/home/pgrulich/projects/nes/nebulastream/nes-runtime/include/Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp\\00\", align 1")) {
                if(!replaceString(line, "[15 x i8], align 1", "[15 x i8] c\"pos < capacity\\00\", align 1")) {
                    if(!replaceString(line, "[26 x i8], align 1", "[26 x i8] c\"vector::_M_realloc_insert\\00\", align 1")) {
                        replaceString(line, "[71 x i8], align 1", "[71 x i8] c\"void NES::Nautilus::Interface::ChainedHashMap::insert(Entry *, hash_t)\\00\", align 1");
                    }
                }
            }
        }
        modifiedContent << line << std::endl;
    }
    inputFile.close();

    // Overwrite input file with modified file content.
    std::ofstream outputFile(filename);
    if (!outputFile) {
        std::cout << "Failed to open output file." << std::endl;
        return false;
    }
    outputFile << modifiedContent.str(); // Overwrite the content of the input file
    outputFile.close();
    return true;
}

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
    if(!Result) {
        std::cout << "Could not demangle function with name: " << functionName << '\n';
        return "";
    } else {
        return std::string(Result);
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
        if(argc > 0) {
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
        if(functionName == "_ZN3NES8Nautilus9Interface9hashValueIiEEmmT_") {
            func->setName("hashValueI32");
        } else {
            demangledName = demangleToBaseName(functionName);
        }
        if(!demangledName.empty()) {
            unmangledToMangledNamesMap.emplace(
                std::pair<std::string, std::string>{demangledName, functionName});
        }
    }

    // Get functions from LLVM IR module. Use mapping to mangled function names, if needed.
    std::vector<std::string> ExtractFuncs{"NES__Runtime__TupleBuffer__getNumberOfTuples",
                                          "NES__Runtime__TupleBuffer__setNumberOfTuples",
                                          "NES__Runtime__TupleBuffer__getBuffer",
                                          "NES__Runtime__TupleBuffer__getBufferSize",
                                          "NES__Runtime__TupleBuffer__getWatermark", // Does this cover NES__Runtime__TupleBuffer__Watermark? -> Why does it have a different name in 'RecordBuffer'?
                                          "NES__Runtime__TupleBuffer__setWatermark",
                                          "NES__Runtime__TupleBuffer__getCreationTimestampInMS",
                                          "NES__Runtime__TupleBuffer__setSequenceNumber",
                                          "NES__Runtime__TupleBuffer__getSequenceNumber",
                                          "NES__Runtime__TupleBuffer__setCreationTimestampInMS",
                                         "NES__Runtime__TupleBuffer__getOriginId",
                                         "NES__Runtime__TupleBuffer__setOriginId",
                                         "getProbeHashMapProxy",
                                         "findChainProxy",
                                        // "insertProxy",
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
    // We create a vector of GlobValues that represent the functions that we do NOT want to extract.
    std::vector<GlobalValue*> FunctionsToKeep(ProxyFunctionValues.begin(), ProxyFunctionValues.end());
    // We then run the GlobalValue (GV) extraction pass and remove all functions that we want to extract.
    auto const extractGVPass = new llvm::ExtractGVPass(FunctionsToKeep, /* false: keep functions */ false, false);
    llvm::ModuleAnalysisManager dummyAnalysisManager;
    extractGVPass->run(*LLVMModule, dummyAnalysisManager);

    // Now that we only have the GVs that we need left, mark the module as fully materialized.
    ExitOnErr(LLVMModule->materializeAll());

    // Remove Debug Info
    assert(llvm::StripDebugInfo(*LLVMModule));
    // Apply Passes to remove unreachable globals, dead function declarations, and debug info.
    legacy::PassManager Passes;
    Passes.add(createGlobalDCEPass());          // Delete unreachable globals
    Passes.add(createStripDeadDebugInfoPass()); // Remove dead debug info
    Passes.add(createStripDeadPrototypesPass());// Remove dead func decls

    std::error_code EC;
    // We statically write proxy functions to /tmp/proxiesReduced.ll (we might change this behavior in #3710)
    const std::string filename = std::filesystem::temp_directory_path().string() + "/proxiesReduced.ll";
    ToolOutputFile Out(filename, EC, sys::fs::OF_None);
    if (EC) {
        errs() << EC.message() << '\n';
        return 1;
    }

    Passes.add(createPrintModulePass(Out.os(), "", false));
    Passes.run(*LLVMModule.get());

    // Declare success.
    Out.keep();

     if(strAndPrettyFunctionFixPass(filename)) {
         return 0;
     }
    return 1;
    //return 0;
}