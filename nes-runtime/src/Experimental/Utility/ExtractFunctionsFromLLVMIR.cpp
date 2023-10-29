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

using namespace llvm;

std::optional<std::string> demangleToBaseName(const std::string& functionName) {
    const char* mangled = functionName.c_str();
    size_t Size = functionName.size();
    char* Buf = static_cast<char*>(std::malloc(Size));

    llvm::ItaniumPartialDemangler Mangler;
    if (Mangler.partialDemangle(mangled)) {
        return {};
    }
    if (!Mangler.isFunction()) {
        return {};
    }

    char* Result = Mangler.getFunctionBaseName(Buf, &Size);
    return std::string(Result);
}

int main(int argc, char** argv) {
    InitLLVM X(argc, argv);
    bool Recursive = true;
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
        auto demangledOption = demangleToBaseName(func->getName().str());
        if (demangledOption.has_value()) {
            unmangledToMangledNamesMap.emplace(std::pair<std::string, std::string>{*demangledOption, func->getName().str()});
        }
    }

    // Get functions from LLVM IR module. Use mapping to mangled function names, if needed.
    std::vector<std::string> ExtractFuncs{"destructReference",
                                          "clearHLL",
                                          "estimateHLL",
                                          "hlladd",
                                          "mergeHLL",
                                          "max",
                                          "min",
                                          "clearTDigest",
                                          "estimateTDigest",
                                          "insert",
                                          "mergeTDigest",
                                          "calculateAbs",
                                          "calculateAcos",
                                          "calculateAsin",
                                          "calculateAtan2",
                                          "calculateAtanDouble",
                                          "calculateAtanFloat",
                                          "bitcounter",
                                          "calculateCbrt",
                                          "calculateCeil",
                                          "calculateCos",
                                          "calculateCot",
                                          "calculateDegrees",
                                          "calculateExp",
                                          "calculateFactorial",
                                          "calculateFloor",
                                          "calculateGamma",
                                          "calculateLGamma",
                                          "calculateLn",
                                          "calculateLog2",
                                          "calculateLog10",
                                          "calculateMax",
                                          "calculateMin",
                                          "calculateMod",
                                          "calculatePower",
                                          "calculateRadians",
                                          "random",
                                          "calculateSign",
                                          "calculateSin",
                                          "calculateSqrt",
                                          "calculateTan",
                                          "calculateTrunc",
                                          "regexExtract",
                                          "regexMatch",
                                          "regexReplace",
                                          "regex_search",
                                          "textHamming",
                                          "textJaccard",
                                          "textJaro",
                                          "textJaroWinkler",
                                          "textLevenshtein",
                                          "loadAssociatedTextValue",
                                          "storeAssociatedTextValue",
                                          "allocateBufferProxy",
                                          "emitBufferProxy",
                                          "getGlobalOperatorHandlerProxy",
                                          "getWorkerIdProxy",
                                          "getThreadLocalState",
                                          "setupHandler",
                                          "getStates",
                                          "getKeyedStateProxy",
                                          "getPagedVectorProxy",
                                          "setupJoinBuildHandler",
                                          "getProbeHashMapProxy",
                                          "IncrementThreadSaveAndCheckLimit",
                                          "encodeData",
                                          "getStackProxy",
                                          "getStateEntrySizeProxy",
                                          "setupHandlerProxy",
                                          "getSortStateEntrySizeProxy",
                                          "getStateProxy",
                                          "SortProxy",
                                          "appendToGlobalSliceStore",
                                          "triggerSlidingWindows",
                                          "findKeyedBucketsByTs",
                                          "getKeyedBucket",
                                          "getKeyedBucketListSize",
                                          "getKeyedBucketStore",
                                          "setupKeyedBucketWindowHandler",
                                          "triggerKeyedBucketsProxy",
                                          "findBucketsByTs",
                                          "getBucket",
                                          "getBucketListSize",
                                          "getBucketStore",
                                          "getDefaultBucketState",
                                          "setupBucketWindowHandler",
                                          "triggerBucketsProxy",
                                          "createKeyedState",
                                          "freeKeyedSliceMergeTask",
                                          "getKeyedNumberOfSlicesFromTask",
                                          "getKeyedSliceState",
                                          "getKeyedSliceStateFromTask",
                                          "setupKeyedSliceMergingHandler",
                                          "findKeyedSliceStateByTsProxy",
                                          "getKeyedSliceStoreProxy",
                                          "setupWindowHandler2",
                                          "triggerKeyedThreadLocalWindow",
                                          "deleteSlice",
                                          "createGlobalState",
                                          "freeNonKeyedSliceMergeTask",
                                          "getDefaultMergingState",
                                          "getGlobalSliceState",
                                          "getNonKeyedNumberOfSlices",
                                          "getNonKeyedSliceState",
                                          "setupSliceMergingHandler",
                                          "findSliceStateByTsProxy",
                                          "getDefaultState",
                                          "getSliceStoreProxy",
                                          "setupWindowHandler",
                                          "triggerThreadLocalStateProxy",
                                          "deleteNonKeyedSlice",
                                          "getHashTableRefProxy",
                                          "insertFunctionProxy",
                                          "getHashSliceProxy",
                                          "getNumberOfPagesProxyForHashJoin",
                                          "getPageFromBucketAtPosProxyForHashJoin",
                                          "getPartitionIdProxy",
                                          "getSliceIdHJProxy",
                                          "getWindowEndProxyForHashJoin",
                                          "getWindowStartProxyForHashJoin",
                                          "getDefaultMemRef",
                                          "getHJSliceProxy",
                                          "getLocalHashTableProxy",
                                          "getSliceEndProxy",
                                          "getSliceStartProxy",
                                          "getPagedVectorRefProxy",
                                          "getNLJPagedVectorProxy",
                                          "getNLJSliceRefFromIdProxy",
                                          "getNLJWindowEndProxy",
                                          "getNLJWindowStartProxy",
                                          "getSliceIdNLJProxy",
                                          "getCurrentWindowProxy",
                                          "getNLJSliceEndProxy",
                                          "getNLJSliceRefProxy",
                                          "getNLJSliceStartProxy",
                                          "checkWindowsTriggerProxy",
                                          "setNumberOfWorkerThreadsProxy",
                                          "triggerAllWindowsProxy",
                                          "calcNumWindowsProxy",
                                          "getAllWindowsToFillProxy",
                                          "getDefaultMemRefProxy",
                                          "deleteAllSlicesProxy",
                                          "addAggregationValues",
                                          "createStateIfNotExist",
                                          "getIsKeyedThresholdWindowOpen",
                                          "getKeyedAggregationValue",
                                          "getKeyedThresholdWindowRecordCount",
                                          "incrementKeyedThresholdWindowCount",
                                          "lockKeyedThresholdWindowHandler",
                                          "resetKeyedThresholdWindowCount",
                                          "setKeyedThresholdWindowIsWindowOpen",
                                          "unlockKeyedThresholdWindowHandler",
                                          "getAggregationValue",
                                          "getIsWindowOpen",
                                          "getRecordCount",
                                          "incrementCount",
                                          "lockWindowHandler",
                                          "resetCount",
                                          "setIsWindowOpen",
                                          "unlockWindowHandler",
                                          "NES__Runtime__TupleBuffer__getBuffer",
                                          "NES__Runtime__TupleBuffer__getCreationTimestampInMS",
                                          "NES__Runtime__TupleBuffer__getNumberOfTuples",
                                          "NES__Runtime__TupleBuffer__getOriginId",
                                          "NES__Runtime__TupleBuffer__getSequenceNumber",
                                          "NES__Runtime__TupleBuffer__getWatermark",
                                          "NES__Runtime__TupleBuffer__setCreationTimestampInMS",
                                          "NES__Runtime__TupleBuffer__setNumberOfTuples",
                                          "NES__Runtime__TupleBuffer__setOriginId",
                                          "NES__Runtime__TupleBuffer__setSequenceNumber",
                                          "NES__Runtime__TupleBuffer__setWatermark",
                                          "getLength",
                                          "listEquals",
                                          "readListIndex",
                                          "writeListIndex",
                                          "memcpy",
                                          "memeq",
                                          "getBitLength",
                                          "leftCharacters",
                                          "leftPad",
                                          "leftTrim",
                                          "lowercaseText",
                                          "lrTrim",
                                          "readTextIndex",
                                          "rightCharacters",
                                          "rightPad",
                                          "rightTrim",
                                          "textConcat",
                                          "textEquals",
                                          "TextGetLength",
                                          "textLike",
                                          "textPosition",
                                          "textPrefix",
                                          "textRepeat",
                                          "textReplace",
                                          "textReverse",
                                          "textSimilarTo",
                                          "textSubstring",
                                          "uppercaseText",
                                          "writeTextIndex",
                                          "agecurrentTime",
                                          "centuryProxy",
                                          "getDaysChrono",
                                          "getHoursChrono",
                                          "getMinutesChrono",
                                          "getMonthNameProxy",
                                          "getMonthsChrono",
                                          "getSecondsChrono",
                                          "getYearsChrono",
                                          "intervalproxy",
                                          "stringtomillisecondsproxy",
                                          "weekdayNameproxy",
                                          "addHashToBloomFilterProxy",
                                          "customBitCastProxy",
                                          "hashTextValue",
                                          "hashValue",
                                          "findChainProxy",
                                          "getPageProxy",
                                          "insetProxy",
                                          "allocateNewPageProxy",
                                          "getPagedVectorPageProxy"};

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
    ToolOutputFile Out("proxiesReduced.ll", EC, sys::fs::OF_None);
    if (EC) {
        errs() << EC.message() << '\n';
        return 1;
    }

    Passes.add(createPrintModulePass(Out.os(), "", false));
    Passes.run(*LLVMModule.get());

    // Declare success.
    Out.keep();

    return 0;
}