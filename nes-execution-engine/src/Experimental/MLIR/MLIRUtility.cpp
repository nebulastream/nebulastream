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

#include <Experimental/MLIR/MLIRGenerator.hpp>
#include <Experimental/MLIR/MLIRUtility.hpp>
#include <llvm/ADT/Triple.h>
#include <llvm/ExecutionEngine/ExecutionEngine.h>
#include <llvm/ExecutionEngine/JITSymbol.h>
#include <llvm/ExecutionEngine/Orc/Mangling.h>
#include <llvm/IR/LLVMContext.h>
#include <llvm/Support/CommandLine.h>
#include <llvm/Support/DynamicLibrary.h>
#include <llvm/Support/FileCollector.h>
#include <llvm/Support/Process.h>
#include <llvm/Support/raw_ostream.h>
#include <llvm/Target/TargetMachine.h>
#include <llvm/Target/TargetOptions.h>
#include <llvm/Transforms/IPO/SCCP.h>
#include <llvm/IRReader/IRReader.h>
#include <llvm/Linker/Linker.h>
#include <llvm/Support/SourceMgr.h>
#include <llvm/Support/TargetSelect.h>
#include <llvm/MC/TargetRegistry.h>
#include <mlir/Conversion/LLVMCommon/LoweringOptions.h>
#include <mlir/Conversion/SCFToStandard/SCFToStandard.h>
#include <mlir/Conversion/StandardToLLVM/ConvertStandardToLLVMPass.h>
#include <mlir/Conversion/VectorToLLVM/ConvertVectorToLLVM.h>
#include <mlir/Conversion/MemRefToLLVM/MemRefToLLVM.h>
#include <mlir/Conversion/AffineToStandard/AffineToStandard.h>
#include "mlir/Conversion/ReconcileUnrealizedCasts/ReconcileUnrealizedCasts.h"
#include <mlir/Dialect/Affine/IR/AffineOps.h>
#include <mlir/Dialect/LLVMIR/LLVMDialect.h>
#include <mlir/Dialect/Math/IR/Math.h>
#include <mlir/Dialect/SCF/SCF.h>
#include <mlir/Dialect/Vector/IR/VectorOps.h>
#include <mlir/Dialect/MemRef/IR/MemRef.h>
#include <mlir/Dialect/StandardOps/IR/Ops.h>
#include <mlir/ExecutionEngine/ExecutionEngine.h>
#include <mlir/ExecutionEngine/OptUtils.h>
#include <mlir/IR/BuiltinOps.h>
#include <mlir/IR/MLIRContext.h>
#include <mlir/IR/OperationSupport.h>
#include <mlir/Parser.h>
#include <mlir/Pass/PassManager.h>
#include <mlir/Target/LLVMIR/Dialect/LLVMIR/LLVMToLLVMIRTranslation.h>
#include <mlir/Target/LLVMIR/Export.h>
#include <mlir/Target/LLVMIR/LLVMTranslationInterface.h>
#include <mlir/Transforms/Passes.h>
#include <string>
#include <utility>
#include <filesystem>
#include <fstream>

#include <llvm/ExecutionEngine/Orc/JITTargetMachineBuilder.h>
#include <llvm/MC/TargetRegistry.h>
#include <llvm/Support/Host.h>
#include <llvm/Support/raw_ostream.h>
#include <llvm/MC/MCSubtargetInfo.h>
// #include <llvm-c/ExternC.h>
// #include <llvm-c/Target.h>
// #include <llvm-c/Types.h>

namespace NES::ExecutionEngine::Experimental::MLIR {
MLIRUtility::MLIRUtility(std::string mlirFilepath, bool debugFromFile)
    : mlirFilepath(std::move(mlirFilepath)), debugFromFile(debugFromFile){};

std::string MLIRUtility::insertComments(const std::string& moduleString) {
    std::string moduleWithComments;
    std::istringstream moduleStream(moduleString);

    for (std::string line; std::getline(moduleStream, line);) {
        int lastWhiteSpaceIdx = (int) line.find('%');
        int equalIdx = (int) line.find('=', lastWhiteSpaceIdx);
        if (line.substr(equalIdx + 2, 15) == "llvm.mlir.undef") {
            int commentStartIdx = equalIdx + 19;
            int commentEndIdx = (int) line.find('=', equalIdx + 19) - 1;
            std::string whiteSpaces = line.substr(0, lastWhiteSpaceIdx);
            std::string commentString = line.substr(commentStartIdx, commentEndIdx - commentStartIdx);
            commentString.erase(std::remove(commentString.begin(), commentString.end(), '"'), commentString.end());
            moduleWithComments += whiteSpaces + commentString + '\n';
        } else {
            moduleWithComments += line + '\n';
        }
    }
    return moduleWithComments;
}

void MLIRUtility::printMLIRModule(mlir::OwningOpRef<mlir::ModuleOp>& mlirModule, DebugFlags* debugFlags) {
    if (!debugFlags) {
        printf("No debug flags.\n");
        //        mlirModule->dump();
    } else {
        // Print location names and replace mlir::LLVM::UndefOp(s) with comments.
        std::string mlirString;
        llvm::raw_string_ostream llvmStringStream(mlirString);
        auto* basicError = new std::error_code();
        llvm::raw_fd_ostream fileStream(mlirFilepath, *basicError);
        // Enable debug info. Write module content to stream.
        auto* opPrintFlags = new mlir::OpPrintingFlags();
        if (debugFlags->enableDebugInfo) {
            opPrintFlags->enableDebugInfo(debugFlags->prettyDebug);
        }
        mlirModule->print(llvmStringStream, *opPrintFlags);
        // Insert comments. Print module to console. Write module to file.
        if (debugFlags->comments) {
            mlirString = insertComments(mlirString);
        }
        if (!mlirFilepath.empty()) {
            fileStream.write(mlirString.c_str(), mlirString.length());
        }
        std::cout << mlirString.c_str() << std::endl;
    }
}

int MLIRUtility::loadModuleFromString(const std::string& mlirString, DebugFlags* debugFlags) {
    context.loadDialect<mlir::StandardOpsDialect>();
    context.loadDialect<mlir::LLVM::LLVMDialect>();
    context.loadDialect<mlir::scf::SCFDialect>();
    module = parseSourceString(mlirString, &context);

    if (debugFlags) {
        printf("Kek %d", debugFlags->comments);
    }
    // module->dump();
    if (!module) {
        llvm::errs() << "NESMLIRGenerator::loadMLIR: Could not load MLIR module" << '\n';
        return 1;
    }

    mlir::PassManager passManager(&context);
    applyPassManagerCLOptions(passManager);
    passManager.addPass(mlir::createInlinerPass());
    passManager.addPass(mlir::createLowerToCFGPass());
    passManager.addPass(mlir::createLowerToLLVMPass());

    if (mlir::failed(passManager.run(*module))) {
        return 1;
    }
    return 0;
}

//Todo remove!!
int MLIRUtility::loadModuleFromStrings(const std::string& mlirString, const std::string& mlirString2, DebugFlags* debugFlags) {
    //    auto texst = mlir::UnrealizedConversionCastOp();
    //    auto whatDialect = mlir::UnrealizedConversionCastOp()->getDialect();
    //    mlir::LLVMTranslationDialectInterface testTransInt(mlir::UnrealizedConversionCastOp()->getDialect());
    //    context.loadDialect<testTransInt.getDialect()>();
    context.loadDialect<mlir::StandardOpsDialect>();
    context.loadDialect<mlir::LLVM::LLVMDialect>();
    context.loadDialect<mlir::scf::SCFDialect>();
    context.loadDialect<mlir::vector::VectorDialect>();

    module = parseSourceString(mlirString, &context);
    auto module2 = parseSourceString(mlirString2, &context);

    if (debugFlags) {
        printf("Kek %d", debugFlags->comments);
    }
    //    auto opIterator = module2->getOps().begin()->getBlock()->getOperations().front();
    //    module-> push_back(module2->getOps().begin()->getBlock()->getOperations().front().clone());
    // module->dump();

    if (!module) {
        llvm::errs() << "NESMLIRGenerator::loadMLIR: Could not load MLIR module" << '\n';
        return 1;
    }

    mlir::PassManager passManager(&context);
    applyPassManagerCLOptions(passManager);
    passManager.addPass(mlir::createInlinerPass());
    passManager.addPass(mlir::createLowerToCFGPass());
    // passManager.addPass(mlir::createConvertVectorToLLVMPass());
    passManager.addPass(mlir::createLowerToLLVMPass());

    if (mlir::failed(passManager.run(*module))) {
        return 1;
    }
    return 0;
}

bool MLIRUtility::generateMLIR(std::shared_ptr<IR::NESIR> nesIR, bool useSCF) {
    // Load all Dialects required to process/generate the required MLIR.
    context.loadDialect<mlir::StandardOpsDialect>();
    context.loadDialect<mlir::LLVM::LLVMDialect>();
    context.loadDialect<mlir::scf::SCFDialect>();
    // context.loadDialect<mlir::vector::VectorDialect>();

    // auto memberFunctions = MLIRGenerator::GetMemberFunctions(context);
    auto memberFunctions = std::vector<mlir::FuncOp>{};
    mlirGenerator = std::make_shared<MLIRGenerator>(context, memberFunctions);
    mlirGenerator->useSCF = useSCF;
    module = mlirGenerator->generateModuleFromNESIR(nesIR);
    if (!module) {
        llvm::errs() << "MLIRGenerator::loadMLIR: Could not load MLIR module" << '\n';
        return false;
    }
    return true;
}

bool MLIRUtility::lowerMLIR() {
    mlir::PassManager passManager(&context);
    applyPassManagerCLOptions(passManager);
    // passManager.addPass(mlir::createInlinerPass());
    passManager.addPass(mlir::createLowerToCFGPass());
    passManager.addPass(mlir::createLowerToLLVMPass());

    if (mlir::failed(passManager.run(*module))) {
        return false;
    }
    return true;
}


int MLIRUtility::loadAndProcessMLIR(std::shared_ptr<IR::NESIR> nesIR, DebugFlags* , bool useSCF) {

    // Todo find a good place to load the required Dialects
    // Load all Dialects required to process/generate the required MLIR.
    context.loadDialect<mlir::StandardOpsDialect>();
    context.loadDialect<mlir::LLVM::LLVMDialect>();
    context.loadDialect<mlir::scf::SCFDialect>();
    context.loadDialect<mlir::math::MathDialect>();
    context.loadDialect<mlir::AffineDialect>();
    context.loadDialect<mlir::vector::VectorDialect>();
    context.loadDialect<mlir::memref::MemRefDialect>();
    // passManager.addPass(mlir::createConvertVectorToLLVMPass());
    // Generate MLIR for the sample NESAbstraction or load from file.
    if (debugFromFile) {
        assert(!mlirFilepath.empty() && "No MLIR filename to read from given.");
        std::cout << "Loading Module from File!\n";
        module = mlir::parseSourceFile(mlirFilepath, &context);
    } else {
        // create NESAbstraction for testing and an MLIRGeneratoruseSCF
        // Insert functions necessary to get information from TupleBuffer objects and more.
        // auto memberFunctions = MLIRGenerator::GetMemberFunctions(context);
        auto memberFunctions = std::vector<mlir::FuncOp>{};
        mlirGenerator = std::make_shared<MLIRGenerator>(context, memberFunctions);
        mlirGenerator->useSCF = useSCF;
        module = mlirGenerator->generateModuleFromNESIR(nesIR);
       // printMLIRModule(module, debugFlags);
    }

    if (!module) {
        llvm::errs() << "NESMLIRGenerator::loadMLIR: Could not load MLIR module" << '\n';
        return 1;
    }

    // Todo encapsulate print and pass functionality
    // Todo #54 establish proper optimization pipeline (acknowledge debugging)
    // Apply any generic pass manager command line options and run the pipeline.
    mlir::PassManager passManager(&context);
    applyPassManagerCLOptions(passManager);
    // passManager.addPass(mlir::createInlinerPass());
    passManager.addPass(mlir::createLowerAffinePass());
    passManager.addPass(mlir::createLowerToCFGPass());//SCF
    passManager.addPass(mlir::createConvertVectorToLLVMPass());
    mlir::LowerToLLVMOptions llvmLoweringOptions(&context);
    llvmLoweringOptions.emitCWrappers = true;
    passManager.addPass(mlir::createLowerToLLVMPass(llvmLoweringOptions));
    passManager.addPass(mlir::createMemRefToLLVMPass()); //Needs to be second to last for unrealized pass to work
    passManager.addPass(mlir::createReconcileUnrealizedCastsPass());
    
    if (mlir::failed(passManager.run(*module))) {
        return 1;
    }
    return 0;
}


llvm::function_ref<llvm::Error(llvm::Module*)> MLIRUtility::getOptimizingTransformer(bool linkProxyFunctions) {
    if (linkProxyFunctions) {
        return [] (llvm::Module* llvmIRModule) mutable {
            llvm::SMDiagnostic Err;
            auto timer = std::make_shared<Timer<>>("LLVM IR Module Optimization");
            timer->start();
            // PROXY_FUNCTIONS_RESULT_DIR is a CMake paramater that holds the path to the  proxy functions file.
            auto proxyFunctionsIR = llvm::parseIRFile(std::string(PROXY_FUNCTIONS_RESULT_DIR),
                                                      Err, llvmIRModule->getContext());
            timer->snapshot("Proxy Module Parsed");
            
            llvm::Linker::linkModules(*llvmIRModule, std::move(proxyFunctionsIR));
            timer->snapshot("Proxy Module Linked");

            //Todo we probably need to provide a TargetMachine to enable e.g. AVX optimizations
            // -> no available option to provide info on avx
            // TargetMachine(const Target &T, StringRef DataLayoutString, const Triple &TargetTriple, 
            // StringRef CPU, StringRef FS, const TargetOptions &Options);
            
            auto optPipeline = mlir::makeOptimizingTransformer(3, 3, nullptr);
            auto optimizedModule = optPipeline(llvmIRModule);
            timer->snapshot("Linked Module Optimized");

            timer->pause();
            std::string timerString;
            for(auto snapshot : timer->getSnapshots()) {
                timerString += std::to_string(snapshot.getPrintTime()) + ',';
            }
            timerString += '\n';

            // Write inlining results to folder where benchmark is executed (will be removed later).
            std::ofstream fs("llvmLambda.csv", std::ios::app);
            if(fs.is_open()) { 
                fs.write(timerString.c_str(), timerString.size());
            }

           std::string llvmIRString;
           llvm::raw_string_ostream llvmStringStream(llvmIRString);
           llvmIRModule->print(llvmStringStream, nullptr);

           auto* basicError = new std::error_code();
           //Todo Also use CMake parameter for generated file.
           llvm::raw_fd_ostream fileStream("generated.ll", *basicError);
           fileStream.write(llvmIRString.c_str(), llvmIRString.length());
            return optimizedModule;
        };
    } else {
        std::cout << "Proxy Inlining: No'\n'";
        return [](llvm::Module* llvmIRModule) {
            //Todo  right now, we do not utilize the available 'call site attributes', instead, the attributes for execute are empty
            // -> we could extract the line with attribute '#0' -> execute attribute, and replace it with the fully available attribute list
            // -> only for testing, we need to find a more robust method in the future
            // DOES NOT WORK! Attributes are overwritten again
            // std::string cpuInfo = R"cpu("target-cpu"="skylake")cpu"; 
            // std::string targetFeatures = R"features("target-features"="+64bit,+adx,+aes,+avx,+avx2,+bmi,+bmi2,+clflushopt,+cmov,+crc32,+cx16,+cx8,+f16c,+fma,+fsgsbase,+fxsr,+invpcid,+lzcnt,+mmx,+movbe,+pclmul,+popcnt,+prfchw,+rdrnd,+rdseed,+sahf,+sgx,+sse,+sse2,+sse3,+sse4.1,+sse4.2,+ssse3,+x87,+xsave,+xsavec,+xsaveopt,+xsaves,-amx-bf16,-amx-int8,-amx-tile,-avx512bf16,-avx512bitalg,-avx512bw,-avx512cd,-avx512dq,-avx512er,-avx512f,-avx512fp16,-avx512ifma,-avx512pf,-avx512vbmi,-avx512vbmi2,-avx512vl,-avx512vnni,-avx512vp2intersect,-avx512vpopcntdq,-avxvnni,-cldemote,-clwb,-clzero,-enqcmd,-fma4,-gfni,-hreset,-kl,-lwp,-movdir64b,-movdiri,-mwaitx,-pconfig,-pku,-prefetchwt1,-ptwrite,-rdpid,-rtm,-serialize,-sha,-shstk,-sse4a,-tbm,-tsxldtrk,-uintr,-vaes,-vpclmulqdq,-waitpkg,-wbnoinvd,-widekl,-xop")features"; 
            // llvm::Triple newTriple;
            // newTriple.setTriple("x86_64-pc-linux-gnu");
            // std::string dataLayout = "e-m:e-p270:32:32-p271:32:32-p272:64:64-i64:64-f80:128-n8:16:32:64-S128";

            // llvm::StringMap<bool> FeatureMap;
            // llvm::sys::getHostCPUFeatures(FeatureMap);
            
            // std::string targetFeatures;
            // for(auto string : FeatureMap.keys()) {
            //     if(FeatureMap[string]) {
            //         targetFeatures += '+' + string.str() + ',';
            //     }
            // }
            // std::string errorString = "Target not found";
            // auto target = llvm::TargetRegistry::lookupTarget("x86_64-unknown-linux-gnu", errorString);
            // llvm::TargetOptions newTargetOptions;
            // llvm::Optional<llvm::Reloc::Model> relocModel;
            // auto targetMachine = target->createTargetMachine("x86_64-pc-linux-gnu", "skylake", targetFeatures, newTargetOptions, relocModel);
            // std::function<llvm::Error (llvm::Module *)> optPipeline;
            std::function<llvm::Error (llvm::Module *)> optPipeline = mlir::makeOptimizingTransformer(3, 3, nullptr);;

            // if(targetMachine) {
            //     std::cout << "Using Target Machine\n";
            //     optPipeline = mlir::makeOptimizingTransformer(3, 3, targetMachine);
            // } else {
            //     std::cout << "Not using Target Machine\n";
            //     // optPipeline = mlir::makeOptimizingTransformer(3, 3, targetMachine);
            // }
            auto optimizedModule = optPipeline(llvmIRModule);

            // llvmIRModule->print(llvm::outs(), nullptr);
            std::string llvmIRString;
            llvm::raw_string_ostream llvmStringStream(llvmIRString);
            llvmIRModule->print(llvmStringStream, nullptr);

            auto* basicError = new std::error_code();
            //Todo Also use CMake parameter for generated file.
            llvm::raw_fd_ostream fileStream("generated.ll", *basicError);
            fileStream.write(llvmStringStream.str().c_str(), llvmStringStream.str().length());
            return optimizedModule;
        };
    }
}

std::unique_ptr<mlir::ExecutionEngine> MLIRUtility::prepareEngine(bool linkProxyFunctions, std::shared_ptr<Timer<>> parentTimer) {
    if(!parentTimer) {
        parentTimer = std::make_shared<Timer<>>("CompilationBasedPipelineExecutionEngine");
    }
    //Todo move
    std::unordered_set<std::string> ExtractFuncs{
        "getMurMurHash",
        "getCRC32Hash",
        "stringToUpperCase",
        "standardDeviationGetMean",
        "standardDeviationGetVariance",
        "standardDeviationGetStdDev",
        "NES__QueryCompiler__PipelineContext__getGlobalOperatorStateProxy",
        "NES__Runtime__TupleBuffer__getNumberOfTuples",
        "NES__Runtime__TupleBuffer__setNumberOfTuples",
        "NES__Runtime__TupleBuffer__getBuffer",
        "NES__Runtime__TupleBuffer__getBufferSize",
        "NES__Runtime__TupleBuffer__getWatermark",
        "NES__Runtime__TupleBuffer__setWatermark",
        "NES__Runtime__TupleBuffer__getCreationTimestamp",
        "NES__Runtime__TupleBuffer__setSequenceNumber",
        "NES__Runtime__TupleBuffer__getSequenceNumber",
        "NES__Runtime__TupleBuffer__setCreationTimestamp"
        // "NES__QueryCompiler__PipelineContext__emitBufferProxy",
    };

        
    // Initialize LLVM targets. This can e.g. lead to registering a x86 target machine, with that machine's features.
    // https://llvm.org/doxygen/X86TargetMachine_8cpp_source.html#l00067
    llvm::InitializeNativeTarget();
    llvm::InitializeNativeTargetAsmPrinter();

    // Attempt at registering sub target info for existing target(machine)
    // std::string errorString = "Target not found";
    // auto testTarget = *llvm::TargetRegistry::targets().begin()++;
    // llvm::Target::MCSubtargetInfoCtorFnTy test = (llvm::MCSubtargetInfo *(*)(const llvm::Triple&, llvm::StringRef, llvm::StringRef)) 
    //     llvm::TargetRegistry::lookupTarget("x86_64-unknown-linux-gnu", errorString)->createMCSubtargetInfo("x86_64-pc-linux-gnu", "skylake", "+avx2");
    // llvm::TargetRegistry::RegisterMCSubtargetInfo(testTarget, test);

    // Register and execute the translation from MLIR to LLVM IR, which must happen before we can JIT-compile.
    mlir::registerLLVMDialectTranslation(*module->getContext());
    parentTimer->snapshot("Lowering to LLVM IR");

    /// Link proxyFunctions into MLIR module. Optimize MLIR module.
    llvm::function_ref<llvm::Error(llvm::Module*)> printOptimizingTransformer = getOptimizingTransformer(linkProxyFunctions);
    

    // Create an MLIR execution engine. The execution engine eagerly JIT-compiles the module.
    auto maybeEngine =
        //Todo: does disabling debug info improve compilation times?
        mlir::ExecutionEngine::create(*module, nullptr, printOptimizingTransformer, llvm::CodeGenOpt::Level::Aggressive);
        // mlir::ExecutionEngine::create(*module, nullptr, printOptimizingTransformer, llvm::CodeGenOpt::Level::Aggressive, {}, false, false, false);
    assert(maybeEngine && "failed to construct an execution engine");
    auto& engine = maybeEngine.get();
    parentTimer->snapshot("JIT Compilation");

    //Todo #2936: do we need to register symbols and addresses
    if(mlirGenerator && !mlirGenerator->getJitProxyFunctionSymbols().empty()) {
        auto runtimeSymbolMap = [&](llvm::orc::MangleAndInterner interner) {
            auto symbolMap = llvm::orc::SymbolMap();
            for (int i = 0; i < (int) mlirGenerator->getJitProxyFunctionSymbols().size(); ++i) {
                if(!linkProxyFunctions || !ExtractFuncs.contains(mlirGenerator->getJitProxyFunctionSymbols().at(i))) {
                    symbolMap[interner(mlirGenerator->getJitProxyFunctionSymbols().at(i))] =
                        llvm::JITEvaluatedSymbol(mlirGenerator->getJitProxyTargetAddresses().at(i), llvm::JITSymbolFlags::Callable);
                }
            }
            return symbolMap;
        };
        engine->registerSymbols(runtimeSymbolMap);
    }
    parentTimer->snapshot("JIT Symbols Registered");

    // Invoke the JIT-compiled function.
    int64_t result = 0;
    return std::move(engine);
}

/**
 * @brief Takes loaded and lowered MLIR module, jit compiles it and calls 'execute()'
 * @param module: MLIR module that contains the 'execute' function.
 * @return int: 1 if error occurred, else 0
 */
int MLIRUtility::runJit(bool linkProxyFunctions, void* inputBufferPtr, void* outputBufferPtr) {
    // Initialize LLVM targets.
    llvm::InitializeNativeTarget();
    llvm::InitializeNativeTargetAsmPrinter();


    // Register the translation from MLIR to LLVM IR, which must happen before we can JIT-compile.
    mlir::registerLLVMDialectTranslation(*module->getContext());

    /// Link proxyFunctions into MLIR module. Optimize MLIR module.
    llvm::function_ref<llvm::Error(llvm::Module*)> printOptimizingTransformer = getOptimizingTransformer(linkProxyFunctions);

    // Create an MLIR execution engine. The execution engine eagerly JIT-compiles the module.
    auto maybeEngine =
        mlir::ExecutionEngine::create(*module, nullptr, printOptimizingTransformer, llvm::CodeGenOpt::Level::Aggressive);
    assert(maybeEngine && "failed to construct an execution engine");
    auto& engine = maybeEngine.get();

    auto runtimeSymbolMap = [&](llvm::orc::MangleAndInterner interner) {
        auto symbolMap = llvm::orc::SymbolMap();
        for (int i = 0; i < (int) mlirGenerator->getJitProxyFunctionSymbols().size(); ++i) {
            symbolMap[interner(mlirGenerator->getJitProxyFunctionSymbols().at(i))] =
                llvm::JITEvaluatedSymbol(mlirGenerator->getJitProxyTargetAddresses().at(i), llvm::JITSymbolFlags::Callable);
        }
        return symbolMap;
    };
    engine->registerSymbols(runtimeSymbolMap);

    // Invoke the JIT-compiled function.
    int64_t result = 0;
    auto invocationResult =
        engine->invoke("execute", inputBufferPtr, outputBufferPtr, mlir::ExecutionEngine::Result<int64_t>(result));
    printf("Result: %ld\n", result);

    if (invocationResult) {
        llvm::errs() << "JIT invocation failed\n";
        return -1;
    }

    return (bool) inputBufferPtr + (bool) outputBufferPtr;
}
}// namespace NES::ExecutionEngine::Experimental::MLIR