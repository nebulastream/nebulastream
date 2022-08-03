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
#include <llvm/ExecutionEngine/ExecutionEngine.h>
#include <llvm/ExecutionEngine/JITSymbol.h>
#include <llvm/ExecutionEngine/Orc/Mangling.h>
#include <llvm/Support/CommandLine.h>
#include <llvm/Support/FileCollector.h>
#include <llvm/Transforms/IPO/SCCP.h>
#include <llvm/IRReader/IRReader.h>
#include <llvm/Linker/Linker.h>
#include <llvm/Support/SourceMgr.h>
#include <llvm/Support/TargetSelect.h>
#include <mlir/Conversion/SCFToStandard/SCFToStandard.h>
#include <mlir/Conversion/StandardToLLVM/ConvertStandardToLLVMPass.h>
#include <mlir/Dialect/LLVMIR/LLVMDialect.h>
#include <mlir/Dialect/SCF/SCF.h>
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

    auto memberFunctions = MLIRGenerator::GetMemberFunctions(context);
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
    // Generate MLIR for the sample NESAbstraction or load from file.
    if (debugFromFile) {
        assert(!mlirFilepath.empty() && "No MLIR filename to read from given.");
        module = mlir::parseSourceFile(mlirFilepath, &context);
    } else {
        // create NESAbstraction for testing and an MLIRGenerator
        // Insert functions necessary to get information from TupleBuffer objects and more.
        auto memberFunctions = MLIRGenerator::GetMemberFunctions(context);
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
    passManager.addPass(mlir::createInlinerPass());
    passManager.addPass(mlir::createLowerToCFGPass());
    passManager.addPass(mlir::createLowerToLLVMPass());

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
            std::ofstream fs("inlining.csv", std::ios::app);
            if(fs.is_open()) { 
                fs.write(timerString.c_str(), timerString.size());
            }

            // std::string llvmIRString;
            // llvm::raw_string_ostream llvmStringStream(llvmIRString);
            // llvmIRModule->print(llvmStringStream, nullptr);

            // auto* basicError = new std::error_code();
            // //Todo Also use CMake parameter for generated file.
            // llvm::raw_fd_ostream fileStream("../../../../llvm-ir/nes-runtime_opt/generated.ll", *basicError);
            // fileStream.write(llvmIRString.c_str(), llvmIRString.length());
            return optimizedModule;
        };
    } else {
        return [](llvm::Module* llvmIRModule) {
            auto optPipeline = mlir::makeOptimizingTransformer(3, 3, nullptr);
            auto optimizedModule = optPipeline(llvmIRModule);
            // llvmIRModule->print(llvm::outs(), nullptr);
            return optimizedModule;
        };
    }
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

std::unique_ptr<mlir::ExecutionEngine> MLIRUtility::prepareEngine(bool linkProxyFunctions, std::shared_ptr<Timer<>> parentTimer) {
    if(!parentTimer) {
        parentTimer = std::make_shared<Timer<>>("CompilationBasedPipelineExecutionEngine");
    }
    //Todo move
    std::unordered_set<std::string> ExtractFuncs{
        "getHash",
        "NES__QueryCompiler__PipelineContext__getGlobalOperatorStateProxy", 
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

    // Initialize LLVM targets.
    llvm::InitializeNativeTarget();
    llvm::InitializeNativeTargetAsmPrinter();

    // Register and execute the translation from MLIR to LLVM IR, which must happen before we can JIT-compile.
    mlir::registerLLVMDialectTranslation(*module->getContext());
    parentTimer->snapshot("Lowering to LLVM IR");

    /// Link proxyFunctions into MLIR module. Optimize MLIR module.
    llvm::function_ref<llvm::Error(llvm::Module*)> printOptimizingTransformer = getOptimizingTransformer(linkProxyFunctions);
    

    // Create an MLIR execution engine. The execution engine eagerly JIT-compiles the module.
    auto maybeEngine =
        mlir::ExecutionEngine::create(*module, nullptr, printOptimizingTransformer, llvm::CodeGenOpt::Level::Aggressive);
    assert(maybeEngine && "failed to construct an execution engine");
    auto& engine = maybeEngine.get();
    parentTimer->snapshot("JIT Compilation");

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
    parentTimer->snapshot("JIT Symbols Registered");

    // Invoke the JIT-compiled function.
    int64_t result = 0;
    return std::move(engine);
}
}// namespace NES::ExecutionEngine::Experimental::MLIR