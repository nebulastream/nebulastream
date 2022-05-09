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

#include <Experimental/MLIR/MLIRUtility.hpp>
#include <Experimental/MLIR/NESIRToMLIR.hpp>
#include <mlir/Target/LLVMIR/Export.h>
#include <string>

#include "mlir/Conversion/SCFToStandard/SCFToStandard.h"
#include "mlir/Conversion/StandardToLLVM/ConvertStandardToLLVMPass.h"
#include "mlir/ExecutionEngine/OptUtils.h"
#include "mlir/IR/OperationSupport.h"
#include "mlir/Dialect/LLVMIR/LLVMDialect.h"
#include "mlir/Dialect/SCF/SCF.h"
#include "mlir/Dialect/StandardOps/IR/Ops.h"
#include "mlir/Parser.h"
#include "mlir/Pass/PassManager.h"
#include "mlir/Target/LLVMIR/Dialect/LLVMIR/LLVMToLLVMIRTranslation.h"
#include "mlir/Target/LLVMIR/LLVMTranslationInterface.h"
#include "mlir/Transforms/Passes.h"
#include "llvm/ExecutionEngine/ExecutionEngine.h"
#include "llvm/ExecutionEngine/JITSymbol.h"
#include "llvm/Support/TargetSelect.h"

#include <llvm/Support/SourceMgr.h>
#include <utility>

#include "llvm/IRReader/IRReader.h"
#include "llvm/Linker/Linker.h"

#include <unistd.h>

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
        printf("%s", mlirString.c_str());
    }
}

int MLIRUtility::loadModuleFromString(const std::string& mlirString, DebugFlags* debugFlags) {
    context.loadDialect<mlir::StandardOpsDialect>();
    context.loadDialect<mlir::LLVM::LLVMDialect>();
    context.loadDialect<mlir::scf::SCFDialect>();
    module = parseSourceString(mlirString, &context);

    if(debugFlags) { printf("Kek %d", debugFlags->comments); }
//    auto opIterator = module->getOps().begin()->getBlock()->getOperations().begin();
//    opIterator->dump();
    module->dump();
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

    if(debugFlags) { printf("Kek %d", debugFlags->comments); }
//    auto opIterator = module2->getOps().begin()->getBlock()->getOperations().front();
//    module-> push_back(module2->getOps().begin()->getBlock()->getOperations().front().clone());
    module->dump();

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

int MLIRUtility::loadAndProcessMLIR(NES::NESIR* nesIR, DebugFlags* debugFlags) {

    // Todo find a good place to load the required Dialects
    // Load all Dialects required to process/generate the required MLIR.
    context.loadDialect<mlir::StandardOpsDialect>();
    context.loadDialect<mlir::LLVM::LLVMDialect>();
    context.loadDialect<mlir::scf::SCFDialect>();
//    context.loadDialect<>()
    // Generate MLIR for the sample NESAbstraction or load from file.
    if (debugFromFile) {
        assert(!mlirFilepath.empty() && "No MLIR filename to read from given.");
        module = mlir::parseSourceFile(mlirFilepath, &context);
    } else {
        // create NESAbstraction for testing and an MLIRGenerator
        // Insert functions necessary to get information from TupleBuffer objects and more.
        auto classMemberFunctions = MLIRGenerator::insertClassMemberFunctions(context);
        auto MLIRGen = new MLIRGenerator(context, classMemberFunctions);
        module = MLIRGen->generateModuleFromNESIR(nesIR);
        printMLIRModule(module, debugFlags);
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

/**
 * @brief Takes loaded and lowered MLIR module, jit compiles it and calls 'execute()'
 * @param module: MLIR module that contains the 'execute' function.
 * @return int: 1 if error occurred, else 0
 */
int MLIRUtility::runJit(const std::vector<std::string>& symbols, const std::vector<llvm::JITTargetAddress>& jitAddresses,
                        bool useProxyFunctions, void* inputBufferPtr, int8_t* outputBufferPtr) {
    // Initialize LLVM targets.
    llvm::InitializeNativeTarget();
    llvm::InitializeNativeTargetAsmPrinter();

    // Register the translation from MLIR to LLVM IR, which must happen before we can JIT-compile.
    mlir::registerLLVMDialectTranslation(*module->getContext());
    module->dump();

    /// Link proxyFunctions into MLIR module. Optimize MLIR module.
    llvm::function_ref<llvm::Error(llvm::Module*)> printOptimizingTransformer;
    if (useProxyFunctions) {
        printOptimizingTransformer = [](llvm::Module* llvmIRModule) {
            llvm::SMDiagnostic Err;
            //Todo change path
            auto proxyFunctionsIR =
                llvm::parseIRFile("/home/rudi/mlir/proxyFunctionsIR/proxyFunctionsIR.ll",
                                  Err,
                                  llvmIRModule->getContext());
            llvm::Linker::linkModules(*llvmIRModule, std::move(proxyFunctionsIR));
            auto optPipeline = mlir::makeOptimizingTransformer(3, 3, nullptr);
            auto optimizedModule = optPipeline(llvmIRModule);
            llvmIRModule->print(llvm::errs(), nullptr);
            return optimizedModule;
        };
    } else {
        printOptimizingTransformer = [](llvm::Module* llvmIRModule) {
            auto optPipeline = mlir::makeOptimizingTransformer(3, 3, nullptr);
            auto optimizedModule = optPipeline(llvmIRModule);
            llvmIRModule->print(llvm::errs(), nullptr);
            return optimizedModule;
        };
    }

    // Create an MLIR execution engine. The execution engine eagerly JIT-compiles the module.
    auto maybeEngine =
        mlir::ExecutionEngine::create(*module, nullptr, printOptimizingTransformer, llvm::CodeGenOpt::Level::Aggressive);
    assert(maybeEngine && "failed to construct an execution engine");
    auto& engine = maybeEngine.get();

    auto runtimeSymbolMap = [&](llvm::orc::MangleAndInterner interner) {
        auto symbolMap = llvm::orc::SymbolMap();
        for (int i = 0; i < (int) symbols.size(); ++i) {
            symbolMap[interner(symbols.at(i))] = llvm::JITEvaluatedSymbol(jitAddresses.at(i), llvm::JITSymbolFlags::Callable);
        }
        return symbolMap;
    };
    engine->registerSymbols(runtimeSymbolMap);

    // Invoke the JIT-compiled function.
    int64_t result = 0;
    auto invocationResult = engine->invoke("execute", inputBufferPtr, outputBufferPtr, mlir::ExecutionEngine::Result<int64_t>(result));
    printf("Result: %ld\n", result);

    if (invocationResult) {
        llvm::errs() << "JIT invocation failed\n";
        return -1;
    }

    return 0;
}