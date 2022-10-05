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

#include "Util/Logger/Logger.hpp"
#include <Nautilus/Backends/MLIR/MLIRUtility.hpp>
#include <Nautilus/Backends/MLIR/MLIRLoweringProvider.hpp>
#include <Nautilus/Backends/MLIR/MLIRPassManager.hpp>
#include <Nautilus/Backends/MLIR/LLVMIROptimizer.hpp>
#include <Nautilus/Backends/MLIR/JITCompiler.hpp>
#include <mlir/Dialect/LLVMIR/LLVMDialect.h>
#include <mlir/Dialect/SCF/SCF.h>
#include <mlir/Dialect/Math/IR/Math.h>
#include <mlir/Dialect/Vector/IR/VectorOps.h>
#include <mlir/Dialect/MemRef/IR/MemRef.h>
#include <mlir/Dialect/Affine/IR/AffineOps.h>
#include <mlir/IR/MLIRContext.h>
#include <mlir/Parser.h>


namespace NES::Nautilus::Backends::MLIR {

void MLIRUtility::writeMLIRModuleToFile(mlir::OwningOpRef<mlir::ModuleOp>& mlirModule, std::string mlirFilepath) {    
    std::string mlirString;
    llvm::raw_string_ostream llvmStringStream(mlirString);
    auto* basicError = new std::error_code();
    llvm::raw_fd_ostream fileStream(mlirFilepath, *basicError);

    auto* opPrintFlags = new mlir::OpPrintingFlags();
    mlirModule->print(llvmStringStream, *opPrintFlags);
    if (!mlirFilepath.empty()) {
        fileStream.write(mlirString.c_str(), mlirString.length());
    }
    std::cout << mlirString.c_str() << std::endl;
}

int MLIRUtility::loadAndExecuteModuleFromString(const std::string& mlirString, const std::string& moduleString) {
    mlir::MLIRContext context;
    mlir::OwningOpRef<mlir::ModuleOp> module;
    module = mlir::parseSourceString(mlirString, &context);

    // Take the MLIR module from the MLIRLoweringProvider and apply lowering and optimization passes.
    if(!MLIR::MLIRPassManager::lowerAndOptimizeMLIRModule(module, {}, {})) {
        NES_FATAL_ERROR("Could not lower and optimize MLIR");
    }

    // Lower MLIR module to LLVM IR and create LLVM IR optimization pipeline.
    auto optPipeline = MLIR::LLVMIROptimizer::getLLVMOptimizerPipeline(MLIR::LLVMIROptimizer::O3, /*inlining*/ false);

    // JIT compile LLVM IR module and return engine that provides access compiled execute function.
    auto engine = MLIR::JITCompiler::jitCompileModule(module, optPipeline, {}, {});
    if(!engine->invoke(moduleString)) {
        return -1;
    }
    else return 0;
}

std::unique_ptr<mlir::ExecutionEngine> 
MLIRUtility::compileNESIRToMachineCode(std::shared_ptr<NES::Nautilus::IR::IRGraph> ir) {
    mlir::MLIRContext context;
    auto loweringProvider = std::make_unique<MLIR::MLIRLoweringProvider>(context);
    auto module = loweringProvider->generateModuleFromNESIR(ir);
    // Take the MLIR module from the MLIRLoweringProvider and apply lowering and optimization passes.
    if(MLIR::MLIRPassManager::lowerAndOptimizeMLIRModule(module, {}, {})) {
        NES_FATAL_ERROR("Could not lower and optimize MLIR");
    }

    // Lower MLIR module to LLVM IR and create LLVM IR optimization pipeline.
    auto optPipeline = MLIR::LLVMIROptimizer::getLLVMOptimizerPipeline(MLIR::LLVMIROptimizer::O3, /*inlining*/ false);

    // JIT compile LLVM IR module and return engine that provides access compiled execute function.
    return MLIR::JITCompiler::jitCompileModule(module, optPipeline, loweringProvider->getJitProxyFunctionSymbols(), 
                                                loweringProvider->getJitProxyTargetAddresses());
}

mlir::OwningOpRef<mlir::ModuleOp>
MLIRUtility::loadMLIRModuleFromFilePath(const std::string &mlirFilePath, mlir::MLIRContext &context) {
    // mlir::MLIRContext context;
    context.loadDialect<mlir::StandardOpsDialect>();
    context.loadDialect<mlir::LLVM::LLVMDialect>();
    context.loadDialect<mlir::scf::SCFDialect>();
    context.loadDialect<mlir::math::MathDialect>();
    context.loadDialect<mlir::AffineDialect>();
    context.loadDialect<mlir::vector::VectorDialect>();
    context.loadDialect<mlir::memref::MemRefDialect>();
    assert(!mlirFilePath.empty() && "No MLIR filename to read from given.");
    mlir::OwningOpRef<mlir::ModuleOp> module;
    module = mlir::parseSourceFile(mlirFilePath, &context);
    return module;
}

mlir::OwningOpRef<mlir::ModuleOp>
MLIRUtility::loadMLIRModuleFromNESIR(std::shared_ptr<NES::Nautilus::IR::IRGraph> ir, mlir::MLIRContext &context) {
    // mlir::MLIRContext context;
    context.loadDialect<mlir::StandardOpsDialect>();
    context.loadDialect<mlir::LLVM::LLVMDialect>();
    context.loadDialect<mlir::scf::SCFDialect>();
    context.loadDialect<mlir::math::MathDialect>();
    context.loadDialect<mlir::AffineDialect>();
    context.loadDialect<mlir::vector::VectorDialect>();
    context.loadDialect<mlir::memref::MemRefDialect>();
    auto loweringProvider = std::make_unique<MLIR::MLIRLoweringProvider>(context);
    auto module = loweringProvider->generateModuleFromNESIR(ir);
    // Take the MLIR module from the MLIRLoweringProvider and apply lowering and optimization passes.
    // if(parentTimer) { parentTimer->snapshot("MLIR Generation"); }
    // if(MLIR::MLIRPassManager::lowerAndOptimizeMLIRModule(module, {}, {}, parentTimer)) {
    //     NES_FATAL_ERROR("Could not lower and optimize MLIR");
    // }
    return module;
}

std::unique_ptr<mlir::ExecutionEngine> 
MLIRUtility::lowerAndCompileMLIRModuleToMachineCode(mlir::OwningOpRef<mlir::ModuleOp> &module, 
                                            MLIR::LLVMIROptimizer::OptimizationLevel optLevel, bool inlining,
                                            std::shared_ptr<Timer<>> parentTimer) {
    if(MLIR::MLIRPassManager::lowerAndOptimizeMLIRModule(module, {}, {}, parentTimer)) {
        NES_FATAL_ERROR("Could not lower and optimize MLIR");
    }

    // Lower MLIR module to LLVM IR and create LLVM IR optimization pipeline.
    auto optPipeline = MLIR::LLVMIROptimizer::getLLVMOptimizerPipeline(optLevel, inlining);

    // JIT compile LLVM IR module and return engine that provides access compiled execute function.
    auto loweringProvider = std::make_unique<MLIR::MLIRLoweringProvider>(*module->getContext());
    return MLIR::JITCompiler::jitCompileModule(module, optPipeline, loweringProvider->getJitProxyFunctionSymbols(), 
                                                loweringProvider->getJitProxyTargetAddresses());
}

}// namespace NES::Nautilus::Backends::MLIR