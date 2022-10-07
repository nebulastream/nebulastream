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

#include <Nautilus/Backends/MLIR/MLIRPassManager.hpp>
#include <mlir/Conversion/SCFToStandard/SCFToStandard.h>
#include <mlir/Conversion/StandardToLLVM/ConvertStandardToLLVMPass.h>
#include <mlir/ExecutionEngine/OptUtils.h>
#include <mlir/IR/MLIRContext.h>
#include <mlir/Pass/PassManager.h>
#include <mlir/Transforms/Passes.h>
// MLIR Lowering Passes
#include <mlir/Conversion/LLVMCommon/LoweringOptions.h>
#include <mlir/Conversion/VectorToLLVM/ConvertVectorToLLVM.h>
#include <mlir/Conversion/MemRefToLLVM/MemRefToLLVM.h>
#include <mlir/Conversion/AffineToStandard/AffineToStandard.h>
#include "mlir/Conversion/ReconcileUnrealizedCasts/ReconcileUnrealizedCasts.h"
// MLIR Opt passes
#include <mlir/Dialect/Affine/Passes.h>
#include <mlir/Dialect/SCF/Passes.h>

namespace NES::Nautilus::Backends::MLIR {
/**
 * @brief Takes a LoweringPass Enum and returns the corresponding mlir lowering pass.
 * 
 * @param loweringPass: Used to get the correct mlir lowering pass.
 * @return std::unique_ptr<mlir::Pass>: MLIR lowering pass corresponding to supplied Enum.
 */
std::unique_ptr<mlir::Pass> getMLIRLoweringPass(MLIRPassManager::LoweringPass loweringPass) {
    switch(loweringPass) {
        case MLIRPassManager::LLVM:
            return mlir::createLowerToLLVMPass();
        case MLIRPassManager::SCF:
            return mlir::createLowerToCFGPass();
    }
}

/**
 * @brief Takes a OptimizationPass Enum and returns the corresponding mlir optimization pass.
 * 
 * @param optimizationPass: Used to get the correct mlir optimization pass.
 * @return std::unique_ptr<mlir::Pass>: MLIR optimization pass corresponding to supplied Enum.
 */
std::unique_ptr<mlir::Pass> getMLIROptimizationPass(MLIRPassManager::OptimizationPass optimizationPass) {
    switch(optimizationPass) {
        case MLIRPassManager::OptimizationPass::Inline:
            return mlir::createInlinerPass();
    }
}

//Todo: might require new context? -> mlir::MLIRContext *context
int MLIRPassManager::lowerAndOptimizeMLIRModule(mlir::OwningOpRef<mlir::ModuleOp> &module, 
                                                std::vector<LoweringPass> loweringPasses, 
                                                std::vector<OptimizationPass> optimizationPasses,
                                                std::shared_ptr<Timer<>> timer) {
    mlir::PassManager passManager(module->getContext());
    applyPassManagerCLOptions(passManager);

    // Apply optimization passes.
    if(!optimizationPasses.empty()) {
        for(auto optimizationPass : optimizationPasses) {
            passManager.addPass(getMLIROptimizationPass(optimizationPass));    
        }
    } else {
        // passManager.addPass(mlir::createInlinerPass());
        // passManager.addPass(mlir::createLoopInvariantCodeMotionPass());
        // passManager.addPass(mlir::createCSEPass());
        // passManager.addPass(mlir::createControlFlowSinkPass());
        // passManager.addPass(mlir::createCanonicalizerPass());
        // passManager.addPass(mlir::createAffineParallelizePass()); //Either below or this
        // passManager.addPass(mlir::createSuperVectorizePass(8));
        // passManager.addPass(mlir::createLoopUnrollPass());
        // passManager.addPass(mlir::createLoopTilingPass());
        // passManager.addPass(mlir::createLoopFusionPass());
        // passManager.addPass(mlir::createForLoopPeelingPass());
        // passManager.addPass(mlir::createSCFForLoopCanonicalizationPass());
        // passManager.addPass(mlir::createForLoopSpecializationPass());
    }
    // Run Optimization passes.
    if (mlir::failed(passManager.run(*module))) {
        llvm::errs() << "MLIRPassManager::lowerAndOptimizeMLIRModule: Failed to apply passes to generated MLIR" << '\n';
        return 1;
    }
    if(timer) {timer->snapshot("MLIR Optimization");}
    passManager.clear();

    // Set up lowering passes.
    if(!loweringPasses.empty()) {
        for(auto loweringPass : loweringPasses) {
            passManager.addPass(getMLIRLoweringPass(loweringPass));
        }
    } else {
        passManager.addPass(mlir::createLowerAffinePass());
        passManager.addPass(mlir::createLowerToCFGPass()); //SCF
        passManager.addPass(mlir::createConvertVectorToLLVMPass());
        mlir::LowerToLLVMOptions llvmLoweringOptions(module->getContext()); 
        llvmLoweringOptions.emitCWrappers = true;
        passManager.addPass(mlir::createLowerToLLVMPass(llvmLoweringOptions));
        // passManager.addPass(mlir::createLowerToLLVMPass());
        passManager.addPass(mlir::createMemRefToLLVMPass()); //Needs to be second to last for unrealized pass to work
        passManager.addPass(mlir::createReconcileUnrealizedCastsPass());
    }
    // Run Lowering passes.
    if (mlir::failed(passManager.run(*module))) {
        llvm::errs() << "MLIRPassManager::lowerAndOptimizeMLIRModule: Failed to apply passes to generated MLIR" << '\n';
        return 1;
    }
    if(timer) {timer->snapshot("MLIR Lowering");}
    return 0;
}
}// namespace NES::Nautilus::Backends::MLIR

    