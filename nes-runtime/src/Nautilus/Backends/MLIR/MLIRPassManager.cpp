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
#include <mlir/Conversion/ControlFlowToLLVM/ControlFlowToLLVM.h>
#include <mlir/Conversion/FuncToLLVM/ConvertFuncToLLVMPass.h>
#include <mlir/Conversion/SCFToControlFlow/SCFToControlFlow.h>
#include <mlir/ExecutionEngine/OptUtils.h>
#include <mlir/IR/MLIRContext.h>
#include <mlir/Pass/PassManager.h>
#include <mlir/Transforms/Passes.h>

namespace NES::Nautilus::Backends::MLIR {
/**
 * @brief Takes a LoweringPass Enum and returns the corresponding mlir lowering pass.
 * 
 * @param loweringPass: Used to get the correct mlir lowering pass.
 * @return std::unique_ptr<mlir::Pass>: MLIR lowering pass corresponding to supplied Enum.
 */
std::unique_ptr<mlir::Pass> getMLIRLoweringPass(MLIRPassManager::LoweringPass loweringPass) {
    switch (loweringPass) {
        case MLIRPassManager::LoweringPass::LLVM: return mlir::cf::createConvertControlFlowToLLVMPass();
        case MLIRPassManager::LoweringPass::SCF: return mlir::createConvertSCFToCFPass();
    }
}

/**
 * @brief Takes a OptimizationPass Enum and returns the corresponding mlir optimization pass.
 * 
 * @param optimizationPass: Used to get the correct mlir optimization pass.
 * @return std::unique_ptr<mlir::Pass>: MLIR optimization pass corresponding to supplied Enum.
 */
std::unique_ptr<mlir::Pass> getMLIROptimizationPass(MLIRPassManager::OptimizationPass optimizationPass) {
    switch (optimizationPass) {
        case MLIRPassManager::OptimizationPass::Inline: return mlir::createInlinerPass();
    }
}

int MLIRPassManager::lowerAndOptimizeMLIRModule(mlir::OwningOpRef<mlir::ModuleOp>& module,
                                                std::vector<LoweringPass> loweringPasses,
                                                std::vector<OptimizationPass> optimizationPasses) {
    mlir::PassManager passManager(module->getContext());
    applyPassManagerCLOptions(passManager);

    // Apply optimization passes.
    if (!optimizationPasses.empty()) {
        for (auto optimizationPass : optimizationPasses) {
            passManager.addPass(getMLIROptimizationPass(optimizationPass));
        }
    } else {
        passManager.addPass(mlir::createInlinerPass());
    }
    // Apply lowering passes.
    if (!loweringPasses.empty()) {
        for (auto loweringPass : loweringPasses) {
            passManager.addPass(getMLIRLoweringPass(loweringPass));
        }
    } else {
        passManager.addPass(mlir::createConvertSCFToCFPass());
        passManager.addPass(mlir::createConvertFuncToLLVMPass());
        passManager.addPass(mlir::cf::createConvertControlFlowToLLVMPass());
    }

    // Run passes.
    if (mlir::failed(passManager.run(*module))) {
        llvm::errs() << "MLIRPassManager::lowerAndOptimizeMLIRModule: Failed to apply passes to generated MLIR" << '\n';
        return 1;
    }
    return 0;
}
}// namespace NES::Nautilus::Backends::MLIR