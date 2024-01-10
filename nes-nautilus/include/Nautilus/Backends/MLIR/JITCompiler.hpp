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

#ifndef NES_NAUTILUS_INCLUDE_NAUTILUS_BACKENDS_MLIR_JITCOMPILER_HPP_
#define NES_NAUTILUS_INCLUDE_NAUTILUS_BACKENDS_MLIR_JITCOMPILER_HPP_

#include <Nautilus/Backends/MLIR/MLIRLoweringProvider.hpp>
#include <Nautilus/Util/CompilationOptions.hpp>
#include <Util/DumpHelper.hpp>
#include <llvm/IR/Module.h>
#include <mlir/ExecutionEngine/ExecutionEngine.h>
#include <mlir/IR/BuiltinOps.h>
#include <mlir/Pass/Pass.h>
#include <vector>

namespace NES::Nautilus::Backends::MLIR {

/**
 * @brief The JITCompiler takes a generated MLIR module, 
 * and applies configured lowering & optimization passes to it.
 */
class JITCompiler {
  public:
    JITCompiler(); // Disable default constructor
    ~JITCompiler();// Disable default destructor

    static std::unique_ptr<mlir::ExecutionEngine>
    jitCompileModule(mlir::OwningOpRef<mlir::ModuleOp>& mlirModule,
                     const llvm::function_ref<llvm::Error(llvm::Module*)> optPipeline,
                     const std::vector<std::string>& jitProxyFunctionSymbols,
                     const std::vector<llvm::JITTargetAddress>& jitProxyFunctionTargetAddresses,
                     const CompilationOptions& options,
                     const DumpHelper& dumpHelper);
};
}// namespace NES::Nautilus::Backends::MLIR
#endif // NES_NAUTILUS_INCLUDE_NAUTILUS_BACKENDS_MLIR_JITCOMPILER_HPP_
