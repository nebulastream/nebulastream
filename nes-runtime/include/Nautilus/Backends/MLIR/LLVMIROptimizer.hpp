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

#ifndef NES_RUNTIME_INCLUDE_NAUTILUS_BACKENDS_MLIR_LLVMIROPTIMIZER_HPP_
#define NES_RUNTIME_INCLUDE_NAUTILUS_BACKENDS_MLIR_LLVMIROPTIMIZER_HPP_

namespace NES {
class DumpHelper;
namespace Nautilus {
class CompilationOptions;
}
}// namespace NES

#include <llvm/IR/Module.h>
#include <mlir/IR/BuiltinOps.h>
#include <mlir/Pass/Pass.h>
#include <vector>

namespace NES::Nautilus::Backends::MLIR {

/**
 * @brief The LLVMIROptimizer takes a generated MLIR module, 
 * and applies configured lowering & optimization passes to it.
 */
class LLVMIROptimizer {
  public:
    LLVMIROptimizer(); // Disable default constructor
    ~LLVMIROptimizer();// Disable default destructor

    static std::function<llvm::Error(llvm::Module*)> getLLVMOptimizerPipeline(const CompilationOptions& options,
                                                                              const DumpHelper& dumpHelper);
};
}// namespace NES::Nautilus::Backends::MLIR
#endif  // NES_RUNTIME_INCLUDE_NAUTILUS_BACKENDS_MLIR_LLVMIROPTIMIZER_HPP_
