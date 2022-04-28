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

#ifndef NES_INCLUDE_EXPERIMENTAL_MLIRUTILITY_HPP_
#define NES_INCLUDE_EXPERIMENTAL_MLIRUTILITY_HPP_

#ifdef MLIR_COMPILER
#include <mlir/ExecutionEngine/ExecutionEngine.h>
#include <mlir/IR/BuiltinOps.h>
#include <mlir/IR/MLIRContext.h>
#include <llvm/ExecutionEngine/JITSymbol.h>
#include <llvm/ExecutionEngine/Orc/Mangling.h>

class MLIRUtility {
  public:
    MLIRUtility() = default;
    ~MLIRUtility() = default;

    /**
     * @brief Creates an MLIR module from a string.
     * @param mlirString: A string that must contain a valid MLIR module.
     * @return int: 1 if error occurred, else 0
     */
    int loadModuleFromString(const std::string &mlirString);

    /**
     * @brief Takes symbols and JITAddresses and JIT runs created module.
     * @param llvmIRModule: External symbol names, e.g. function names.
     * @param jitAddresses: Memory addresses of external functions, objects, etc.
     * @return int: 1 if error occurred, else 0
     */
    int runJit(const std::vector<std::string> &llvmIRModule, const std::vector<llvm::JITTargetAddress> &jitAddresses);

  private:
    mlir::OwningOpRef<mlir::ModuleOp> module;
    mlir::MLIRContext context;
};
#endif //MLIR_COMPILER
#endif //NES_INCLUDE_EXPERIMENTAL_MLIRUTILITY_HPP_