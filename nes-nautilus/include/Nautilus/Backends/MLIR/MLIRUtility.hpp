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

#ifndef NES_NAUTILUS_INCLUDE_NAUTILUS_BACKENDS_MLIR_MLIRUTILITY_HPP_
#define NES_NAUTILUS_INCLUDE_NAUTILUS_BACKENDS_MLIR_MLIRUTILITY_HPP_

#include <Nautilus/IR/IRGraph.hpp>
#include <mlir/ExecutionEngine/ExecutionEngine.h>
#include <mlir/IR/BuiltinOps.h>
#include <mlir/IR/MLIRContext.h>
#include <mlir/IR/OwningOpRef.h>
#include <string>

namespace NES::Nautilus::Backends::MLIR {

/**
 * @brief Provides utility functions for the MLIR backend.
 */
class MLIRUtility {
  public:
    MLIRUtility();
    ~MLIRUtility();

    /**
     * @brief Writes an MLIR module to a file. A module that is loaded from file allows for step-through debugging.
     * @param mlirModule: The module to write.
     * @param mlirFilepath: Target write path.
     */
    static void writeMLIRModuleToFile(mlir::OwningOpRef<mlir::ModuleOp>& mlirModule, std::string mlirFilepath);

    /**
     * @brief Takes an MLIR module in string representation,  and lowers, compiles, and executes it.
     * @param moduleString: String that should represent a valid MLIR module.
     * @param rootFunctionName: Name of the function that is called to initiate execution.
     * @return int: 0 if success, 1 otherwise.
     */
    static int loadAndExecuteModuleFromString(const std::string& moduleString, const std::string& rootFunctionName);

    /**
     * @brief Takes NESIR, lowers it to MLIR, optimizes it, and returns an MLIR ExecutionEngine.
     * @param ir: NESIR that is lowered to MLIR, and JIT compiled.
     * @return std::unique_ptr<mlir::ExecutionEngine> Provides a function pointer to executable function.
     */
    static std::unique_ptr<mlir::ExecutionEngine> compileNESIRToMachineCode(std::shared_ptr<NES::Nautilus::IR::IRGraph> ir);
};

}// namespace NES::Nautilus::Backends::MLIR
#endif // NES_NAUTILUS_INCLUDE_NAUTILUS_BACKENDS_MLIR_MLIRUTILITY_HPP_
