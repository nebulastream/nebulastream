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

#include <Nautilus/IR/IRGraph.hpp>
#include <mlir/ExecutionEngine/ExecutionEngine.h>
#include <mlir/IR/BuiltinOps.h>
#include <mlir/IR/MLIRContext.h>
#include <mlir/IR/OwningOpRef.h>
#include <string>

namespace NES::ExecutionEngine::Experimental::MLIR {

class MLIRLoweringProvider;

class MLIRUtility {
  public:

    MLIRUtility();
    ~MLIRUtility();

    /**
     * @brief Writes an MLIR module to a file. A module that is loaded from file allows for step-through debugging.
     * @param mlirModule: The module to write.
     */
    static void writeMLIRModuleToFile(mlir::OwningOpRef<mlir::ModuleOp>& mlirModule, std::string mlirFilepath);

    /**
     * @brief 
     * @param mlirString 
     * @param rootFunctionName 
     * @return int 
     */
    static int loadAndExecuteModuleFromString(const std::string& mlirString, const std::string& rootFunctionName);

    /**
     * @brief 
     * @param mlirString 
     * @param rootFunctionName 
     * @return int 
     */
    static std::unique_ptr<mlir::ExecutionEngine> 
    compileNESIRToMachineCode(std::shared_ptr<NES::Nautilus::IR::IRGraph> ir);
};

}// namespace NES::ExecutionEngine::Experimental::MLIR
#endif//NES_INCLUDE_EXPERIMENTAL_MLIRUTILITY_HPP_