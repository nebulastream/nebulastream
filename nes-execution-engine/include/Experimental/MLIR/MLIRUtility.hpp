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

#include <Nautilus/IR/NESIR.hpp>
#include <memory>
#include <mlir/ExecutionEngine/ExecutionEngine.h>
#include <mlir/IR/MLIRContext.h>
#include <mlir/IR/OwningOpRef.h>
#include <mlir/IR/BuiltinOps.h>
#include <string>

namespace NES::ExecutionEngine::Experimental::MLIR {

class MLIRGenerator;

class MLIRUtility {
  public:
    /**
     * @brief Controls debugging related aspects of MLIR parsing and creation.
     * comments: Insert comments into MLIR module.
     * enableDebugInfo: Create MLIR with named locations.
     * prettyDebug: Prettily write debug info. Invalidates MLIR!
     */
    struct DebugFlags {
        bool comments;
        bool enableDebugInfo;
        bool prettyDebug;
    };

    /**
     * @brief Used to generate MLIR, print/write and JIT compile it.
     * @param mlirFilepath: Path from/to which to read/write MLIR.
     * @param debugFromFile: If true, create MLIR from mlirFilePath (enables debugging)
     */
    MLIRUtility(std::string mlirFilepath, bool debugFromFile);
    ~MLIRUtility() = default;

    /**
    * @brief Creates MLIR and if successful, applies lowering passes to it
    * @param NESTree: NES abstraction tree from which MLIR is generated.
    * @param debugFlags: Determine whether and how to print/write MLIR.
    * @return int: 1 if error occurred, else 0
    */
    int loadAndProcessMLIR(std::shared_ptr<IR::NESIR> nesIR, DebugFlags* debugFlags = nullptr, bool useSCF = true);

    int loadModuleFromString(const std::string& mlirString, DebugFlags* debugFlags = nullptr);
    int loadModuleFromStrings(const std::string& mlirString, const std::string& mlirString2, DebugFlags* debugFlags);

    /**
     * @brief Takes symbols and JITAddresses and JIT runs created module.
     * @param llvmIRModule External symbol names, e.g. function names.
     * @param jitAddresses: Memory addresses of external functions, objects, etc.
     * @return int: 1 if error occurred, else 0
     */
    int runJit(bool linkProxyFunctions, void* inputBufferPtr = nullptr, void* outputBufferPtr = nullptr);
    std::unique_ptr<mlir::ExecutionEngine> prepareEngine(bool linkProxyFunctions = false);

    /**
     * @brief Can print a module and write it to a file, depending on debugFlags.
     * @param mlirModule: The module to print/write.
     * @param debugFlags: Determine whether and how to print/write MLIR.
     */
    void printMLIRModule(mlir::OwningOpRef<mlir::ModuleOp>& mlirModule, DebugFlags* debugFlags);

    /**
     * @brief Creates a function that applies a custom LLVM IR optimizer the LLVM IR query code generated from MLIR
     * @param linkProxyFunctions: if true, link proxy function definitions from prepared LLVM IR module
     */
    llvm::function_ref<llvm::Error(llvm::Module*)> getOptimizingTransformer(bool linkProxyFunctions);

  private:
    mlir::OwningOpRef<mlir::ModuleOp> module;
    mlir::MLIRContext context;
    std::shared_ptr<MLIRGenerator> mlirGenerator;
    std::string mlirFilepath;
    bool debugFromFile;

    static std::string insertComments(const std::string& moduleString);
};

}// namespace NES::ExecutionEngine::Experimental::MLIR
#endif//NES_INCLUDE_EXPERIMENTAL_MLIRUTILITY_HPP_