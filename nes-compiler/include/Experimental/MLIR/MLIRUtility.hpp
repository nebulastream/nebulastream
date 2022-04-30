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
#include <Experimental/NESIR/BasicBlocks/LoopBasicBlock.hpp>
#include <mlir/ExecutionEngine/ExecutionEngine.h>
#include <mlir/IR/BuiltinOps.h>
#include <mlir/IR/MLIRContext.h>
#include <llvm/ExecutionEngine/JITSymbol.h>
#include <llvm/ExecutionEngine/Orc/Mangling.h>


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
         * @param debugFromFile: If true, create MLIR from mlirFilePath 
            (enables debugging)
         */
        MLIRUtility(std::string mlirFilepath, bool debugFromFile);
        ~MLIRUtility() = default;

        /**
        * @brief Creates MLIR and if successful, applies lowering passes to it
        * @param NESTree: NES abstraction tree from which MLIR is generated.
        * @param debugFlags: Determine whether and how to print/write MLIR.
        * @return int: 1 if error occurred, else 0
        */
        int loadAndProcessMLIR(const std::shared_ptr<NES::LoopBasicBlock>& loopBasicBlock,
                               DebugFlags *debugFlags = nullptr);

        int loadModuleFromString(const std::string &mlirString, DebugFlags *debugFlags);

        /**
         * @brief Takes symbols and JITAddresses and JIT runs created module.
         * @param llvmIRModule External symbol names, e.g. function names.
         * @param jitAddresses: Memory addresses of external functions, objects, etc.
         * @return int: 1 if error occurred, else 0
         */
        int runJit(const std::vector<std::string> &llvmIRModule,
                const std::vector<llvm::JITTargetAddress> &jitAddresses, bool useProxyFunctions);

        /**
         * @brief Can print a module and write it to a file, depending on debugFlags.
         * @param mlirModule: The module to print/write.
         * @param debugFlags: Determine whether and how to print/write MLIR.
         */
        void printMLIRModule(mlir::OwningOpRef<mlir::ModuleOp> &mlirModule,
                             DebugFlags *debugFlags);

    private:
        mlir::OwningOpRef<mlir::ModuleOp> module;
        mlir::MLIRContext context;
        std::string mlirFilepath;
        bool debugFromFile;

        static std::string insertComments(const std::string &moduleString);

};
#endif //MLIR_COMPILER
#endif //NES_INCLUDE_EXPERIMENTAL_MLIRUTILITY_HPP_