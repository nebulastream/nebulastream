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

#ifndef NES_INCLUDE_EXPERIMENTAL_NESABSTRACTIONTOMLIR_HPP_
#define NES_INCLUDE_EXPERIMENTAL_NESABSTRACTIONTOMLIR_HPP_

#include <Experimental/NESIR/BasicBlocks/BasicBlock.hpp>
#include <Experimental/NESIR/NESIR.hpp>
#include <Experimental/NESIR/Operations/AddIntOperation.hpp>
#include <Experimental/NESIR/Operations/AddressOperation.hpp>
#include <Experimental/NESIR/Operations/BranchOperation.hpp>
#include <Experimental/NESIR/Operations/CompareOperation.hpp>
#include <Experimental/NESIR/Operations/ConstantIntOperation.hpp>
#include <Experimental/NESIR/Operations/FunctionOperation.hpp>
#include <Experimental/NESIR/Operations/IfOperation.hpp>
#include <Experimental/NESIR/Operations/LoadOperation.hpp>
#include <Experimental/NESIR/Operations/LoopOperation.hpp>
#include <Experimental/NESIR/Operations/Operation.hpp>
#include <Experimental/NESIR/Operations/ProxyCallOperation.hpp>
#include <Experimental/NESIR/Operations/ReturnOperation.hpp>
#include <Experimental/NESIR/Operations/StoreOperation.hpp>
#include <Experimental/NESIR/ProxyFunctions.hpp>
#include <cstdint>
#include <llvm/ADT/StringMap.h>
#include <llvm/ADT/StringSet.h>
#include <llvm/ExecutionEngine/JITSymbol.h>
#include <memory>
#include <mlir/Dialect/LLVMIR/LLVMDialect.h>
#include <mlir/Dialect/SCF/SCF.h>
#include <mlir/Dialect/StandardOps/IR/Ops.h>
#include <mlir/IR/Attributes.h>
#include <mlir/IR/Builders.h>
#include <mlir/IR/BuiltinAttributes.h>
#include <mlir/IR/BuiltinOps.h>
#include <mlir/IR/BuiltinTypes.h>
#include <mlir/IR/MLIRContext.h>
#include <mlir/IR/Operation.h>
#include <mlir/IR/PatternMatch.h>
#include <mlir/IR/Value.h>
#include <unordered_map>
#include <unordered_set>

namespace NES::ExecutionEngine::Experimental::MLIR {

class MLIRGenerator {
  public:
    /**
     * @brief Inserts mlir versions of frequently needed class member functions.
     */
    static std::vector<mlir::FuncOp> GetMemberFunctions(mlir::MLIRContext& context);

    /**
     * @brief Enables converting a NESAbstraction to executable MLIR code.
     * @param MLIRBuilder Builder that provides functionalities to generate MLIR.
     * @param MLIRModule The MLIR module that the generated MLIR is inserted into.
     * @param rootNode The first node of the NES abstraction.
     */
    MLIRGenerator(mlir::MLIRContext& context, std::vector<mlir::FuncOp>& memberFunctions);
    ~MLIRGenerator() = default;

    mlir::ModuleOp generateModuleFromNESIR(std::shared_ptr<IR::NESIR> nesIR);

    std::vector<std::string> getJitProxyFunctionSymbols();
    std::vector<llvm::JITTargetAddress> getJitProxyTargetAddresses();

  private:
    // MLIR variables
    std::shared_ptr<mlir::OpBuilder> builder;
    mlir::MLIRContext* context;
    mlir::ModuleOp theModule;
    // Map that contains execute input args, function call results and intermediary results from NESIR Operations.
    std::vector<mlir::FuncOp> memberFunctions;
    NES::ProxyFunctions ProxyFunctions;
    std::vector<std::string> jitProxyFunctionSymbols;
    std::vector<llvm::JITTargetAddress> jitProxyFunctionTargetAddresses;
    std::unordered_set<std::string> inductionVars;
    // Utility
    mlir::RewriterBase::InsertPoint* globalInsertPoint;
    mlir::Value globalString;
    mlir::FlatSymbolRefAttr printfReference;
    llvm::StringMap<mlir::Value> printfStrings;

    void generateMLIR(IR::BasicBlockPtr basicBlock, std::unordered_map<std::string, mlir::Value>& blockArgs);

    /**
     * @brief Calls the specific generate function based on currentNode's type.
     * @param parentBlock MLIR Block that new operation is inserted into.
     */
    void generateMLIR(const IR::Operations::OperationPtr& operation, std::unordered_map<std::string, mlir::Value>& blockArgs);
    void generateMLIR(std::shared_ptr<IR::Operations::FunctionOperation> funcOp, std::unordered_map<std::string, mlir::Value>& blockArgs);
    void generateMLIR(std::shared_ptr<IR::Operations::LoopOperation> loopOp, std::unordered_map<std::string, mlir::Value>& blockArgs);
    void generateMLIR(std::shared_ptr<IR::Operations::ConstantIntOperation> constIntOp,
                      std::unordered_map<std::string, mlir::Value>& blockArgs);
    void generateMLIR(std::shared_ptr<IR::Operations::AddIntOperation> addIntOp, std::unordered_map<std::string, mlir::Value>& blockArgs);
    void generateMLIR(std::shared_ptr<IR::Operations::StoreOperation> storeOp, std::unordered_map<std::string, mlir::Value>& blockArgs);
    void generateMLIR(std::shared_ptr<IR::Operations::LoadOperation> loadOp, std::unordered_map<std::string, mlir::Value>& blockArgs);
    void generateMLIR(std::shared_ptr<IR::Operations::AddressOperation> addressOp, std::unordered_map<std::string, mlir::Value>& blockArgs);

    void generateMLIR(std::shared_ptr<IR::Operations::IfOperation> ifOp, std::unordered_map<std::string, mlir::Value>& blockArgs);
    void generateMLIR(std::shared_ptr<IR::Operations::CompareOperation> compareOp, std::unordered_map<std::string, mlir::Value>& blockArgs);
    void generateMLIR(std::shared_ptr<IR::Operations::BranchOperation> branchOp, std::unordered_map<std::string, mlir::Value>& blockArgs);
    void generateMLIR(std::shared_ptr<IR::Operations::ReturnOperation> returnOp, std::unordered_map<std::string, mlir::Value>& blockArgs);
    void generateMLIR(std::shared_ptr<IR::Operations::ProxyCallOperation> proxyCallOp,
                      std::unordered_map<std::string, mlir::Value>& blockArgs);

    /**
     * @brief Inserts an external, but non-class-member-function, into MLIR.
     * @param name: Function name.
     * @param numResultBits: Number of bits of returned Integer.
     * @param argTypes: Argument types of function.
     * @param varArgs: Include variable arguments.
     * @return FlatSymbolRefAttr: Reference to function used in CallOps.
     */
    mlir::FlatSymbolRefAttr
    insertExternalFunction(const std::string& name, mlir::Type resultType, std::vector<mlir::Type> argTypes, bool varArgs);

    /**
     * @brief Inserts an mlir::LLVM::UnknownOp into module with comment string
     as named location. When debug printing, UnknownOp is converted to comment.
     * @param comment: Used to create named location. Turned to comment if debug.
     */
    void insertComment(const std::string& comment);

    /**
     * @brief Generates a Name(d)Loc(ation) that is attached to the operation.
     * @param name: Name of the location. Used for debugging.
     */
    mlir::Location getNameLoc(const std::string& name);

    /**
     * @brief Get MLIR Type from a basic NES type.
     * @param type: NES basic type.
     * @return mlir::Type: MLIR Type.
     */
    mlir::Type getMLIRType(IR::Operations::Operation::BasicType type);

    /**
     * @brief Get MLIR Type from a basic NES type.
     * @param type: NES basic type.
     * @return mlir::Type: MLIR Type.
     */
    std::vector<mlir::Type> getMLIRType(std::vector<IR::Operations::Operation::BasicType> types);

    /**
     * @brief Get a constant MLIR Integer.
     * @param loc: NamedLocation for debugging purposes.
     * @param numBits: Bit width of the returned constant Integer.
     * @param value: Value of the returned Integer.
     * @return mlir::Value: Constant MLIR Integer value
     */
    mlir::Value getConstInt(const std::string& location, int numBits, int64_t value);

    /**
     * @brief Get the Bit Width from a basic NES type.
     * @param type: Basic NES type.
     * @return int8_t: Bit width.
     */
    int8_t getBitWidthFromType(IR::Operations::Operation::BasicType type);

    //
    IR::Operations::OperationPtr findLastTerminatorOp(IR::BasicBlockPtr thenBlock, int ifParentBlockLevel);
};
}// namespace NES::ExecutionEngine::Experimental::MLIR
#endif//NES_INCLUDE_EXPERIMENTAL_NESABSTRACTIONTOMLIR_HPP_
