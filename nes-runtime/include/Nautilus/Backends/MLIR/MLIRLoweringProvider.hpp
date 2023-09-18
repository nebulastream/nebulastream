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

#ifndef NES_RUNTIME_INCLUDE_NAUTILUS_BACKENDS_MLIR_MLIRLOWERINGPROVIDER_HPP_
#define NES_RUNTIME_INCLUDE_NAUTILUS_BACKENDS_MLIR_MLIRLOWERINGPROVIDER_HPP_

#include <Nautilus/Backends/MLIR/ProxyFunctions.hpp>
#include <Nautilus/IR/BasicBlocks/BasicBlock.hpp>
#include <Nautilus/IR/IRGraph.hpp>
#include <Nautilus/IR/Operations/AddressOperation.hpp>
#include <Nautilus/IR/Operations/ArithmeticOperations/AddOperation.hpp>
#include <Nautilus/IR/Operations/ArithmeticOperations/DivOperation.hpp>
#include <Nautilus/IR/Operations/ArithmeticOperations/ModOperation.hpp>
#include <Nautilus/IR/Operations/ArithmeticOperations/MulOperation.hpp>
#include <Nautilus/IR/Operations/ArithmeticOperations/SubOperation.hpp>
#include <Nautilus/IR/Operations/BranchOperation.hpp>
#include <Nautilus/IR/Operations/CastOperation.hpp>
#include <Nautilus/IR/Operations/ConstBooleanOperation.hpp>
#include <Nautilus/IR/Operations/ConstFloatOperation.hpp>
#include <Nautilus/IR/Operations/ConstIntOperation.hpp>
#include <Nautilus/IR/Operations/FunctionOperation.hpp>
#include <Nautilus/IR/Operations/IfOperation.hpp>
#include <Nautilus/IR/Operations/LoadOperation.hpp>
#include <Nautilus/IR/Operations/LogicalOperations/AndOperation.hpp>
#include <Nautilus/IR/Operations/LogicalOperations/BitWiseAndOperation.hpp>
#include <Nautilus/IR/Operations/LogicalOperations/BitWiseLeftShiftOperation.hpp>
#include <Nautilus/IR/Operations/LogicalOperations/BitWiseOrOperation.hpp>
#include <Nautilus/IR/Operations/LogicalOperations/BitWiseRightShiftOperation.hpp>
#include <Nautilus/IR/Operations/LogicalOperations/BitWiseXorOperation.hpp>
#include <Nautilus/IR/Operations/LogicalOperations/CompareOperation.hpp>
#include <Nautilus/IR/Operations/LogicalOperations/NegateOperation.hpp>
#include <Nautilus/IR/Operations/LogicalOperations/OrOperation.hpp>
#include <Nautilus/IR/Operations/Loop/LoopOperation.hpp>
#include <Nautilus/IR/Operations/Operation.hpp>
#include <Nautilus/IR/Operations/ProxyCallOperation.hpp>
#include <Nautilus/IR/Operations/ReturnOperation.hpp>
#include <Nautilus/IR/Operations/StoreOperation.hpp>
#include <Nautilus/Util/Frame.hpp>
#include <llvm/ADT/StringMap.h>
#include <llvm/ADT/StringSet.h>
#include <llvm/ExecutionEngine/JITSymbol.h>
#include <mlir/IR/PatternMatch.h>
#include <unordered_set>

namespace NES::Nautilus::Backends::MLIR {

class MLIRLoweringProvider {
  public:
    // A ValueFrame is hashmap that binds operation names to MLIR values.
    // It is used to 'pass' values between mlir operations.
    // Control Flow can cause new ValueFrames to be created, to correctly model value access rights (scopes).
    using ValueFrame = Frame<std::string, mlir::Value>;

    /**
     * @brief Allows to lower Nautilus IR to MLIR.
     * @param MLIRContext: Used by MLIR to manage MLIR module creation.
     */
    MLIRLoweringProvider(mlir::MLIRContext& context);

    ~MLIRLoweringProvider();
    /**
     * @brief Root MLIR generation function. Takes  as an IRGraph, and recursively lowers its operations to MLIR.
     * @param :  represented as an IRGraph.
     * @return mlir::mlir::OwningOpRef<mlir::ModuleOp> that is equivalent to the  module, and can be lowered to LLVM IR in one step.
     */
    mlir::OwningOpRef<mlir::ModuleOp> generateModuleFromIR(std::shared_ptr<IR::IRGraph>);

    /**
     * @return std::vector<std::string>: All proxy function symbols used in the module.
     */
    std::vector<std::string> getJitProxyFunctionSymbols();

    /**
     * @return std::vector<llvm::JITTargetAddress>: All proxy function addresses used in the module.
     */
    std::vector<llvm::JITTargetAddress> getJitProxyTargetAddresses();

  private:
    // MLIR variables
    mlir::MLIRContext* context;
    mlir::ModuleOp theModule;
    std::unique_ptr<mlir::OpBuilder> builder;
    NES::ProxyFunctions ProxyFunctions;
    std::vector<std::string> jitProxyFunctionSymbols;
    std::vector<llvm::JITTargetAddress> jitProxyFunctionTargetAddresses;
    std::unordered_set<std::string> inductionVars;
    // Utility
    mlir::RewriterBase::InsertPoint* globalInsertPoint;
    mlir::Value globalString;
    mlir::FlatSymbolRefAttr printfReference;
    llvm::StringMap<mlir::Value> printfStrings;
    std::unordered_map<std::string, mlir::Block*> blockMapping;//Keeps track of already created basic blocks.

    /**
     * @brief Generates MLIR from a  basic block. Iterates over basic block operations and calls generate.
     * 
     * @param basicBlock: The  basic block that MLIR code is generated for.
     * @param frame: An unordered map that MLIR operations insert their resulting values, and identifiers in.
     */
    void generateMLIR(IR::BasicBlockPtr basicBlock, ValueFrame& frame);

    /**
     * @brief Calls the specific generate function based on currentNode's type.
     * @param Operation:  operation that the MLIRLoweringProvider generates MLIR code for.
     * @param frame: An unordered map that MLIR operations insert their resulting values, and identifiers in.
     */
    void generateMLIR(const IR::Operations::OperationPtr& operation, ValueFrame& frame);
    void generateMLIR(std::shared_ptr<IR::Operations::FunctionOperation> funcOp, ValueFrame& frame);
    void generateMLIR(std::shared_ptr<IR::Operations::ConstIntOperation> constIntOp, ValueFrame& frame);
    void generateMLIR(std::shared_ptr<IR::Operations::ConstFloatOperation> constFloatOp, ValueFrame& frame);
    void generateMLIR(std::shared_ptr<IR::Operations::ConstBooleanOperation> constBooleanOp, ValueFrame& frame);
    void generateMLIR(std::shared_ptr<IR::Operations::AddOperation> addIntOp, ValueFrame& frame);
    void generateMLIR(std::shared_ptr<IR::Operations::SubOperation> subIntOp, ValueFrame& frame);
    void generateMLIR(std::shared_ptr<IR::Operations::MulOperation> mulIntOp, ValueFrame& frame);
    void generateMLIR(std::shared_ptr<IR::Operations::DivOperation> divFloatOp, ValueFrame& frame);
    void generateMLIR(std::shared_ptr<IR::Operations::ModOperation> modIntOp, ValueFrame& frame);
    void generateMLIR(std::shared_ptr<IR::Operations::StoreOperation> storeOp, ValueFrame& frame);
    void generateMLIR(std::shared_ptr<IR::Operations::LoadOperation> loadOp, ValueFrame& frame);
    void generateMLIR(std::shared_ptr<IR::Operations::AddressOperation> addressOp, ValueFrame& frame);
    void generateMLIR(std::shared_ptr<IR::Operations::IfOperation> ifOp, ValueFrame& frame);
    void generateMLIR(std::shared_ptr<IR::Operations::CompareOperation> compareOp, ValueFrame& frame);
    void generateMLIR(std::shared_ptr<IR::Operations::BranchOperation> branchOp, ValueFrame& frame);
    void generateMLIR(std::shared_ptr<IR::Operations::ReturnOperation> returnOp, ValueFrame& frame);
    void generateMLIR(std::shared_ptr<IR::Operations::ProxyCallOperation> proxyCallOp, ValueFrame& frame);
    void generateMLIR(std::shared_ptr<IR::Operations::OrOperation> yieldOperation, ValueFrame& frame);
    void generateMLIR(std::shared_ptr<IR::Operations::AndOperation> yieldOperation, ValueFrame& frame);
    void generateMLIR(std::shared_ptr<IR::Operations::NegateOperation> yieldOperation, ValueFrame& frame);
    void generateMLIR(std::shared_ptr<IR::Operations::BitWiseAndOperation> yieldOperation, ValueFrame& frame);
    void generateMLIR(std::shared_ptr<IR::Operations::BitWiseOrOperation> yieldOperation, ValueFrame& frame);
    void generateMLIR(std::shared_ptr<IR::Operations::BitWiseXorOperation> yieldOperation, ValueFrame& frame);
    void generateMLIR(std::shared_ptr<IR::Operations::BitWiseLeftShiftOperation> yieldOperation, ValueFrame& frame);
    void generateMLIR(std::shared_ptr<IR::Operations::BitWiseRightShiftOperation> yieldOperation, ValueFrame& frame);
    void generateMLIR(std::shared_ptr<IR::Operations::CastOperation> castOperation, ValueFrame& frame);
    void generateMLIR(std::shared_ptr<IR::Operations::LoopOperation> loopOp, ValueFrame& frame);

    /**
     * @brief Generates a basic block inside of the current MLIR module. Used for control flow (if,loop).
     * @param blockInvocation:  basic block that is invocated.
     * @param frame: An unordered map that MLIR operations insert their resulting values, and identifiers in.
     * @return mlir::Block*: Returns a pointer to an MLIR basic block.
     */
    mlir::Block* generateBasicBlock(IR::Operations::BasicBlockInvocation& blockInvocation, ValueFrame& frame);

    /**
     * @brief Inserts an external, but non-class-member-function, into MLIR.
     * @param name: Function name.
     * @param numResultBits: Number of bits of returned Integer.
     * @param argTypes: Argument types of function.
     * @param varArgs: Include variable arguments.
     * @return FlatSymbolRefAttr: Reference to function used in CallOps.
     */
    mlir::FlatSymbolRefAttr insertExternalFunction(const std::string& name,
                                                   void* functionPtr,
                                                   mlir::Type resultType,
                                                   std::vector<mlir::Type> argTypes,
                                                   bool varArgs);

    /**
     * @brief Generates a Name(d)Loc(ation) that is attached to the operation.
     * @param name: Name of the location. Used for debugging.
     */
    mlir::Location getNameLoc(const std::string& name);

    /**
     * @brief Get MLIR Type from a basic  type.
     * @param type:  basic type.
     * @return mlir::Type: MLIR Type.
     */
    mlir::Type getMLIRType(IR::Types::StampPtr type);

    /**
     * @brief Get a vector of MLIR Types from a vector of  types.
     * @param types: Vector of basic  types.
     * @return mlir::Type: Vector of MLIR types.
     */
    std::vector<mlir::Type> getMLIRType(std::vector<IR::Operations::OperationPtr> types);

    /**
     * @brief Get a constant MLIR Integer.
     * @param location: NamedLocation for debugging purposes.
     * @param numBits: Bit width of the returned constant Integer.
     * @param value: Value of the returned Integer.
     * @return mlir::Value: Constant MLIR Integer value.
     */
    mlir::Value getConstInt(const std::string& location, IR::Types::StampPtr stamp, int64_t value);

    /**
     * @brief Get a constant MLIR Integer.
     * @param location: NamedLocation for debugging purposes.
     * @param value: Value of the returned boolean.
     */
    mlir::Value getConstBool(const std::string& location, bool value);

    /**
     * @brief Get the Bit Width from a basic NES type.
     * @param type: Basic NES type.
     * @return int8_t: Bit width.
     */
    int8_t getBitWidthFromType(IR::Operations::PrimitiveStamp type);

    /**
     * @brief Create a ValueFrame From Parent Block object.
     * 
     * @param frame: An unordered map that MLIR operations insert their resulting values, and identifiers in.
     * @param invocation: The parent basic block.
     * @return ValueFrame: An unordered map with identifiers and MLIR values in it.
     */
    ValueFrame createFrameFromParentBlock(ValueFrame& frame, IR::Operations::BasicBlockInvocation& invocation);
};
}// namespace NES::Nautilus::Backends::MLIR
#endif// NES_RUNTIME_INCLUDE_NAUTILUS_BACKENDS_MLIR_MLIRLOWERINGPROVIDER_HPP_
