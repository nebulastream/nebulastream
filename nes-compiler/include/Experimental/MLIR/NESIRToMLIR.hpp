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

#include <Experimental/NESIR/BasicBlocks/LoopBasicBlock.hpp>
#include <Experimental/NESIR/BasicBlocks/IfBasicBlock.hpp>

#include <Experimental/NESIR/Operations/AddIntOperation.hpp>
#include <Experimental/NESIR/Operations/StoreOperation.hpp>
#include <Experimental/NESIR/Operations/LoadOperation.hpp>
#include <Experimental/NESIR/Operations/ConstantIntOperation.hpp>
#include <Experimental/NESIR/Operations/PredicateOperation.hpp>

#include <mlir/Dialect/StandardOps/IR/Ops.h>
#include <mlir/Dialect/LLVMIR/LLVMDialect.h>
#include <mlir/Dialect/SCF/SCF.h>

#include <mlir/IR/Attributes.h>
#include <mlir/IR/Builders.h>
#include <mlir/IR/BuiltinAttributes.h>
#include <mlir/IR/BuiltinOps.h>
#include <mlir/IR/BuiltinTypes.h>
#include <mlir/IR/MLIRContext.h>
#include <mlir/IR/Operation.h>
#include <mlir/IR/PatternMatch.h>
#include <mlir/IR/Value.h>
#include <llvm/ADT/StringMap.h>
#include <llvm/ADT/StringSet.h>

#include <cstdint>
#include <memory>

using namespace mlir;

class MLIRGenerator {
    public:
    /**
     * @brief Enables converting a NESAbstraction to executable MLIR code.
     * @param MLIRBuilder Builder that provides functionalities to generate MLIR.
     * @param MLIRModule The MLIR module that the generated MLIR is inserted into.
     * @param rootNode The first node of the NES abstraction.
     */
    MLIRGenerator(mlir::MLIRContext &context);
    ~MLIRGenerator() = default;

    /**
     * @brief Recursively iterates over a NES abstraction tree to generate MLIR
     * @return An MLIR module that contains the generated 'execute' function.
     */
    mlir::ModuleOp generateMLIR(std::shared_ptr<NES::LoopBasicBlock> rootNode);

private:
    // MLIR variables
    std::shared_ptr<OpBuilder> builder;
    MLIRContext *context;
    mlir::ModuleOp theModule;
    FuncOp executeFunction;
    // NES Variables
    mlir::LLVM::GlobalOp inputBufferGlobal;
    mlir::LLVM::GlobalOp outputBufferGlobal;
    // Utility
    mlir::Value constZero;
    mlir::RewriterBase::InsertPoint *globalInsertPoint;
    mlir::Value globalString;
    mlir::FlatSymbolRefAttr printfReference;
    llvm::StringMap<mlir::Value> printfStrings;

    //Todo Adapt to NESIR approach
    // - generateMLIR(LoopBasicBlock) [ ]
    //   - entry point (for now)
    // - generateMLIR(IfBasicBlock)
    //   - not needed (for now)
    // - generateMLIR(Operation)
    //   - AddOperation   [ ]
    //   - LoadOperation  [ ]
    //   - StoreOperation [ ]
    //   - ConstantIntOp  [ ]

    /**
     * @brief Calls the specific generate function based on currentNode's type.
     * @param parentBlock MLIR Block that new operation is inserted into.
     */
    void generateMLIR(const NES::OperationPtr& operation);
    void generateMLIR(std::shared_ptr<NES::ConstantIntOperation> operation);
    void generateMLIR(std::shared_ptr<NES::AddIntOperation> operation);
    void generateMLIR(std::shared_ptr<NES::StoreOperation> operation);
    void generateMLIR(std::shared_ptr<NES::LoadOperation> operation);

    /**
     * @brief Prints content of a tuple from within MLIR.
     * @param currentTuple: The tuple to print.
     * @param indice : Which fields of the tuple to print.
     * @param type : The field types.
     */
    void debugBufferPrint(mlir::Value currentTuple, std::vector<uint64_t> indices,
                     std::vector<NES::Operation::BasicType> types);

    /**
     * @brief Create a Global String object.
     * @param loc: NamedLocation for debugging purposes.
     * @param name: Name of the global string.
     * @param value: String value.
     * @return Value: GEP address of global string.
     */
    Value createGlobalString(Location loc, StringRef name, StringRef value);

    /**
     * @brief Inserts Input- and OutputBuffer pointers globally.
     * @param types: Field types of buffers.
     */
    void insertTupleBuffer(std::vector<NES::Operation::BasicType> types);


    /**
     * @brief Print a single value from the TupleBuffer.
     * @param printedValue: Address of a TupleBuffer value.
     * @param type: Type of the value.
     */
    void insertValuePrint(mlir::Value printedValue, NES::Operation::BasicType type);

    /**
     * @brief Inserts an external, but non-class-member-function, into MLIR.
     * @param name: Function name.
     * @param numResultBits: Number of bits of returned Integer.
     * @param argTypes: Argument types of function.
     * @param varArgs: Include variable arguments.
     * @return FlatSymbolRefAttr: Reference to function used in CallOps.
     */
    FlatSymbolRefAttr insertExternalFunction(const std::string &name,
                                            uint8_t numResultBits,
                                            std::vector<mlir::Type> argTypes,
                                            bool varArgs);

    /**
     * @brief Inserts an mlir::LLVM::UnknownOp into module with comment string
     as named location. When debug printing, UnknownOp is converted to comment.
     * @param comment: Used to create named location. Turned to comment if debug.
     */
    void insertComment(const std::string &comment);

    /**
     * @brief Generates a Name(d)Loc(ation) that is attached to the operation.
     * @param name: Name of the location. Used for debugging.
     */
    Location getNameLoc(const std::string &name);
    
    /**
     * @brief Get MLIR Type from a basic NES type.
     * @param type: NES basic type.
     * @return mlir::Type: MLIR Type.
     */
    mlir::Type getMLIRType(NES::Operation::BasicType type);

    /**
     * @brief Get a constant MLIR Integer.
     * @param loc: NamedLocation for debugging purposes.
     * @param numBits: Bit width of the returned constant Integer.
     * @param value: Value of the returned Integer.
     * @return mlir::Value: Constant MLIR Integer value
     */
    mlir::Value getConstInt(Location loc, int numBits, int64_t value);

    /**
     * @brief Get the Bit Width from a basic NES type.
     * @param type: Basic NES type.
     * @return int8_t: Bit width.
     */
    int8_t getBitWidthFromType(NES::Operation::BasicType type);
};
#endif //NES_INCLUDE_EXPERIMENTAL_NESABSTRACTIONTOMLIR_HPP_
