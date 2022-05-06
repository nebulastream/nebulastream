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

#include <Experimental/MLIR/NESIRToMLIR.hpp>

#include "mlir/Dialect/Arithmetic/IR/Arithmetic.h"
#include "mlir/Dialect/LLVMIR/LLVMDialect.h"
#include "mlir/Dialect/LLVMIR/LLVMTypes.h"
#include "mlir/IR/Attributes.h"
#include "mlir/IR/Builders.h"
#include "mlir/IR/BuiltinAttributes.h"
#include "mlir/IR/BuiltinTypes.h"
#include "mlir/IR/Location.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/IR/Operation.h"
#include "mlir/IR/PatternMatch.h"
#include "mlir/IR/Value.h"
#include "mlir/IR/Verifier.h"
#include "mlir/Support/LLVM.h"
#include "llvm/ADT/StringRef.h"
#include "llvm/Transforms/Utils/Cloning.h"
#include <cstdint>
#include <memory>
#include <vector>

using namespace mlir;

//==-----------------------==//
//==-- UTILITY FUNCTIONS --==//
//==-----------------------==//
mlir::Type MLIRGenerator::getMLIRType(NES::Operation::BasicType type) {
    switch (type) {
        case NES::Operation::INT1:
        case NES::Operation::BOOLEAN: return builder->getIntegerType(1);
        case NES::Operation::INT8:
        case NES::Operation::CHAR: return builder->getIntegerType(8);
        case NES::Operation::INT16: return builder->getIntegerType(16);
        case NES::Operation::INT32: return builder->getIntegerType(32);
        case NES::Operation::INT64: return builder->getIntegerType(64);
        case NES::Operation::FLOAT: return Float32Type::get(context);
        case NES::Operation::DOUBLE: return Float64Type::get(context);
        case NES::Operation::INT8PTR: return LLVM::LLVMPointerType::get(builder->getI8Type());
        default: return builder->getIntegerType(32);
    }
}

mlir::Value MLIRGenerator::getConstInt(Location loc, int numBits, int64_t value) {
    return builder->create<LLVM::ConstantOp>(loc,
                                             builder->getIntegerType(numBits),
                                             builder->getIntegerAttr(builder->getIndexType(), value));
}

void MLIRGenerator::insertComment(const std::string& comment) {
    mlir::ValueRange emptyRange{};
    builder->create<LLVM::UndefOp>(getNameLoc(comment),
                                   LLVM::LLVMVoidType::get(context),
                                   emptyRange,
                                   builder->getNamedAttr(comment, builder->getZeroAttr(builder->getI32Type())));
}

Location MLIRGenerator::getNameLoc(const std::string& name) {
    auto baseLocation = mlir::FileLineColLoc::get(builder->getStringAttr("Query_1"), 0, 0);
    return NameLoc::get(builder->getStringAttr(name), baseLocation);
}

mlir::arith::CmpIPredicate convertToMLIRComparison(NES::PredicateOperation::BinaryOperatorType comparisonType) {
    //TODO extend to all comparison types
    switch (comparisonType) {
        case (NES::PredicateOperation::BinaryOperatorType::EQUAL_OP): return mlir::arith::CmpIPredicate::eq;
        case (NES::PredicateOperation::BinaryOperatorType::LESS_THAN_OP): return mlir::arith::CmpIPredicate::slt;
        case (NES::PredicateOperation::BinaryOperatorType::UNEQUAL_OP): return mlir::arith::CmpIPredicate::ne;
    }
}

//==-----------------------------------==//
//==-- Create & Insert Functionality --==//
//==-----------------------------------==//
FlatSymbolRefAttr MLIRGenerator::insertExternalFunction(const std::string& name,
                                                        uint8_t numResultBits,
                                                        std::vector<mlir::Type> argTypes,
                                                        bool varArgs) {
    // If function was declared already, return reference.
    if (theModule.lookupSymbol<LLVM::LLVMFuncOp>(name))
        return SymbolRefAttr::get(context, name);
    // Create function arg & result types (currently only int for result).
    LLVM::LLVMFunctionType llvmFnType;
    if (numResultBits != 0) {
        llvmFnType = LLVM::LLVMFunctionType::get(builder->getIntegerType(numResultBits), argTypes, varArgs);
    } else {
        llvmFnType = LLVM::LLVMFunctionType::get(LLVM::LLVMVoidType::get(context), argTypes, varArgs);
    }
    // The InsertionGuard saves the current IP and restores it after scope is left.
    PatternRewriter::InsertionGuard insertGuard(*builder);
    builder->restoreInsertionPoint(*globalInsertPoint);
    // Create function in global scope. Return reference.
    builder->create<LLVM::LLVMFuncOp>(theModule.getLoc(), name, llvmFnType, LLVM::Linkage::External, false);
    return SymbolRefAttr::get(context, name);
}

Value MLIRGenerator::insertProxyCall(const std::string& funcName,
                                     const std::string& ptrName,
                                     NES::Operation::BasicType returnType) {
    printf("KEK: %d", returnType);
    // We cannot use LLVM::LLVMVoidType or MLIR::OpaqueType to get a void*.
    // Instead we represent void* via int8*.
    auto currentInsertionPoint = builder->saveInsertionPoint();
    builder->restoreInsertionPoint(*globalInsertPoint);

    // Create global function signature. Get Address of inserted object pointer.
//    auto proxyFunc = insertExternalFunction(funcName, 64, {}, true); //Todo varArgs necessary?
    auto numTuplesType = mlir::FunctionType::get(context, LLVM::LLVMPointerType::get(
                                                              LLVM::LLVMPointerType::get(builder->getI8Type())),
                                                 builder->getIndexType());
    //Todo find way to add external linkage?
    auto proxyFunc = builder->create<mlir::FuncOp>(theModule.getLoc(), funcName, numTuplesType, builder->getStringAttr("private"));//, LLVM::Linkage::External, false);

    auto objectGEP = builder->create<LLVM::GlobalOp>(getNameLoc("TB Pointer"),
                                                     LLVM::LLVMPointerType::get(builder->getIntegerType(8)),
                                                     false,
                                                     LLVM::Linkage::External,
                                                     StringRef{ptrName},
                                                     Attribute{});

    // Load object address locally. Call proxy member function with objectPtr.
    builder->restoreInsertionPoint(currentInsertionPoint);
    Value objectPtr = builder->create<LLVM::AddressOfOp>(getNameLoc("loadPtr"), objectGEP);
    return builder->create<mlir::CallOp>(getNameLoc("memberCall"), proxyFunc, objectPtr).getResult(0);
}

//==---------------------------------==//
//==-- MAIN WORK - Generating MLIR --==//
//==---------------------------------==//
MLIRGenerator::MLIRGenerator(mlir::MLIRContext& context) : context(&context) {
    builder = std::make_shared<OpBuilder>(&context);
    this->theModule = mlir::ModuleOp::create(getNameLoc("module"));

    // Store InsertPoint for inserting globals such as Strings or TupleBuffers.
    globalInsertPoint = new mlir::RewriterBase::InsertPoint(theModule.getBody(), theModule.begin());
    // Insert printFromMLIR function for debugging use.
    printfReference = insertExternalFunction("printFromMLIR", 32, {builder->getIntegerType(8)}, true);
};

mlir::ModuleOp MLIRGenerator::generateModuleFromNESIR(NES::NESIR* nesIR) {
    //Todo load TupleBuffer as int64 pointer
    generateMLIR(nesIR->getRootOperation());

    //Todo should perform this on higher level!!!
    // Check if module is valid and return.
    theModule->dump();
    if (failed(mlir::verify(theModule))) {
        theModule.emitError("module verification error");
        return nullptr;
    }
    return theModule;
}

void MLIRGenerator::generateMLIR(NES::BasicBlockPtr basicBlock) {
    // Fixme: Instead of having a 'rootBlock' we have a rootOperation
    //  - We start generating MLIR from NESIR by calling generateMLIR(rootOperation)
    //   - an Operation can have BasicBlocks (LoopOperation, IfOperation)
    //    - if an Operation has a BasicBlock, we adjust the insertionPoint during MLIRGeneration
    //    - INTUITION: An Operation with a BasicBlock creates a new scope.
    for (const auto& operation : basicBlock->getOperations()) {
        generateMLIR(operation);
    }
}

Value MLIRGenerator::generateMLIR(const NES::OperationPtr& operation) {
    switch (operation->getOperationType()) {
        case NES::Operation::OperationType::FunctionOp:
            return generateMLIR(std::static_pointer_cast<NES::FunctionOperation>(operation));
        case NES::Operation::OperationType::LoopOp: return generateMLIR(std::static_pointer_cast<NES::LoopOperation>(operation));
        case NES::Operation::OperationType::ConstantOp:
            return generateMLIR(std::static_pointer_cast<NES::ConstantIntOperation>(operation));
        case NES::Operation::OperationType::AddOp: return generateMLIR(std::static_pointer_cast<NES::AddIntOperation>(operation));
        case NES::Operation::OperationType::LoadOp: return generateMLIR(std::static_pointer_cast<NES::LoadOperation>(operation));
        case NES::Operation::OperationType::AddressOp:
            return generateMLIR(std::static_pointer_cast<NES::AddressOperation>(operation));
        case NES::Operation::OperationType::StoreOp:
            return generateMLIR(std::static_pointer_cast<NES::StoreOperation>(operation));
        case NES::Operation::OperationType::PredicateOp:
            return generateMLIR(std::static_pointer_cast<NES::PredicateOperation>(operation));
    }
}

Value MLIRGenerator::generateMLIR(std::shared_ptr<NES::FunctionOperation> functionOperation) {

    //Todo establish TupleBuffer as input
    // - [ ] call getNumTuples() to get the number of tuples available in TB -> proxy first, later INLINE
    //   - we do NOT need to get recordWidth or similar, because SCHEMA IS STATIC! -> can statically set via AddressOperation
    // - [ ] set numTuples as upper limit of ForLoop!
    // - [ ] use TupleBuffer* as input instead of int8_t* for execute()
    //   - [ ] call getBuffer proxy function to get dataBufferPtr as int8_t
    //   - [ ] connect to previously established flow
    // - [ ] use more elegant way to add JIT symbols and function addresses
    //   - how to do it in general?

    // Generate execute function. Set input/output types and get its entry block.
    llvm::SmallVector<mlir::Type, 4> inputTypes(0);
    for (auto inputArg : functionOperation->getInputArgs()) {
        inputTypes.emplace_back(getMLIRType(inputArg));
    }
    llvm::SmallVector<mlir::Type, 4> outputTypes(1, getMLIRType(functionOperation->getOutputArg()));
    auto functionInOutTypes = builder->getFunctionType(inputTypes, outputTypes);

    auto mlirFunction = mlir::FuncOp::create(getNameLoc("EntryPoint"), functionOperation->getName(), functionInOutTypes);

    //avoid mangling
    mlirFunction->setAttr("llvm.emit_c_interface", mlir::UnitAttr::get(context));
    mlirFunction.addEntryBlock();

    // Set InsertPoint to beginning of the execute function.
    builder->setInsertionPointToStart(&mlirFunction.getBody().front());

    // Store references to function args in the functionArgs map.
    auto functionArgsIterator = mlirFunction.args_begin();
    for (int i = 0; i < (int) functionOperation->getInputArgNames().size(); ++i) {
        functionArgs.emplace(std::pair{functionOperation->getInputArgNames().at(i), functionArgsIterator[i]});
    }

    numTuples = insertProxyCall("getNumTuples", "InputTupleBuffer", NES::Operation::BasicType::INT64);

    // Generate MLIR for operations in function body (BasicBlock)
    generateMLIR(functionOperation->getFunctionBasicBlock());

    // Insert return into 'execute' function block. This is the FINAL return.
    builder->create<LLVM::ReturnOp>(getNameLoc("return"), constZero);//return zero if successful

    theModule.push_back(mlirFunction);
    return constZero;
}

// Todo define entry point here. Is return correct?
Value MLIRGenerator::generateMLIR(std::shared_ptr<NES::LoopOperation> loopOperation) {
    //==------------------------------==//
    //==--------- LOOP HEAD ----------==//
    //==------------------------------==//
    // Create 0 constant accessible in execute function scope
    constZero = getConstInt(getNameLoc("constant32Zero"), 64, 0);
    constOne = getConstInt(getNameLoc("const64One"), 64, 1);

    // Define index variables for loop. Define the loop iteration variable
    Value iterationVariable = getConstInt(getNameLoc("IterationVar"), 64, 0);

//    auto upperBound = numTuples.dyn_cast<arith::ConstantIndexOp>();
    auto lowerBound = builder->create<arith::ConstantIndexOp>(getNameLoc("lowerBound"), 0);
    auto stepSize = builder->create<arith::ConstantIndexOp>(getNameLoc("stepSize"), 1);
//    auto upperBound = builder->create<LLVM::ConstantOp>(getNameLoc("fakeUpperBound"), builder->getIndexType(), builder->getI64IntegerAttr(2));

    //==------------------------------==//
    //==--------- LOOP BODY ----------==//
    //==------------------------------==//
    insertComment("// Main For Loop");
    //Todo numTuples -> index for upperBoundArg
//    auto fixedUpperBound = builder->create<arith::BitcastOp>(getNameLoc("fixedUB"), builder->getIndexType(), numTuples);
    auto forLoop = builder->create<scf::ForOp>(getNameLoc("forLoop"), lowerBound, numTuples, stepSize, iterationVariable);

    // Set Insertion Point (IP) to inside of loop body. Process child node. Restore IP on return.
    auto currentInsertionPoint = builder->saveInsertionPoint();
    builder->setInsertionPointToStart(forLoop.getBody());
    currentRecordIdx = forLoop.getRegionIterArgs().begin()[0];
    generateMLIR(loopOperation->getLoopBasicBlock());
    Value increasedIdx = builder->create<LLVM::AddOp>(getNameLoc("++iterationVar"), currentRecordIdx, constOne);
    builder->create<scf::YieldOp>(getNameLoc("yieldOperation"), increasedIdx);

    builder->restoreInsertionPoint(currentInsertionPoint);

    return constZero;
}

Value MLIRGenerator::generateMLIR(std::shared_ptr<NES::ConstantIntOperation> constIntOp) {
    printf("ConstIntValue: %ld\n", constIntOp->getConstantIntValue());
    //    builder->clearInsertionPoint();
    // TODO use dynamic type from NESIR
    Value constVal = builder->create<LLVM::ConstantOp>(getNameLoc("Constant Op"),
                                                       builder->getIntegerType(64),
                                                       builder->getI64IntegerAttr(constIntOp->getConstantIntValue()));
    return constVal;
}
Value MLIRGenerator::generateMLIR(std::shared_ptr<NES::AddIntOperation> addOp) {
    printf("Is add op: %d\n", addOp->classof(addOp.get()));

    //Todo cannot call RHS twice
    Value additionResult =
        builder->create<LLVM::AddOp>(getNameLoc("binOpResult"), generateMLIR(addOp->getLHS()), generateMLIR(addOp->getRHS()));
    auto printValFunc = insertExternalFunction("printValueFromMLIR", 0, {}, true);
    builder->create<LLVM::CallOp>(getNameLoc("printFunc"), mlir::None, printValFunc, additionResult);
    return additionResult;
}

Value MLIRGenerator::generateMLIR(std::shared_ptr<NES::AddressOperation> addressOp) {
    printf("Address Op field offset: %ld\n", addressOp->getFieldOffset());
    // Calculate current offset //Todo dynamically calculate offset -> also for multiple types/fields
    //        Value globalArrayPtr = builder->create<LLVM::AddressOfOp>(getNameLoc("loadedArray"), inputBufferPtr);
    Value recordOffset = builder->create<LLVM::MulOp>(getNameLoc("recordOffset"),
                                                      currentRecordIdx,
                                                      getConstInt(getNameLoc("1"), 64, addressOp->getRecordWidth()));
    Value fieldOffset = builder->create<LLVM::AddOp>(getNameLoc("fieldOffset"),
                                                     recordOffset,
                                                     getConstInt(getNameLoc("1"), 64, addressOp->getFieldOffset()));
    //return I8* to first byte of field data
    Value elementAddress = builder->create<LLVM::GEPOp>(getNameLoc("fieldAccess"),
                                                        LLVM::LLVMPointerType::get(builder->getI8Type()),
                                                        functionArgs["inputBuffer"],
                                                        ArrayRef<Value>({fieldOffset}));
    return builder->create<LLVM::BitcastOp>(getNameLoc("I64 addr"),
                                            LLVM::LLVMPointerType::get(builder->getI64Type()),
                                            elementAddress);
}

Value MLIRGenerator::generateMLIR(std::shared_ptr<NES::LoadOperation> loadOp) {
    printf("Load Op.\n");
    Value loadedVal = builder->create<LLVM::LoadOp>(getNameLoc("loadedValue"), generateMLIR(loadOp->getAddressOp()));
    //        Value zextVal = builder->create<LLVM::ZExtOp>(getNameLoc("zext"), builder->getI64Type(), loadedVal);
    return loadedVal;
}

Value MLIRGenerator::generateMLIR(std::shared_ptr<NES::StoreOperation> storeOp) {
    printf("OutputBufferIdx[0]: %ld\n", storeOp->getOutputBufferIdx());
    //Todo calculate store offset -> should require another addressOfOp
    //        Value fieldPointer = builder->create<LLVM::AddressOfOp>(getNameLoc("loadedArray"), outputBufferGlobal);
    Value castedAddress = builder->create<LLVM::BitcastOp>(getNameLoc("Casted Address"),
                                                           LLVM::LLVMPointerType::get(builder->getI64Type()),
                                                           functionArgs["outputBuffer"]);
    //        Value indexWithOffset = builder->create<LLVM::MulOp>(getNameLoc("indexWithOffset"),
    //                                                             getConstInt(getNameLoc("const 8"), 32, 8), currentRecordIdx);
    auto outputPtr = builder->create<LLVM::GEPOp>(getNameLoc("fieldAccess"),
                                                  LLVM::LLVMPointerType::get(builder->getI64Type()),
                                                  castedAddress,
                                                  ArrayRef<Value>({currentRecordIdx}));
    builder->create<LLVM::StoreOp>(getNameLoc("outputStore"), generateMLIR(storeOp->getValueToStore()), outputPtr);
    return constZero;// Todo: this should not return a value.
}