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
#include <mlir/Parser.h>
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

std::vector<mlir::FuncOp> MLIRGenerator::insertClassMemberFunctions(mlir::MLIRContext& context) {
    std::string moduleStr2 = R"mlir(
        module {
            func private @getNumTuples(%arg0: !llvm.ptr<i8>) -> index {
                %0 = llvm.mlir.constant(1 : i32) : i32
                %1 = llvm.mlir.constant(0 : i64) : i64
                %2 = llvm.bitcast %arg0 : !llvm.ptr<i8> to !llvm.ptr<ptr<struct<"class.NES::Runtime::detail::BufferControlBlock", (struct<"struct.std::atomic", (struct<"struct.std::__atomic_base", (i32)>)>, i32, i64, i64, i64, i64, ptr<struct<"class.NES::Runtime::detail::MemorySegment", (ptr<i8>, i32, ptr<struct<"class.NES::Runtime::detail::BufferControlBlock">>)>>, struct<"struct.std::atomic.0", (struct<"struct.std::__atomic_base.1", (ptr<struct<"class.NES::Runtime::BufferRecycler", opaque>>)>)>, struct<"class.std::function", (struct<"class.std::_Function_base", (struct<"union.std::_Any_data", (struct<"union.std::_Nocopy_types", (struct<(i64, i64)>)>)>, ptr<func<i1 (ptr<struct<"union.std::_Any_data", (struct<"union.std::_Nocopy_types", (struct<(i64, i64)>)>)>>, ptr<struct<"union.std::_Any_data", (struct<"union.std::_Nocopy_types", (struct<(i64, i64)>)>)>>, i32)>>)>, ptr<func<void (ptr<struct<"union.std::_Any_data", (struct<"union.std::_Nocopy_types", (struct<(i64, i64)>)>)>>, ptr<ptr<struct<"class.NES::Runtime::detail::MemorySegment", (ptr<i8>, i32, ptr<struct<"class.NES::Runtime::detail::BufferControlBlock">>)>>>, ptr<ptr<struct<"class.NES::Runtime::BufferRecycler", opaque>>>)>>)>)>>>
                %3 = llvm.load %2 : !llvm.ptr<ptr<struct<"class.NES::Runtime::detail::BufferControlBlock", (struct<"struct.std::atomic", (struct<"struct.std::__atomic_base", (i32)>)>, i32, i64, i64, i64, i64, ptr<struct<"class.NES::Runtime::detail::MemorySegment", (ptr<i8>, i32, ptr<struct<"class.NES::Runtime::detail::BufferControlBlock">>)>>, struct<"struct.std::atomic.0", (struct<"struct.std::__atomic_base.1", (ptr<struct<"class.NES::Runtime::BufferRecycler", opaque>>)>)>, struct<"class.std::function", (struct<"class.std::_Function_base", (struct<"union.std::_Any_data", (struct<"union.std::_Nocopy_types", (struct<(i64, i64)>)>)>, ptr<func<i1 (ptr<struct<"union.std::_Any_data", (struct<"union.std::_Nocopy_types", (struct<(i64, i64)>)>)>>, ptr<struct<"union.std::_Any_data", (struct<"union.std::_Nocopy_types", (struct<(i64, i64)>)>)>>, i32)>>)>, ptr<func<void (ptr<struct<"union.std::_Any_data", (struct<"union.std::_Nocopy_types", (struct<(i64, i64)>)>)>>, ptr<ptr<struct<"class.NES::Runtime::detail::MemorySegment", (ptr<i8>, i32, ptr<struct<"class.NES::Runtime::detail::BufferControlBlock">>)>>>, ptr<ptr<struct<"class.NES::Runtime::BufferRecycler", opaque>>>)>>)>)>>>
                %4 = llvm.getelementptr %3[%1, 1] : (!llvm.ptr<struct<"class.NES::Runtime::detail::BufferControlBlock", (struct<"struct.std::atomic", (struct<"struct.std::__atomic_base", (i32)>)>, i32, i64, i64, i64, i64, ptr<struct<"class.NES::Runtime::detail::MemorySegment", (ptr<i8>, i32, ptr<struct<"class.NES::Runtime::detail::BufferControlBlock">>)>>, struct<"struct.std::atomic.0", (struct<"struct.std::__atomic_base.1", (ptr<struct<"class.NES::Runtime::BufferRecycler", opaque>>)>)>, struct<"class.std::function", (struct<"class.std::_Function_base", (struct<"union.std::_Any_data", (struct<"union.std::_Nocopy_types", (struct<(i64, i64)>)>)>, ptr<func<i1 (ptr<struct<"union.std::_Any_data", (struct<"union.std::_Nocopy_types", (struct<(i64, i64)>)>)>>, ptr<struct<"union.std::_Any_data", (struct<"union.std::_Nocopy_types", (struct<(i64, i64)>)>)>>, i32)>>)>, ptr<func<void (ptr<struct<"union.std::_Any_data", (struct<"union.std::_Nocopy_types", (struct<(i64, i64)>)>)>>, ptr<ptr<struct<"class.NES::Runtime::detail::MemorySegment", (ptr<i8>, i32, ptr<struct<"class.NES::Runtime::detail::BufferControlBlock">>)>>>, ptr<ptr<struct<"class.NES::Runtime::BufferRecycler", opaque>>>)>>)>)>>, i64) -> !llvm.ptr<i32>
                %5 = llvm.load %4 : !llvm.ptr<i32>
                %6 = llvm.zext %5 : i32 to i64
                %7 = arith.index_cast %6 : i64 to index
                return %7 : index
            }
            func private @getDataBuffer(%arg0: !llvm.ptr<i8>) -> !llvm.ptr<i8> {
                %0 = llvm.mlir.constant(8 : i64) : i64
                %1 = llvm.getelementptr %arg0[%0] : (!llvm.ptr<i8>, i64) -> !llvm.ptr<i8>
                %2 = llvm.bitcast %1 : !llvm.ptr<i8> to !llvm.ptr<ptr<i8>>
                %3 = llvm.load %2 : !llvm.ptr<ptr<i8>>
                llvm.return %3 : !llvm.ptr<i8>
            }
        }
    )mlir";
    std::vector<mlir::FuncOp> classMemberFunctions;
    auto simpleModule = parseSourceString(moduleStr2, &context);
    auto proxyOpIterator = simpleModule->getOps().begin()->getBlock()->getOperations().begin();
    
    for(;proxyOpIterator != simpleModule->getOps().begin()->getBlock()->getOperations().end(); proxyOpIterator++) {
        classMemberFunctions.push_back(static_cast<mlir::FuncOp>(proxyOpIterator->clone()));
    }
    return classMemberFunctions;
}

//==---------------------------------==//
//==-- MAIN WORK - Generating MLIR --==//
//==---------------------------------==//
MLIRGenerator::MLIRGenerator(mlir::MLIRContext& context, std::vector<mlir::FuncOp>& classMemberFunctions)
    : context(&context), classMemberFunctions(classMemberFunctions) {


    // Create builder object, which helps to generate MLIR. Create Module, which contains generated MLIR.
    builder = std::make_shared<OpBuilder>(&context);
    this->theModule = mlir::ModuleOp::create(getNameLoc("module"));
    // Insert all needed MemberClassFunctions into the MLIR module.
    for(auto memberFunction : classMemberFunctions) {
        theModule.push_back(memberFunction);
    }
    // Store InsertPoint for inserting globals such as Strings or TupleBuffers.
    globalInsertPoint = new mlir::RewriterBase::InsertPoint(theModule.getBody(), theModule.begin());
    // Insert printFromMLIR function for debugging use.
    printfReference = insertExternalFunction("printFromMLIR", 32, {builder->getIntegerType(8)}, true);
};

mlir::ModuleOp MLIRGenerator::generateModuleFromNESIR(NES::NESIR* nesIR) {
    generateMLIR(nesIR->getRootOperation());
    theModule->dump();
    if (failed(mlir::verify(theModule))) {
        theModule.emitError("module verification error");
        return nullptr;
    }
    return theModule;
}

void MLIRGenerator::generateMLIR(NES::BasicBlockPtr basicBlock) {
    //Todo change scopes of current InsertionPoint here!
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

    // Store references to function args in the valueMap map.
    auto valueMapIterator = mlirFunction.args_begin();
    for (int i = 0; i < (int) functionOperation->getInputArgNames().size(); ++i) {
        valueMap.emplace(std::pair{functionOperation->getInputArgNames().at(i), valueMapIterator[i]});
    }
    // Store container for getNumberOfTuples and getDataBuffer function results in valueMap.
    valueMap.emplace(std::pair{"numTuples", builder->create<mlir::CallOp>(getNameLoc("memberCall"),
                                classMemberFunctions[ClassMemberFunctions::GetNumTuples], valueMap["InputBuffer"]).getResult(0)});
    valueMap.emplace(std::pair{"inputBuffer", builder->create<mlir::CallOp>(getNameLoc("memberCall"),
                                classMemberFunctions[ClassMemberFunctions::GetDataBuffer], valueMap["InputBuffer"]).getResult(0)});

    // Generate MLIR for operations in function body (BasicBlock)
    generateMLIR(functionOperation->getFunctionBasicBlock());

    // Insert return into 'execute' function block. This is the FINAL return.
    builder->create<LLVM::ReturnOp>(getNameLoc("return"), constZero);//return zero if successful

    theModule.push_back(mlirFunction);
    return constZero;
}

Value MLIRGenerator::generateMLIR(std::shared_ptr<NES::LoopOperation> loopOperation) {
    //==------------------------------==//
    //==--------- LOOP HEAD ----------==//
    //==------------------------------==//
    // Create 0 constant accessible in execute function scope
    constZero = getConstInt(getNameLoc("constant32Zero"), 64, 0);
    constOne = getConstInt(getNameLoc("const64One"), 64, 1);

    // Define index variables for loop. Define the loop iteration variable
    Value iterationVariable = getConstInt(getNameLoc("IterationVar"), 64, 0);
    auto lowerBound = builder->create<arith::ConstantIndexOp>(getNameLoc("lowerBound"), 0);
    auto stepSize = builder->create<arith::ConstantIndexOp>(getNameLoc("stepSize"), 1);

    //==------------------------------==//
    //==--------- LOOP BODY ----------==//
    //==------------------------------==//
    insertComment("// Main For Loop");
    auto forLoop = builder->create<scf::ForOp>(getNameLoc("forLoop"), lowerBound, valueMap["numTuples"],
                                               stepSize, iterationVariable);

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
    // TODO use dynamic type from NESIR, not simply I64
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
    Value recordOffset = builder->create<LLVM::MulOp>(getNameLoc("recordOffset"),
                                                      currentRecordIdx,
                                                      getConstInt(getNameLoc("1"), 64, addressOp->getRecordWidth()));
    Value fieldOffset = builder->create<LLVM::AddOp>(getNameLoc("fieldOffset"),
                                                     recordOffset,
                                                     getConstInt(getNameLoc("1"), 64, addressOp->getFieldOffset()));
    // Return I8* to first byte of field data
    Value elementAddress = builder->create<LLVM::GEPOp>(getNameLoc("fieldAccess"),
                                                        LLVM::LLVMPointerType::get(builder->getI8Type()),
                                                        valueMap["inputBuffer"],
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
                                                           valueMap["outputBuffer"]);
    //        Value indexWithOffset = builder->create<LLVM::MulOp>(getNameLoc("indexWithOffset"),
    //                                                             getConstInt(getNameLoc("const 8"), 32, 8), currentRecordIdx);
    auto outputPtr = builder->create<LLVM::GEPOp>(getNameLoc("fieldAccess"),
                                                  LLVM::LLVMPointerType::get(builder->getI64Type()),
                                                  castedAddress,
                                                  ArrayRef<Value>({currentRecordIdx}));
    builder->create<LLVM::StoreOp>(getNameLoc("outputStore"), generateMLIR(storeOp->getValueToStore()), outputPtr);
    return constZero;// Todo: this should not return a value.
}