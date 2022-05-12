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

#include "Experimental/NESIR/Operations/CompareOperation.hpp"
#include "Experimental/NESIR/Operations/IfOperation.hpp"
#include "Experimental/NESIR/Operations/Operation.hpp"
#include "Experimental/NESIR/Operations/ProxyCallOperation.hpp"
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
#include <exception>
#include <memory>
#include <mlir/Parser.h>
#include <unordered_map>
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

mlir::Value MLIRGenerator::getConstInt(const std::string& location, int numBits, int64_t value) {
    return builder->create<LLVM::ConstantOp>(getNameLoc(location),
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

std::vector<mlir::FuncOp> MLIRGenerator::GetMemberFunctions(mlir::MLIRContext& context) {
    // Important: This needs to be synced with Operation::MemberFunctions
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
    std::vector<mlir::FuncOp> memberFunctions;
    auto simpleModule = parseSourceString(moduleStr2, &context);
    auto proxyOpIterator = simpleModule->getOps().begin()->getBlock()->getOperations().begin();
    
    for(;proxyOpIterator != simpleModule->getOps().begin()->getBlock()->getOperations().end(); proxyOpIterator++) {
        memberFunctions.push_back(static_cast<mlir::FuncOp>(proxyOpIterator->clone()));
    }
    return memberFunctions;
}

//==---------------------------------==//
//==-- MAIN WORK - Generating MLIR --==//
//==---------------------------------==//
MLIRGenerator::MLIRGenerator(mlir::MLIRContext& context, std::vector<mlir::FuncOp>& memberFunctions)
    : context(&context), memberFunctions(memberFunctions) {
    // Create builder object, which helps to generate MLIR. Create Module, which contains generated MLIR.
    builder = std::make_shared<OpBuilder>(&context);
    this->theModule = mlir::ModuleOp::create(getNameLoc("module"));
    // Insert all needed MemberClassFunctions into the MLIR module.
    // Todo handle differently
    // for(auto memberFunction : memberFunctions) {
    //     theModule.push_back(memberFunction);
    // }
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

void MLIRGenerator::generateMLIR(NES::BasicBlockPtr basicBlock, const std::unordered_map<std::string, mlir::Value>& blockArgs) {

    for (const auto& operation : basicBlock->getOperations()) {
        generateMLIR(operation, blockArgs);
    }
}

void MLIRGenerator::generateMLIR(const NES::OperationPtr& operation, const std::unordered_map<std::string, mlir::Value>& blockArgs) {
    switch (operation->getOperationType()) {
        case NES::Operation::OperationType::FunctionOp: generateMLIR(std::static_pointer_cast<NES::FunctionOperation>(operation), blockArgs); break;
        case NES::Operation::OperationType::LoopOp: generateMLIR(std::static_pointer_cast<NES::LoopOperation>(operation), blockArgs); break;
        case NES::Operation::OperationType::ConstantOp:generateMLIR(std::static_pointer_cast<NES::ConstantIntOperation>(operation), blockArgs); break;
        case NES::Operation::OperationType::AddOp: generateMLIR(std::static_pointer_cast<NES::AddIntOperation>(operation), blockArgs); break;
        case NES::Operation::OperationType::LoadOp: generateMLIR(std::static_pointer_cast<NES::LoadOperation>(operation), blockArgs); break;
        case NES::Operation::OperationType::AddressOp: generateMLIR(std::static_pointer_cast<NES::AddressOperation>(operation), blockArgs); break;
        case NES::Operation::OperationType::StoreOp: generateMLIR(std::static_pointer_cast<NES::StoreOperation>(operation), blockArgs); break;
        case NES::Operation::OperationType::PredicateOp: generateMLIR(std::static_pointer_cast<NES::PredicateOperation>(operation), blockArgs); break;
        case NES::Operation::OperationType::BranchOp: generateMLIR(std::static_pointer_cast<NES::BranchOperation>(operation), blockArgs); break;
        case NES::Operation::OperationType::IfOp: generateMLIR(std::static_pointer_cast<NES::IfOperation>(operation), blockArgs); break;
        case NES::Operation::OperationType::CompareOp: generateMLIR(std::static_pointer_cast<NES::CompareOperation>(operation), blockArgs); break;
        case NES::Operation::OperationType::ProxyCallOp: generateMLIR(std::static_pointer_cast<NES::ProxyCallOperation>(operation), blockArgs); break;
        case NES::Operation::OperationType::ReturnOp: generateMLIR(std::static_pointer_cast<NES::ReturnOperation>(operation), blockArgs); break;
    }
}

// Recursion. No dependencies. Does NOT require addressMap insertion.
void MLIRGenerator::generateMLIR(std::shared_ptr<NES::FunctionOperation> functionOp, const std::unordered_map<std::string, mlir::Value>& blockArgs) {
    // Generate execute function. Set input/output types and get its entry block.
    llvm::SmallVector<mlir::Type, 4> inputTypes(0);
    for (auto inputArg : functionOp->getInputArgs()) {
        inputTypes.emplace_back(getMLIRType(inputArg));
    }
    llvm::SmallVector<mlir::Type, 4> outputTypes(1, getMLIRType(functionOp->getOutputArg()));
    auto functionInOutTypes = builder->getFunctionType(inputTypes, outputTypes);

    auto mlirFunction = mlir::FuncOp::create(getNameLoc("EntryPoint"), functionOp->getName(), functionInOutTypes);

    //avoid mangling
    mlirFunction->setAttr("llvm.emit_c_interface", mlir::UnitAttr::get(context));
    mlirFunction.addEntryBlock();

    // Set InsertPoint to beginning of the execute function.
    builder->setInsertionPointToStart(&mlirFunction.getBody().front());

    // Store references to function args in the valueMap map.
    // Todo it would be great if we could insert these values function arg inputs as values into the addressMap
    // - but: since we use (smart)pointers/addresses as keys, 
    auto valueMapIterator = mlirFunction.args_begin();
    for (int i = 0; i < (int) functionOp->getInputArgNames().size(); ++i) {
        functionValuesMap.emplace(std::pair{functionOp->getName() + functionOp->getInputArgNames().at(i), valueMapIterator[i]});
    }
    std::unordered_map<NES::OperationPtr*, Value> addressValueMap;
    
    // Handles special execute() function case. We could also handle this by emplacing MemberFunctionResults when needed.
    if(functionOp->getName() == "execute") {
        functionValuesMap.emplace(std::pair{"numTuples", builder->create<mlir::CallOp>(getNameLoc("memberCall"),
                                    memberFunctions[NES::Operation::GetNumTuples], functionValuesMap["executeInputBuffer"]).getResult(0)});
        functionValuesMap.emplace(std::pair{"inputBuffer", builder->create<mlir::CallOp>(getNameLoc("memberCall"),
                                    memberFunctions[NES::Operation::GetDataBuffer], functionValuesMap["executeInputBuffer"]).getResult(0)});   
        functionValuesMap.emplace(std::pair{"outputBuffer", builder->create<mlir::CallOp>(getNameLoc("memberCall"),
                                    memberFunctions[NES::Operation::GetDataBuffer], functionValuesMap["executeOutputBuffer"]).getResult(0)});   
    }

    // Generate MLIR for operations in function body (BasicBlock)
    generateMLIR(functionOp->getFunctionBasicBlock());

    theModule.push_back(mlirFunction);
}

// Recursion. No dependencies. Does NOT require addressMap insertion.
void MLIRGenerator::generateMLIR(std::shared_ptr<NES::LoopOperation> loopOp, const std::unordered_map<std::string, mlir::Value>& blockArgs) {
    if(loopOp->getLoopType() == NES::LoopOperation::LoopType::ForLoOp, const std::unordered_map<std::string, mlir::Value>& blockArgs) {
        // -=LOOP HEAD=-
        // Define index variables for loop.
        auto loopBranchOp = std::static_pointer_cast<NES::BranchOperation>(loopOp->getLoopBranchOp());
        // Pop Branch Terminator Operation at the end of loop block to circumvent infinite recursion.
        loopBranchOp->getNextBlock()->getOperations().pop_back();
        auto lowerBound = builder->create<arith::ConstantIndexOp>(getNameLoc("lowerBound"), 0);
        auto stepSize = builder->create<arith::ConstantIndexOp>(getNameLoc("stepSize"), 1);
        // Create Loop Operation. Save InsertionPoint(IP) of parent's BasicBlock
        insertComment("// For Loop Start");
        auto forLoop = builder->create<scf::ForOp>(getNameLoc("forLoop"), lowerBound, functionValuesMap["numTuples"], stepSize);
        auto parentBlockInsertionPoint = builder->saveInsertionPoint();

        // -=LOOP BODY=-
        // Set Insertion Point(IP) to loop BasicBlock. Process all Operations in loop BasicBlock. Restore IP to parent's BasicBlock.
        builder->setInsertionPointToStart(forLoop.getBody());
        currentRecordIdx = builder->create<arith::IndexCastOp>(getNameLoc("InductionVar Cast"), builder->getI64Type(), forLoop.getInductionVar());
        generateMLIR(loopBranchOp->getNextBlock());
        // generateMLIR(loopOperation->getLoopBasicBlock());
        builder->restoreInsertionPoint(parentBlockInsertionPoint);
    } else {
        throw std::exception{};
    }
}

// No Recursion. No dependencies. Requires addressMap insertion.
void MLIRGenerator::generateMLIR(std::shared_ptr<NES::AddressOperation> addressOp, const std::unordered_map<std::string, mlir::Value>& blockArgs) {
    printf("AddressOp");
    Value recordOffset = builder->create<LLVM::MulOp>(getNameLoc("recordOffset"),
                                                      currentRecordIdx,
                                                      getConstInt("1", 64, addressOp->getRecordWidth()));
    Value fieldOffset = builder->create<LLVM::AddOp>(getNameLoc("fieldOffset"),
                                                     recordOffset,
                                                     getConstInt("1", 64, addressOp->getFieldOffset()));
    std::string bufferName = (addressOp->getIsInputBuffer()) ? "inputBuffer" : "outputBuffer";
    // Return I8* to first byte of field data
    Value elementAddress = builder->create<LLVM::GEPOp>(getNameLoc("fieldAccess"),
                                                        LLVM::LLVMPointerType::get(builder->getI8Type()),
                                                        functionValuesMap[bufferName],
                                                        ArrayRef<Value>({fieldOffset}));
    addressValueMap.emplace(std::pair{addressOp, builder->create<LLVM::BitcastOp>(getNameLoc("Address Bitcasted"),
                                            LLVM::LLVMPointerType::get(getMLIRType(addressOp->getDataType())),
                                            elementAddress)});
}

// No recursion. Dependencies. Requires addressMap insertion.
void MLIRGenerator::generateMLIR(std::shared_ptr<NES::LoadOperation> loadOp, const std::unordered_map<std::string, mlir::Value>& blockArgs) {
    printf("LoadOp.\n");
    addressValueMap.emplace(loadOp, builder->create<LLVM::LoadOp>(getNameLoc("loadedValue"), addressValueMap[loadOp->getAddressOp()]));
}

// No Recursion. No dependencies. Requires addressMap insertion.
void MLIRGenerator::generateMLIR(std::shared_ptr<NES::ConstantIntOperation> constIntOp, const std::unordered_map<std::string, mlir::Value>& blockArgs) {
    printf("ConstIntOp");
    addressValueMap.emplace(std::pair{constIntOp, getConstInt("ConstantOp", constIntOp->getNumBits(), constIntOp->getConstantIntValue())});
}

// No recursion. Dependencies. Requires addressMap insertion.
void MLIRGenerator::generateMLIR(std::shared_ptr<NES::AddIntOperation> addOp, const std::unordered_map<std::string, mlir::Value>& blockArgs) {
    printf("AddOp");
    addressValueMap.emplace(std::pair{addOp,
        builder->create<LLVM::AddOp>(getNameLoc("binOpResult"), addressValueMap[addOp->getLHS()], addressValueMap[addOp->getRHS()])});
    auto printValFunc = insertExternalFunction("printValueFromMLIR", 0, {}, true);
    builder->create<LLVM::CallOp>(getNameLoc("printFunc"), mlir::None, printValFunc, addressValueMap[addOp]);
}

// No recursion. Dependencies. Does NOT require addressMap insertion. 
void MLIRGenerator::generateMLIR(std::shared_ptr<NES::StoreOperation> storeOp, const std::unordered_map<std::string, mlir::Value>& blockArgs) {
    printf("StoreOp\n");
    builder->create<LLVM::StoreOp>(getNameLoc("outputStore"), addressValueMap[storeOp->getValueToStore()], 
                                   addressValueMap[storeOp->getAddressOp()]);
}

// No recursion. Dependencies. Does NOT require addressMap insertion. 
void MLIRGenerator::generateMLIR(std::shared_ptr<NES::ReturnOperation> returnOp, const std::unordered_map<std::string, mlir::Value>& blockArgs) {
    printf("ReturnOp\n");
    // Insert return into 'execute' function block. This is the FINAL return.
    builder->create<LLVM::ReturnOp>(getNameLoc("return"), getConstInt("return", 64, returnOp->getReturnOpCode()));
}

// No recursion. Dependencies. Does NOT require addressMap insertion. 
void MLIRGenerator::generateMLIR(std::shared_ptr<NES::ProxyCallOperation> proxyCallOp, const std::unordered_map<std::string, mlir::Value>& blockArgs) {
    printf("ProxyCallOperation identifier: %s\n", proxyCallOp->getIdentifier().c_str());
    switch(proxyCallOp->getProxyCallType()) {
        case NES::Operation::GetDataBuffer:
            theModule.push_back(this->memberFunctions.at(0));
            break;
        case NES::Operation::GetNumTuples:
            theModule.push_back(this->memberFunctions.at(0));
            break;
        default:
            insertExternalFunction(proxyCallOp->getIdentifier(), 64, {builder->getI64Type()}, true); //Todo handle correctly
    }
}

//==--------------------- Dummy OPERATIONS ----------------------==//
// No recursion. Dependencies. Does NOT require addressMap insertion. 
void MLIRGenerator::generateMLIR(std::shared_ptr<NES::IfOperation> ifOp, const std::unordered_map<std::string, mlir::Value>& blockArgs) {
    printf("IfOperation boolArgName: %s\n", ifOp->getBoolArgName().c_str());
}
// No recursion. Dependencies. Does NOT require addressMap insertion. 
void MLIRGenerator::generateMLIR(std::shared_ptr<NES::CompareOperation> compareOp, const std::unordered_map<std::string, mlir::Value>& blockArgs) {
    printf("CompareOperation identifier: %s\n", compareOp->getIdentifier().c_str());
}
// No recursion. Dependencies. Does NOT require addressMap insertion. 
void MLIRGenerator::generateMLIR(std::shared_ptr<NES::BranchOperation> branchOp, const std::unordered_map<std::string, mlir::Value>& blockArgs) {
    printf("BranchOperation.getNextBlock.getIdentifier: %s\n", branchOp->getNextBlock()->getIdentifier().c_str());
}