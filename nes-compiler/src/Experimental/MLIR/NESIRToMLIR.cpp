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

#include <Experimental/NESIR/BasicBlocks/BasicBlock.hpp>
#include <Experimental/NESIR/Operations/AddIntOperation.hpp>
#include <Experimental/NESIR/Operations/BranchOperation.hpp>
#include <Experimental/NESIR/Operations/CompareOperation.hpp>
#include <Experimental/NESIR/Operations/IfOperation.hpp>
#include <Experimental/NESIR/Operations/LoopOperation.hpp>
#include <Experimental/NESIR/Operations/Operation.hpp>
#include <Experimental/NESIR/Operations/ProxyCallOperation.hpp>
#include <Experimental/NESIR/Operations/ReturnOperation.hpp>
#include <mlir/Dialect/Arithmetic/IR/Arithmetic.h>
#include <mlir/Dialect/LLVMIR/LLVMDialect.h>
#include <mlir/Dialect/LLVMIR/LLVMTypes.h>
#include <mlir/IR/Attributes.h>
#include <mlir/IR/Builders.h>
#include <mlir/IR/BuiltinAttributes.h>
#include <mlir/IR/BuiltinTypes.h>
#include <mlir/IR/Location.h>
#include <mlir/IR/MLIRContext.h>
#include <mlir/IR/Operation.h>
#include <mlir/IR/PatternMatch.h>
#include <mlir/IR/Value.h>
#include <mlir/IR/Verifier.h>
#include <mlir/Support/LLVM.h>
#include <llvm/ADT/StringRef.h>
#include <llvm/Transforms/Utils/Cloning.h>
#include <cstdint>
#include <exception>
#include <memory>
#include <mlir/Dialect/SCF/SCF.h>
#include <mlir/IR/TypeRange.h>
#include <mlir/Parser.h>
#include <sys/types.h>
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
        case NES::Operation::VOID: return LLVM::LLVMVoidType::get(context);
        default: return builder->getIntegerType(32);
    }
}

std::vector<mlir::Type> MLIRGenerator::getMLIRType(std::vector<NES::Operation::BasicType> types) {
    std::vector<mlir::Type> resultTypes;
    for(auto type : types) {
        resultTypes.push_back(getMLIRType(type));
    }
    return resultTypes;
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

mlir::arith::CmpIPredicate convertToMLIRComparison(NES::CompareOperation::Comparator comparisonType) {
    //TODO extend to all comparison types
    switch (comparisonType) {
        case (NES::CompareOperation::Comparator::ISLT): return mlir::arith::CmpIPredicate::slt;
        default:
            return mlir::arith::CmpIPredicate::slt;
    }
}

// Todo: Problem: loopIfOp->getElseBranchBlock()->getParentBlockLevel would result in correct Op, but is not triggered
// -> we skip correct BranchOp
NES::OperationPtr MLIRGenerator::findSameLevelBlock(NES::BasicBlockPtr thenBlock, int ifParentBlockLevel) {
    auto terminatorOp = thenBlock->getOperations().back();
    // Generate Args for next block
    if (terminatorOp->getOperationType() == NES::Operation::BranchOp) { 
        auto branchOp = std::static_pointer_cast<NES::BranchOperation>(terminatorOp);
        if(branchOp->getNextBlock()->getParentBlockLevel() <= ifParentBlockLevel) {
            return branchOp;
        } else {
            return findSameLevelBlock(branchOp->getNextBlock(), ifParentBlockLevel);
        }
    } else if (terminatorOp->getOperationType() == NES::Operation::LoopOp) {
        auto loopOp = std::static_pointer_cast<NES::LoopOperation>(terminatorOp);
        auto loopIfOp = std::static_pointer_cast<NES::IfOperation>(loopOp->getLoopHeadBlock()->getOperations().back());
        if(loopIfOp->getElseBranchBlock()->getParentBlockLevel() == ifParentBlockLevel) {
            return loopIfOp;
        } else {
            return findSameLevelBlock(loopIfOp->getElseBranchBlock(), ifParentBlockLevel);
        }
    } else { //(terminatorOp->getOperationType() == NES::Operation::IfOp)
        auto ifOp = std::static_pointer_cast<NES::IfOperation>(terminatorOp);
        return findSameLevelBlock(ifOp->getThenBranchBlock(), ifParentBlockLevel);
    }
}

//==-----------------------------------==//
//==-- Create & Insert Functionality --==//
//==-----------------------------------==//
FlatSymbolRefAttr MLIRGenerator::insertExternalFunction(const std::string& name,
                                                        mlir::Type resultType,
                                                        std::vector<mlir::Type> argTypes,
                                                        bool varArgs) {
    // Create function arg & result types (currently only int for result).
    LLVM::LLVMFunctionType llvmFnType = LLVM::LLVMFunctionType::get(resultType, argTypes, varArgs);
    // llvmFnType = LLVM::LLVMFunctionType::get(LLVM::LLVMVoidType::get(context), argTypes, varArgs);

    // The InsertionGuard saves the current IP and restores it after scope is left.
    PatternRewriter::InsertionGuard insertGuard(*builder);
    builder->restoreInsertionPoint(*globalInsertPoint);
    // Create function in global scope. Return reference.
    builder->create<LLVM::LLVMFuncOp>(theModule.getLoc(), name, llvmFnType, LLVM::Linkage::External, false);

    jitProxyFunctionSymbols.push_back(name);
    jitProxyFunctionTargetAddresses.push_back(llvm::pointerToJITTargetAddress(ProxyFunctions.getProxyFunctionAddress(name)));
    return SymbolRefAttr::get(context, name);
}

std::vector<mlir::FuncOp> MLIRGenerator::GetMemberFunctions(mlir::MLIRContext& context) {
    // Important: This needs to be synced with Operation::MemberFunctions
    std::string moduleStr2 = R"mlir(
        module {
            func private @getNumTuples(%arg0: !llvm.ptr<i8>) -> i64 {
                %0 = llvm.mlir.constant(1 : i32) : i32
                %1 = llvm.mlir.constant(0 : i64) : i64
                %2 = llvm.bitcast %arg0 : !llvm.ptr<i8> to !llvm.ptr<ptr<struct<"class.NES::Runtime::detail::BufferControlBlock", (struct<"struct.std::atomic", (struct<"struct.std::__atomic_base", (i32)>)>, i32, i64, i64, i64, i64, ptr<struct<"class.NES::Runtime::detail::MemorySegment", (ptr<i8>, i32, ptr<struct<"class.NES::Runtime::detail::BufferControlBlock">>)>>, struct<"struct.std::atomic.0", (struct<"struct.std::__atomic_base.1", (ptr<struct<"class.NES::Runtime::BufferRecycler", opaque>>)>)>, struct<"class.std::function", (struct<"class.std::_Function_base", (struct<"union.std::_Any_data", (struct<"union.std::_Nocopy_types", (struct<(i64, i64)>)>)>, ptr<func<i1 (ptr<struct<"union.std::_Any_data", (struct<"union.std::_Nocopy_types", (struct<(i64, i64)>)>)>>, ptr<struct<"union.std::_Any_data", (struct<"union.std::_Nocopy_types", (struct<(i64, i64)>)>)>>, i32)>>)>, ptr<func<void (ptr<struct<"union.std::_Any_data", (struct<"union.std::_Nocopy_types", (struct<(i64, i64)>)>)>>, ptr<ptr<struct<"class.NES::Runtime::detail::MemorySegment", (ptr<i8>, i32, ptr<struct<"class.NES::Runtime::detail::BufferControlBlock">>)>>>, ptr<ptr<struct<"class.NES::Runtime::BufferRecycler", opaque>>>)>>)>)>>>
                %3 = llvm.load %2 : !llvm.ptr<ptr<struct<"class.NES::Runtime::detail::BufferControlBlock", (struct<"struct.std::atomic", (struct<"struct.std::__atomic_base", (i32)>)>, i32, i64, i64, i64, i64, ptr<struct<"class.NES::Runtime::detail::MemorySegment", (ptr<i8>, i32, ptr<struct<"class.NES::Runtime::detail::BufferControlBlock">>)>>, struct<"struct.std::atomic.0", (struct<"struct.std::__atomic_base.1", (ptr<struct<"class.NES::Runtime::BufferRecycler", opaque>>)>)>, struct<"class.std::function", (struct<"class.std::_Function_base", (struct<"union.std::_Any_data", (struct<"union.std::_Nocopy_types", (struct<(i64, i64)>)>)>, ptr<func<i1 (ptr<struct<"union.std::_Any_data", (struct<"union.std::_Nocopy_types", (struct<(i64, i64)>)>)>>, ptr<struct<"union.std::_Any_data", (struct<"union.std::_Nocopy_types", (struct<(i64, i64)>)>)>>, i32)>>)>, ptr<func<void (ptr<struct<"union.std::_Any_data", (struct<"union.std::_Nocopy_types", (struct<(i64, i64)>)>)>>, ptr<ptr<struct<"class.NES::Runtime::detail::MemorySegment", (ptr<i8>, i32, ptr<struct<"class.NES::Runtime::detail::BufferControlBlock">>)>>>, ptr<ptr<struct<"class.NES::Runtime::BufferRecycler", opaque>>>)>>)>)>>>
                %4 = llvm.getelementptr %3[%1, 1] : (!llvm.ptr<struct<"class.NES::Runtime::detail::BufferControlBlock", (struct<"struct.std::atomic", (struct<"struct.std::__atomic_base", (i32)>)>, i32, i64, i64, i64, i64, ptr<struct<"class.NES::Runtime::detail::MemorySegment", (ptr<i8>, i32, ptr<struct<"class.NES::Runtime::detail::BufferControlBlock">>)>>, struct<"struct.std::atomic.0", (struct<"struct.std::__atomic_base.1", (ptr<struct<"class.NES::Runtime::BufferRecycler", opaque>>)>)>, struct<"class.std::function", (struct<"class.std::_Function_base", (struct<"union.std::_Any_data", (struct<"union.std::_Nocopy_types", (struct<(i64, i64)>)>)>, ptr<func<i1 (ptr<struct<"union.std::_Any_data", (struct<"union.std::_Nocopy_types", (struct<(i64, i64)>)>)>>, ptr<struct<"union.std::_Any_data", (struct<"union.std::_Nocopy_types", (struct<(i64, i64)>)>)>>, i32)>>)>, ptr<func<void (ptr<struct<"union.std::_Any_data", (struct<"union.std::_Nocopy_types", (struct<(i64, i64)>)>)>>, ptr<ptr<struct<"class.NES::Runtime::detail::MemorySegment", (ptr<i8>, i32, ptr<struct<"class.NES::Runtime::detail::BufferControlBlock">>)>>>, ptr<ptr<struct<"class.NES::Runtime::BufferRecycler", opaque>>>)>>)>)>>, i64) -> !llvm.ptr<i32>
                %5 = llvm.load %4 : !llvm.ptr<i32>
                %6 = llvm.zext %5 : i32 to i64
                return %6 : i64
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
    for(auto memberFunction : memberFunctions) {
        theModule.push_back(memberFunction);
    }
    // Store InsertPoint for inserting globals such as Strings or TupleBuffers.
    globalInsertPoint = new mlir::RewriterBase::InsertPoint(theModule.getBody(), theModule.begin());
};

mlir::ModuleOp MLIRGenerator::generateModuleFromNESIR(std::shared_ptr<NES::NESIR> nesIR) {
    std::unordered_map<std::string, mlir::Value> firstBlockArgs;
    generateMLIR(nesIR->getRootOperation(), firstBlockArgs);
    theModule->dump();
    if (failed(mlir::verify(theModule))) {
        theModule.emitError("module verification error");
        return nullptr;
    }
    return theModule;
}

void MLIRGenerator::generateMLIR(NES::BasicBlockPtr basicBlock, std::unordered_map<std::string, mlir::Value>& blockArgs) {
    auto terminatorOp = basicBlock->getOperations().back();
    basicBlock->popOperation();
    for (const auto& operation : basicBlock->getOperations()) {
        generateMLIR(operation, blockArgs);
    }
    // Generate Args for next block
    if (terminatorOp->getOperationType() == NES::Operation::BranchOp) { 
        auto branchOp = std::static_pointer_cast<NES::BranchOperation>(terminatorOp);
        if(basicBlock->getParentBlockLevel() <= branchOp->getNextBlock()->getParentBlockLevel()) { //Todo 
            generateMLIR(branchOp->getNextBlock(), blockArgs);
        }
    } else if (terminatorOp->getOperationType() == NES::Operation::LoopOp) {
        auto loopOp = std::static_pointer_cast<NES::LoopOperation>(terminatorOp);
        generateMLIR(loopOp, blockArgs);
    } else if (terminatorOp->getOperationType() == NES::Operation::IfOp) {
        auto ifOp = std::static_pointer_cast<NES::IfOperation>(terminatorOp);
        generateMLIR(ifOp, blockArgs);
    } else if (terminatorOp->getOperationType() == NES::Operation::ReturnOp) {
        auto returnOp = std::static_pointer_cast<NES::ReturnOperation>(terminatorOp);
        generateMLIR(returnOp, blockArgs);
    } else { // Currently necessary for LoopOps, where we remove the terminatorOp to prevent recursion.
        generateMLIR(terminatorOp, blockArgs);
    }
}

void MLIRGenerator::generateMLIR(const NES::OperationPtr& operation, std::unordered_map<std::string, mlir::Value>& blockArgs) {
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
void MLIRGenerator::generateMLIR(std::shared_ptr<NES::FunctionOperation> functionOp, std::unordered_map<std::string, mlir::Value>& blockArgs) {
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
    auto valueMapIterator = mlirFunction.args_begin();
    for (int i = 0; i < (int) functionOp->getInputArgNames().size(); ++i) {
        blockArgs.emplace(std::pair{functionOp->getInputArgNames().at(i), valueMapIterator[i]});
    }

    // Generate MLIR for operations in function body (BasicBlock)
    generateMLIR(functionOp->getFunctionBasicBlock(), blockArgs);

    theModule.push_back(mlirFunction);
}

// Recursion. No dependencies. Does NOT require addressMap insertion.
void MLIRGenerator::generateMLIR(std::shared_ptr<NES::LoopOperation> loopOp, std::unordered_map<std::string, mlir::Value>& blockArgs) {
    // -=LOOP HEAD=-
    // Loop Head BasicBlock contains CompareOp and IfOperation(thenBlock, elseBlock)
    // thenBlock=loopBodyBB & elseBlock=executeReturnBB
    auto compareOp = std::static_pointer_cast<NES::CompareOperation>(loopOp->getLoopHeadBlock()->getOperations().at(0));
    auto loopIfOp = std::static_pointer_cast<NES::IfOperation>(loopOp->getLoopHeadBlock()->getOperations().at(1));
    auto loopBodyBB = loopIfOp->getThenBranchBlock();
    auto afterLoopBB = loopIfOp->getElseBranchBlock();

    std::string loopInductionVarName = compareOp->getFirstArgName();
    inductionVars.emplace(loopInductionVarName);

    // Define index variables for loop.
    // For now we assume that loops always use the "i" as the induction var and always increase "i" by one each step.
    auto lowerBound = builder->create<arith::IndexCastOp>(getNameLoc("LowerBound"), builder->getIndexType(), blockArgs[compareOp->getFirstArgName()]);
    auto stepSize = builder->create<arith::IndexCastOp>(getNameLoc("Stepsize"), builder->getIndexType(), blockArgs["const1Op"]);

    // Get the arguments of the branchOp, which branches back into loopHeader. Compare with loopHeader args.
    // All args that are different are args which are changed inside the loop and are hence iterator args.
    std::vector<mlir::Value> iteratorArgs{}; 
    std::vector<std::string> loopTerminatorArgs;
    std::vector<std::string> yieldOpArgs;
    std::vector<std::string> changedHeaderArgs;
    auto loopTerminatorOp = findSameLevelBlock(loopBodyBB, loopOp->getLoopHeadBlock()->getParentBlockLevel());
    if(loopTerminatorOp->getOperationType() == NES::Operation::BranchOp) {
        loopTerminatorArgs = std::static_pointer_cast<NES::BranchOperation>(loopTerminatorOp)->getNextBlockArgs();
    } else {
        loopTerminatorArgs = std::static_pointer_cast<NES::IfOperation>(loopTerminatorOp)->getElseBlockArgs();
    }
    auto loopHeadBBArgs = loopOp->getLoopHeadBlock()->getInputArgs();
    for(int i = 0; i < (int) loopHeadBBArgs.size(); ++i) {
        if(loopHeadBBArgs.at(i) != loopTerminatorArgs.at(i) && loopHeadBBArgs.at(i) != loopInductionVarName) { 
            iteratorArgs.push_back(blockArgs[loopHeadBBArgs.at(i)]); 
            yieldOpArgs.push_back(loopTerminatorArgs.at(i));
        }
    }
    // Create Loop Operation. Save InsertionPoint(IP) of parent's BasicBlock
    insertComment("// For Loop Start");
    auto upperBound = builder->create<arith::IndexCastOp>(getNameLoc("UpperBound"), builder->getIndexType(), 
                                                            blockArgs[compareOp->getSecondArgName()]);

    auto forLoop = builder->create<scf::ForOp>(getNameLoc("forLoop"), lowerBound, upperBound, stepSize, iteratorArgs);
    //Replace header input args that are changed in body with dynamic iter args from ForOp.
    for(int i = 0; i < (int) changedHeaderArgs.size(); ++i) {
        blockArgs[changedHeaderArgs.at(i)] = forLoop.getRegionIterArgs().begin()[i];
    }
    auto parentBlockInsertionPoint = builder->saveInsertionPoint();

    // -=LOOP BODY=-
    // Set Insertion Point(IP) to loop BasicBlock. Process all Operations in loop BasicBlock. Restore IP to parent's BasicBlock.
    builder->setInsertionPointToStart(forLoop.getBody());
    std::unordered_map<std::string, mlir::Value> loopBodyArgs = blockArgs;
    loopBodyArgs[compareOp->getFirstArgName()] = builder->create<arith::IndexCastOp>(getNameLoc("InductionVar Cast"), 
                                              builder->getI64Type(), forLoop.getInductionVar());
    generateMLIR(loopBodyBB, loopBodyArgs);
    // Copy all values that were modified in LoopOp to blockArgs.
    int i = 0;
    for(auto blockArg = blockArgs.begin(); blockArg != blockArgs.end(); blockArg++, ++i) {
        blockArgs[blockArg->first] = loopBodyArgs[blockArg->first];
    }
    // If yieldOpArgs are given, get values from loopBodyArgs and create YieldOp at the very end of the loop body.
    if(yieldOpArgs.size() > 0) {
        std::vector<mlir::Value> forYieldOps;
        for(auto yieldOpArg : yieldOpArgs) {
            forYieldOps.push_back(loopBodyArgs[yieldOpArg]);
        }
        builder->create<scf::YieldOp>(getNameLoc("forYield"), forYieldOps);
    }
    // Leave LoopOp
    builder->restoreInsertionPoint(parentBlockInsertionPoint);
    // LoopOp might be last Op in e.g. a nested If-Else case. The loopEndBB might then be higher in scope and reached from another Op.
    if(loopOp->getLoopHeadBlock()->getParentBlockLevel() <= afterLoopBB->getParentBlockLevel()) {
        generateMLIR(afterLoopBB, blockArgs);
    }
}

// No Recursion. No dependencies. Requires addressMap insertion.
void MLIRGenerator::generateMLIR(std::shared_ptr<NES::AddressOperation> addressOp, std::unordered_map<std::string, mlir::Value>& blockArgs) {
    printf("AddressOp");
    Value recordOffset = builder->create<LLVM::MulOp>(getNameLoc("recordOffset"),
                                                      blockArgs[addressOp->getRecordIdxName()],
                                                      getConstInt("1", 64, addressOp->getRecordWidth()));
    Value fieldOffset = builder->create<LLVM::AddOp>(getNameLoc("fieldOffset"),
                                                     recordOffset,
                                                     getConstInt("1", 64, addressOp->getFieldOffset()));
    // Return I8* to first byte of field data
    Value elementAddress = builder->create<LLVM::GEPOp>(getNameLoc("fieldAccess"),
                                                        LLVM::LLVMPointerType::get(builder->getI8Type()),
                                                        blockArgs[addressOp->getAddressSourceName()],
                                                        ArrayRef<Value>({fieldOffset}));
    blockArgs.emplace(std::pair{addressOp->getIdentifier(), builder->create<LLVM::BitcastOp>(getNameLoc("Address Bitcasted"),
                                            LLVM::LLVMPointerType::get(getMLIRType(addressOp->getDataType())),
                                            elementAddress)});
}

// No recursion. Dependencies. Requires addressMap insertion.
void MLIRGenerator::generateMLIR(std::shared_ptr<NES::LoadOperation> loadOp, std::unordered_map<std::string, mlir::Value>& blockArgs) {
    printf("LoadOp.\n");
    blockArgs.emplace(std::pair{loadOp->getIdentifier(), builder->create<LLVM::LoadOp>(getNameLoc("loadedValue"), blockArgs[loadOp->getArgName()])});
}

// No Recursion. No dependencies. Requires addressMap insertion.
void MLIRGenerator::generateMLIR(std::shared_ptr<NES::ConstantIntOperation> constIntOp, std::unordered_map<std::string, mlir::Value>& blockArgs) {
    printf("ConstIntOp");
    if(!blockArgs.contains(constIntOp->getIdentifier())) {
        blockArgs.emplace(std::pair{constIntOp->getIdentifier(), getConstInt("ConstantOp", constIntOp->getNumBits(), constIntOp->getConstantIntValue())});
    } else {
        blockArgs[constIntOp->getIdentifier()] = getConstInt("ConstantOp", constIntOp->getNumBits(), constIntOp->getConstantIntValue());
    }
}

// No recursion. Dependencies. Requires addressMap insertion.
void MLIRGenerator::generateMLIR(std::shared_ptr<NES::AddIntOperation> addOp, std::unordered_map<std::string, mlir::Value>& blockArgs) {
    printf("AddOp");
    // Only generate Add, if not loop induction variable ++ operation (only loopInduction vars have length 1).
    if(!inductionVars.contains(addOp->getLeftArgName())) {
        if(!blockArgs.contains(addOp->getIdentifier())) {
            blockArgs.emplace(std::pair{addOp->getIdentifier(),
                builder->create<LLVM::AddOp>(getNameLoc("binOpResult"), blockArgs[addOp->getLeftArgName()], blockArgs[addOp->getRightArgName()])});
        } else {
            blockArgs[addOp->getIdentifier()] = builder->create<LLVM::AddOp>(getNameLoc("binOpResult"), blockArgs[addOp->getLeftArgName()], blockArgs[addOp->getRightArgName()]);
        }
    }
}

// No recursion. Dependencies. Does NOT require addressMap insertion. 
void MLIRGenerator::generateMLIR(std::shared_ptr<NES::StoreOperation> storeOp, std::unordered_map<std::string, mlir::Value>& blockArgs) {
    printf("StoreOp\n");
    builder->create<LLVM::StoreOp>(getNameLoc("outputStore"), blockArgs[storeOp->getValueArgName()], 
                                   blockArgs[storeOp->getAddressArgName()]);
}

// No recursion. Dependencies. Does NOT require addressMap insertion. 
void MLIRGenerator::generateMLIR(std::shared_ptr<NES::ReturnOperation> returnOp, std::unordered_map<std::string, mlir::Value>& blockArgs) {
    printf("ReturnOp\n");
    printf("ReturnOp blockArgs size: %d\n", (int) blockArgs.size());
    
    // Insert return into 'execute' function block. This is the FINAL return.
    builder->create<LLVM::ReturnOp>(getNameLoc("return"), getConstInt("return", 64, returnOp->getReturnOpCode()));
}

// No recursion. Dependencies. Does NOT require addressMap insertion. 
void MLIRGenerator::generateMLIR(std::shared_ptr<NES::ProxyCallOperation> proxyCallOp, std::unordered_map<std::string, mlir::Value>& blockArgs) {
    printf("ProxyCallOperation identifier: %s\n", proxyCallOp->getIdentifier().c_str());
    printf("ProxyCallOperation first BlockArg name: %s\n", blockArgs.begin()->first.c_str());

    switch(proxyCallOp->getProxyCallType()) {
        case NES::Operation::GetDataBuffer:
            blockArgs.emplace(std::pair{proxyCallOp->getIdentifier(), builder->create<mlir::CallOp>(getNameLoc("memberCall"),
                    memberFunctions[NES::Operation::GetDataBuffer], blockArgs[proxyCallOp->getInputArgNames().at(0)]).getResult(0)});
            break;
        case NES::Operation::GetNumTuples:
            blockArgs.emplace(std::pair{proxyCallOp->getIdentifier(), builder->create<mlir::CallOp>(getNameLoc("memberCall"),
                    memberFunctions[NES::Operation::GetNumTuples], blockArgs[proxyCallOp->getInputArgNames().at(0)]).getResult(0)});
            break;
        default:
            //Todo probably have create new function call map, because Functions are not representable by mlir::Value
            FlatSymbolRefAttr functionRef;
            if(theModule.lookupSymbol<LLVM::LLVMFuncOp>(proxyCallOp->getIdentifier())) {
                functionRef = SymbolRefAttr::get(context, proxyCallOp->getIdentifier());
            } else {
                functionRef = insertExternalFunction(proxyCallOp->getIdentifier(), getMLIRType(proxyCallOp->getResultType()), 
                                       getMLIRType(proxyCallOp->getInputArgTypes()), true); 
            }
            std::vector<mlir::Value> functionArgs;
            for(auto arg : proxyCallOp->getInputArgNames()) { functionArgs.push_back(blockArgs[arg]); }
            if (proxyCallOp->getResultType() != NES::Operation::VOID) {
                builder->create<LLVM::CallOp>(getNameLoc("printFunc"), getMLIRType(proxyCallOp->getResultType()), functionRef, 
                                              functionArgs);
            } else {
                builder->create<LLVM::CallOp>(getNameLoc("printFunc"), mlir::None, functionRef, 
                                              functionArgs);
            }
            break;
    }
    //Todo Insert function call. Use blockArgs is input args, if function has args.
}

// No recursion. Dependencies. Does NOT require addressMap insertion. 
void MLIRGenerator::generateMLIR(std::shared_ptr<NES::CompareOperation> compareOp, std::unordered_map<std::string, mlir::Value>& blockArgs) {
    printf("CompareOperation identifier: %s\n", compareOp->getIdentifier().c_str());
    printf("CompareOperation first BlockArg name: %s\n", blockArgs.begin()->first.c_str());
    blockArgs.emplace(std::pair{compareOp->getIdentifier(), builder->create<arith::CmpIOp>(getNameLoc("comparison"),
                                               convertToMLIRComparison(compareOp->getComparator()),
                                               blockArgs[compareOp->getFirstArgName()],
                                               blockArgs[compareOp->getSecondArgName()])});
}

// No recursion. Dependencies. Does NOT require addressMap insertion. 
void MLIRGenerator::generateMLIR(std::shared_ptr<NES::IfOperation> ifOp, std::unordered_map<std::string, mlir::Value>& blockArgs) {
    auto ifOpTerminator = findSameLevelBlock(ifOp->getThenBranchBlock(), ifOp->getThenBranchBlock()->getParentBlockLevel()-1);
    NES::BasicBlockPtr afterBlock;
    std::vector<std::string> ifTerminatorArgs;
    // std::vector<std::string> elseTerminatorArgs; //Todo need elseTerminatorArgs to support correct NESIR BB approach.s
    // We can use the afterBlock args later, since they must match the terminatorOps args (no loop).
    if(ifOpTerminator->getOperationType() == NES::Operation::BranchOp) {
        auto branchOp = std::static_pointer_cast<NES::BranchOperation>(ifOpTerminator);
        ifTerminatorArgs = branchOp->getNextBlockArgs();
        afterBlock = branchOp->getNextBlock();
    } else {
        auto nestedIfOp = std::static_pointer_cast<NES::IfOperation>(ifOpTerminator);
        ifTerminatorArgs = nestedIfOp->getThenBlockArgs();
        afterBlock = std::static_pointer_cast<NES::IfOperation>(ifOpTerminator)->getElseBranchBlock();
    }
    // Create IF Operation
    mlir::scf::IfOp mlirIfOp;
    auto currentInsertionPoint = builder->saveInsertionPoint();
    std::vector<mlir::Type> mlirIfOpResultTypes;
    // Register all blockArgs for the IfOperation as output/yield args. All values that are not modified, will just be returned.
    for(auto afterIfBlockArg : afterBlock->getInputArgs()) {
        mlirIfOpResultTypes.push_back(blockArgs[afterIfBlockArg].getType());
    }
    mlirIfOp = builder->create<scf::IfOp>(getNameLoc("ifOperation"), mlirIfOpResultTypes, blockArgs[ifOp->getBoolArgName()], true);
    
    // IF case -> Change scope to Then Branch. Finish by setting obligatory YieldOp for THEN case.
    builder->setInsertionPointToStart(mlirIfOp.getThenBodyBuilder().getInsertionBlock());
    std::unordered_map<std::string, mlir::Value> ifBlockArgs = blockArgs;
    generateMLIR(ifOp->getThenBranchBlock(), ifBlockArgs);
    std::vector<mlir::Value> ifYieldOps;
    for(auto afterIfBlockArg : ifTerminatorArgs) {
        ifYieldOps.push_back(ifBlockArgs[afterIfBlockArg]);
    }
    builder->create<scf::YieldOp>(getNameLoc("ifYield"), ifYieldOps);

    // ELSE case -> Change scope to ELSE Branch. Finish by setting obligatory YieldOp for ELSE case.
    builder->setInsertionPointToStart(mlirIfOp.getElseBodyBuilder().getInsertionBlock());
    std::unordered_map<std::string, mlir::Value> elseBlockArgs = blockArgs;
    if(ifOp->getElseBranchBlock() != nullptr) {
        generateMLIR(ifOp->getElseBranchBlock(), elseBlockArgs); 
    }
    std::vector<mlir::Value> elseYieldOps;
    for(auto afterIfBlockArg : afterBlock->getInputArgs()) { //Todo MUST use elseOpTerminator outArgs
        elseYieldOps.push_back(elseBlockArgs[afterIfBlockArg]);
    }
    builder->create<scf::YieldOp>(getNameLoc("elseYield"), elseYieldOps);

    // Back to IfOp scope. Get all result values from IfOperation, write to blockArgs and proceed.
    builder->restoreInsertionPoint(currentInsertionPoint);
    for(int i = 0; i < (int) afterBlock->getInputArgs().size(); ++i) {
        blockArgs[afterBlock->getInputArgs().at(i)] = mlirIfOp.getResult(i);
    }
    if(afterBlock->getParentBlockLevel() == (ifOp->getThenBranchBlock()->getParentBlockLevel())-1) {
        generateMLIR(afterBlock, blockArgs);
    }
}

//==--------------------- Dummy OPERATIONS ----------------------==//
// No recursion. Dependencies. Does NOT require addressMap insertion. 
void MLIRGenerator::generateMLIR(std::shared_ptr<NES::BranchOperation> branchOp, std::unordered_map<std::string, mlir::Value>& blockArgs) {
    printf("BranchOperation.getNextBlock.getIdentifier: %s\n", branchOp->getNextBlock()->getIdentifier().c_str());
    printf("BranchOperation first BlockArg name: %s\n", blockArgs.begin()->first.c_str());
}

std::vector<std::string> MLIRGenerator::getJitProxyFunctionSymbols() { return jitProxyFunctionSymbols; }
std::vector<llvm::JITTargetAddress> MLIRGenerator::getJitProxyTargetAddresses() { return jitProxyFunctionTargetAddresses; }