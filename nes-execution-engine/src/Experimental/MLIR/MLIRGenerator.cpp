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

#include <Experimental/MLIR/MLIRGenerator.hpp>
#include <Experimental/MLIR/YieldOperation.hpp>
#include <Experimental/NESIR/BasicBlocks/BasicBlock.hpp>
#include <Experimental/NESIR/Operations/ArithmeticOperations/AddOperation.hpp>
#include <Experimental/NESIR/Operations/ArithmeticOperations/DivOperation.hpp>
#include <Experimental/NESIR/Operations/BranchOperation.hpp>
#include <Experimental/NESIR/Operations/ConstFloatOperation.hpp>
#include <Experimental/NESIR/Operations/IfOperation.hpp>
#include <Experimental/NESIR/Operations/LogicalOperations/AndOperation.hpp>
#include <Experimental/NESIR/Operations/LogicalOperations/CompareOperation.hpp>
#include <Experimental/NESIR/Operations/LogicalOperations/OrOperation.hpp>
#include <Experimental/NESIR/Operations/Loop/LoopOperation.hpp>
#include <Experimental/NESIR/Operations/Operation.hpp>
#include <Experimental/NESIR/Operations/ReturnOperation.hpp>
#include <Util/Logger/Logger.hpp>
#include <cstdint>
#include <exception>
#include <llvm/ADT/StringRef.h>
#include <llvm/Transforms/Utils/Cloning.h>
#include <memory>
#include <mlir/Dialect/Arithmetic/IR/Arithmetic.h>
#include <mlir/Dialect/LLVMIR/LLVMDialect.h>
#include <mlir/Dialect/LLVMIR/LLVMTypes.h>
#include <mlir/Dialect/SCF/SCF.h>
#include <mlir/IR/Attributes.h>
#include <mlir/IR/Builders.h>
#include <mlir/IR/BuiltinAttributes.h>
#include <mlir/IR/BuiltinTypes.h>
#include <mlir/IR/Location.h>
#include <mlir/IR/MLIRContext.h>
#include <mlir/IR/Operation.h>
#include <mlir/IR/PatternMatch.h>
#include <mlir/IR/TypeRange.h>
#include <mlir/IR/Value.h>
#include <mlir/IR/Verifier.h>
#include <mlir/Parser.h>
#include <mlir/Support/LLVM.h>
#include <sys/types.h>
#include <unordered_map>
#include <vector>

namespace NES::ExecutionEngine::Experimental::MLIR {
//==-----------------------==//
//==-- UTILITY FUNCTIONS --==//
//==-----------------------==//
mlir::Type MLIRGenerator::getMLIRType(IR::Operations::PrimitiveStamp type) {
    switch (type) {
        case IR::Operations::INT1:
        case IR::Operations::BOOLEAN: return builder->getIntegerType(1);
        case IR::Operations::INT8:
        case IR::Operations::UINT8:
        case IR::Operations::CHAR: return builder->getIntegerType(8);
        case IR::Operations::UINT16:
        case IR::Operations::INT16: return builder->getIntegerType(16);
        case IR::Operations::UINT32:
        case IR::Operations::INT32: return builder->getIntegerType(32);
        case IR::Operations::UINT64:
        case IR::Operations::INT64: return builder->getIntegerType(64);
        case IR::Operations::FLOAT: return mlir::Float32Type::get(context);
        case IR::Operations::DOUBLE: return mlir::Float64Type::get(context);
        case IR::Operations::INT8PTR: return mlir::LLVM::LLVMPointerType::get(builder->getI8Type());
        case IR::Operations::VOID: return mlir::LLVM::LLVMVoidType::get(context);

        default: return builder->getIntegerType(32);
    }
}

std::vector<mlir::Type> MLIRGenerator::getMLIRType(std::vector<IR::Operations::OperationPtr> types) {
    std::vector<mlir::Type> resultTypes;
    for (auto type : types) {
        resultTypes.push_back(getMLIRType(type->getStamp()));
    }
    return resultTypes;
}

mlir::Value MLIRGenerator::getConstInt(const std::string& location, int numBits, int64_t value) {
    return builder->create<mlir::LLVM::ConstantOp>(getNameLoc(location),
                                                   builder->getIntegerType(numBits),
                                                   builder->getIntegerAttr(builder->getIndexType(), value));
}

void MLIRGenerator::insertComment(const std::string& comment) {
    mlir::ValueRange emptyRange{};
    builder->create<mlir::LLVM::UndefOp>(getNameLoc(comment),
                                         mlir::LLVM::LLVMVoidType::get(context),
                                         emptyRange,
                                         builder->getNamedAttr(comment, builder->getZeroAttr(builder->getI32Type())));
}

mlir::Location MLIRGenerator::getNameLoc(const std::string& name) {
    auto baseLocation = mlir::FileLineColLoc::get(builder->getStringAttr("Query_1"), 0, 0);
    return mlir::NameLoc::get(builder->getStringAttr(name), baseLocation);
}

mlir::arith::CmpIPredicate convertToIntMLIRComparison(IR::Operations::CompareOperation::Comparator comparisonType) {
    //TODO extend to all comparison types
    switch (comparisonType) {
        case (IR::Operations::CompareOperation::Comparator::IEQ): return mlir::arith::CmpIPredicate::eq;
        case (IR::Operations::CompareOperation::Comparator::INE): return mlir::arith::CmpIPredicate::ne;
        case (IR::Operations::CompareOperation::Comparator::ISLT): return mlir::arith::CmpIPredicate::slt;
        case (IR::Operations::CompareOperation::Comparator::ISLE): return mlir::arith::CmpIPredicate::sle;
        case (IR::Operations::CompareOperation::Comparator::ISGT): return mlir::arith::CmpIPredicate::sgt;
        case (IR::Operations::CompareOperation::Comparator::ISGE): return mlir::arith::CmpIPredicate::sge;
        //Unsigned Comparisons. Necessary, otherwise: slt(unsigned(8), 3) = true.
        case (IR::Operations::CompareOperation::Comparator::IULT): return mlir::arith::CmpIPredicate::ult;
        case (IR::Operations::CompareOperation::Comparator::IULE): return mlir::arith::CmpIPredicate::ule;
        case (IR::Operations::CompareOperation::Comparator::IUGT): return mlir::arith::CmpIPredicate::ugt;
        case (IR::Operations::CompareOperation::Comparator::IUGE): return mlir::arith::CmpIPredicate::uge;
        default: return mlir::arith::CmpIPredicate::slt;
    }
}
mlir::arith::CmpFPredicate convertToFloatMLIRComparison(IR::Operations::CompareOperation::Comparator comparisonType) {
    switch (comparisonType) {
        // the U in U(LT/LE/..) stands for unordered, not unsigned! Float always comparisons are always signed.
        case (IR::Operations::CompareOperation::Comparator::FOLT): return mlir::arith::CmpFPredicate::OLT;
        case (IR::Operations::CompareOperation::Comparator::FOLE): return mlir::arith::CmpFPredicate::OLE;
        case (IR::Operations::CompareOperation::Comparator::FOEQ): return mlir::arith::CmpFPredicate::OEQ;
        case (IR::Operations::CompareOperation::Comparator::FOGT): return mlir::arith::CmpFPredicate::OGT;
        case (IR::Operations::CompareOperation::Comparator::FOGE): return mlir::arith::CmpFPredicate::OGE;
        case (IR::Operations::CompareOperation::Comparator::FONE): return mlir::arith::CmpFPredicate::ONE;
        default: return mlir::arith::CmpFPredicate::OLT;
    }
}

// Todo: Problem: loopIfOp->getElseBranchBlock()->getParentBlockLevel would result in correct Op, but is not triggered
// -> we skip correct BranchOp
IR::Operations::OperationPtr MLIRGenerator::findLastTerminatorOp(IR::BasicBlockPtr thenBlock, int ifParentBlockLevel) {
    auto terminatorOp = thenBlock->getOperations().back();
    // Generate Args for next block
    if (terminatorOp->getOperationType() == IR::Operations::Operation::BranchOp) {
        auto branchOp = std::static_pointer_cast<IR::Operations::BranchOperation>(terminatorOp);
        if (branchOp->getNextBlockInvocation().getBlock()->getScopeLevel() <= ifParentBlockLevel) {
            return branchOp;
        } else {
            return findLastTerminatorOp(branchOp->getNextBlockInvocation().getBlock(), ifParentBlockLevel);
        }
    } else if (terminatorOp->getOperationType() == IR::Operations::Operation::LoopOp) {
        auto loopOp = std::static_pointer_cast<IR::Operations::LoopOperation>(terminatorOp);
        auto loopIfOp =
            std::static_pointer_cast<IR::Operations::IfOperation>(loopOp->getLoopHeadBlock().getBlock()->getOperations().back());
        if (loopIfOp->getFalseBlockInvocation().getBlock()->getScopeLevel() <= ifParentBlockLevel) {
            return loopIfOp;
        } else {
            return findLastTerminatorOp(loopIfOp->getFalseBlockInvocation().getBlock(), ifParentBlockLevel);
        }
    } else {
        auto ifOp = std::static_pointer_cast<IR::Operations::IfOperation>(terminatorOp);
        if (ifOp->getFalseBlockInvocation().getBlock() != nullptr) {
            return findLastTerminatorOp(ifOp->getFalseBlockInvocation().getBlock(), ifParentBlockLevel);
        } else {
            return findLastTerminatorOp(ifOp->getTrueBlockInvocation().getBlock(), ifParentBlockLevel);
        }
    }
}

//==-----------------------------------==//
//==-- Create & Insert Functionality --==//
//==-----------------------------------==//
mlir::FlatSymbolRefAttr MLIRGenerator::insertExternalFunction(const std::string& name,
                                                              void* functionPtr,
                                                              mlir::Type resultType,
                                                              std::vector<mlir::Type> argTypes,
                                                              bool varArgs) {
    // Create function arg & result types (currently only int for result).
    mlir::LLVM::LLVMFunctionType llvmFnType = mlir::LLVM::LLVMFunctionType::get(resultType, argTypes, varArgs);
    // llvmFnType = LLVM::LLVMFunctionType::get(LLVM::LLVMVoidType::get(context), argTypes, varArgs);

    // The InsertionGuard saves the current IP and restores it after scope is left.
    mlir::PatternRewriter::InsertionGuard insertGuard(*builder);
    builder->restoreInsertionPoint(*globalInsertPoint);
    // Create function in global scope. Return reference.
    builder->create<mlir::LLVM::LLVMFuncOp>(theModule.getLoc(), name, llvmFnType, mlir::LLVM::Linkage::External, false);

    jitProxyFunctionSymbols.push_back(name);
    if (functionPtr == nullptr) {
        functionPtr = ProxyFunctions.getProxyFunctionAddress(name);
    }
    jitProxyFunctionTargetAddresses.push_back(llvm::pointerToJITTargetAddress(functionPtr));
    return mlir::SymbolRefAttr::get(context, name);
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
            // llvm.func @__gxx_personality_v0(...) -> i32
            func private @setNumTuples(%arg0: !llvm.ptr<i8>, %arg1: i64) {// attributes {personality = @__gxx_personality_v0} {
                %0 = llvm.mlir.constant(1 : i32) : i32
                %1 = llvm.mlir.constant(0 : i64) : i64
                %2 = llvm.bitcast %arg0 : !llvm.ptr<i8> to !llvm.ptr<ptr<struct<"class.NES::Runtime::detail::BufferControlBlock", (struct<"struct.std::atomic", (struct<"struct.std::__atomic_base", (i32)>)>, i32, i64, i64, i64, i64, ptr<struct<"class.NES::Runtime::detail::MemorySegment", (ptr<i8>, i32, ptr<struct<"class.NES::Runtime::detail::BufferControlBlock">>)>>, struct<"struct.std::atomic.0", (struct<"struct.std::__atomic_base.1", (ptr<struct<"class.NES::Runtime::BufferRecycler", opaque>>)>)>, struct<"class.std::function", (struct<"class.std::_Function_base", (struct<"union.std::_Any_data", (struct<"union.std::_Nocopy_types", (struct<(i64, i64)>)>)>, ptr<func<i1 (ptr<struct<"union.std::_Any_data", (struct<"union.std::_Nocopy_types", (struct<(i64, i64)>)>)>>, ptr<struct<"union.std::_Any_data", (struct<"union.std::_Nocopy_types", (struct<(i64, i64)>)>)>>, i32)>>)>, ptr<func<void (ptr<struct<"union.std::_Any_data", (struct<"union.std::_Nocopy_types", (struct<(i64, i64)>)>)>>, ptr<ptr<struct<"class.NES::Runtime::detail::MemorySegment", (ptr<i8>, i32, ptr<struct<"class.NES::Runtime::detail::BufferControlBlock">>)>>>, ptr<ptr<struct<"class.NES::Runtime::BufferRecycler", opaque>>>)>>)>)>>>
                %3 = llvm.load %2 : !llvm.ptr<ptr<struct<"class.NES::Runtime::detail::BufferControlBlock", (struct<"struct.std::atomic", (struct<"struct.std::__atomic_base", (i32)>)>, i32, i64, i64, i64, i64, ptr<struct<"class.NES::Runtime::detail::MemorySegment", (ptr<i8>, i32, ptr<struct<"class.NES::Runtime::detail::BufferControlBlock">>)>>, struct<"struct.std::atomic.0", (struct<"struct.std::__atomic_base.1", (ptr<struct<"class.NES::Runtime::BufferRecycler", opaque>>)>)>, struct<"class.std::function", (struct<"class.std::_Function_base", (struct<"union.std::_Any_data", (struct<"union.std::_Nocopy_types", (struct<(i64, i64)>)>)>, ptr<func<i1 (ptr<struct<"union.std::_Any_data", (struct<"union.std::_Nocopy_types", (struct<(i64, i64)>)>)>>, ptr<struct<"union.std::_Any_data", (struct<"union.std::_Nocopy_types", (struct<(i64, i64)>)>)>>, i32)>>)>, ptr<func<void (ptr<struct<"union.std::_Any_data", (struct<"union.std::_Nocopy_types", (struct<(i64, i64)>)>)>>, ptr<ptr<struct<"class.NES::Runtime::detail::MemorySegment", (ptr<i8>, i32, ptr<struct<"class.NES::Runtime::detail::BufferControlBlock">>)>>>, ptr<ptr<struct<"class.NES::Runtime::BufferRecycler", opaque>>>)>>)>)>>>
                %4 = llvm.trunc %arg1 : i64 to i32
                %5 = llvm.getelementptr %3[%1, 1] : (!llvm.ptr<struct<"class.NES::Runtime::detail::BufferControlBlock", (struct<"struct.std::atomic", (struct<"struct.std::__atomic_base", (i32)>)>, i32, i64, i64, i64, i64, ptr<struct<"class.NES::Runtime::detail::MemorySegment", (ptr<i8>, i32, ptr<struct<"class.NES::Runtime::detail::BufferControlBlock">>)>>, struct<"struct.std::atomic.0", (struct<"struct.std::__atomic_base.1", (ptr<struct<"class.NES::Runtime::BufferRecycler", opaque>>)>)>, struct<"class.std::function", (struct<"class.std::_Function_base", (struct<"union.std::_Any_data", (struct<"union.std::_Nocopy_types", (struct<(i64, i64)>)>)>, ptr<func<i1 (ptr<struct<"union.std::_Any_data", (struct<"union.std::_Nocopy_types", (struct<(i64, i64)>)>)>>, ptr<struct<"union.std::_Any_data", (struct<"union.std::_Nocopy_types", (struct<(i64, i64)>)>)>>, i32)>>)>, ptr<func<void (ptr<struct<"union.std::_Any_data", (struct<"union.std::_Nocopy_types", (struct<(i64, i64)>)>)>>, ptr<ptr<struct<"class.NES::Runtime::detail::MemorySegment", (ptr<i8>, i32, ptr<struct<"class.NES::Runtime::detail::BufferControlBlock">>)>>>, ptr<ptr<struct<"class.NES::Runtime::BufferRecycler", opaque>>>)>>)>)>>, i64) -> !llvm.ptr<i32>
                llvm.store %4, %5 : !llvm.ptr<i32>
                llvm.return
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

    for (; proxyOpIterator != simpleModule->getOps().begin()->getBlock()->getOperations().end(); proxyOpIterator++) {
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
    builder = std::make_shared<mlir::OpBuilder>(&context);
    this->theModule = mlir::ModuleOp::create(getNameLoc("module"));
    // Insert all needed MemberClassFunctions into the MLIR module.
    for (auto memberFunction : memberFunctions) {
        theModule.push_back(memberFunction);
    }
    // Store InsertPoint for inserting globals such as Strings or TupleBuffers.
    globalInsertPoint = new mlir::RewriterBase::InsertPoint(theModule.getBody(), theModule.begin());
};

mlir::ModuleOp MLIRGenerator::generateModuleFromNESIR(std::shared_ptr<IR::NESIR> nesIR) {
    ValueFrame firstframe;
    generateMLIR(nesIR->getRootOperation(), firstframe);
    theModule->dump();
    if (failed(mlir::verify(theModule))) {
        theModule.emitError("module verification error");
        return nullptr;
    }
    return theModule;
}

void MLIRGenerator::generateMLIR(IR::BasicBlockPtr basicBlock, ValueFrame& frame) {
    // extract the last (terminator) operation of the block and generate operations for all remaining operations
    //auto terminatorOp = basicBlock->getOperations().back();
    //basicBlock->popOperation();
    for (const auto& operation : basicBlock->getOperations()) {
        generateMLIR(operation, frame);
    }

    /*
    // Generate Args for next block
    if (terminatorOp->getOperationType() == IR::Operations::Operation::BranchOp) {
        auto branchOp = std::static_pointer_cast<IR::Operations::BranchOperation>(terminatorOp);
        if (basicBlock->getScopeLevel() <= branchOp->getNextBlockInvocation().getBlock()->getScopeLevel()) {
            generateMLIR(branchOp->getNextBlockInvocation().getBlock(), frame);
        } else {
            auto blockArguments = branchOp->getNextBlockInvocation().getArguments();
            for (int i = 0; i < (int) blockArguments.size(); ++i) {
                // If higher level block arg names are different, add values with new names and delete old entries.
                auto nextBlockArg = blockArguments.at(i);
                if (!(nextBlockArg->getIdentifier() == blockArguments.at(i)->getIdentifier())
                    && !inductionVars.contains(nextBlockArg->getIdentifier())) {
                    // TODO what to do here?
                    // auto nameChangeNode = frame.extract(blockArguments.at(i)->getIdentifier());
                    // nameChangeNode.key() = nextBlockArg->getIdentifier();
                    // frame.insert(std::move(nameChangeNode));
                }
            }
        }
    } else if (terminatorOp->getOperationType() == IR::Operations::Operation::LoopOp) {
        auto loopOp = std::static_pointer_cast<IR::Operations::LoopOperation>(terminatorOp);
        for (int i = 0; i < (int) loopOp->getLoopBlockArgs().size(); ++i) {
            // If higher level block arg names are different, add values with new names and delete old entries.
            auto nextBlockArg = loopOp->getLoopHeadBlock()->getArguments().at(i);
            if (!(nextBlockArg->getIdentifier() == loopOp->getLoopBlockArgs().at(i))
                && !inductionVars.contains(nextBlockArg->getIdentifier())) {
                // TODO what to do here?
                //auto nameChangeNode = frame.extract(loopOp->getLoopBlockArgs().at(i));
                //nameChangeNode.key() = nextBlockArg->getIdentifier();
                //frame.insert(std::move(nameChangeNode));
            }
        }
        generateMLIR(loopOp, frame);
    } else if (terminatorOp->getOperationType() == IR::Operations::Operation::IfOp) {
        auto ifOp = std::static_pointer_cast<IR::Operations::IfOperation>(terminatorOp);
        generateMLIR(ifOp, frame);
    } else if (terminatorOp->getOperationType() == IR::Operations::Operation::ReturnOp) {
        auto returnOp = std::static_pointer_cast<IR::Operations::ReturnOperation>(terminatorOp);
        generateMLIR(returnOp, frame);
    } else {// Currently necessary for LoopOps, where we remove the terminatorOp to prevent recursion.
        generateMLIR(terminatorOp, frame);
    }
     */
}

void MLIRGenerator::generateMLIR(const IR::Operations::OperationPtr& operation, ValueFrame& frame) {
    switch (operation->getOperationType()) {
        case IR::Operations::Operation::OperationType::FunctionOp:
            generateMLIR(std::static_pointer_cast<IR::Operations::FunctionOperation>(operation), frame);
            break;
        case IR::Operations::Operation::OperationType::LoopOp:
            generateMLIR(std::static_pointer_cast<IR::Operations::LoopOperation>(operation), frame);
            break;
        case IR::Operations::Operation::OperationType::ConstIntOp:
            generateMLIR(std::static_pointer_cast<IR::Operations::ConstIntOperation>(operation), frame);
            break;
        case IR::Operations::Operation::OperationType::ConstFloatOp:
            generateMLIR(std::static_pointer_cast<IR::Operations::ConstFloatOperation>(operation), frame);
            break;
        case IR::Operations::Operation::OperationType::AddOp:
            generateMLIR(std::static_pointer_cast<IR::Operations::AddOperation>(operation), frame);
            break;
        case IR::Operations::Operation::OperationType::SubOp:
            generateMLIR(std::static_pointer_cast<IR::Operations::SubOperation>(operation), frame);
            break;
        case IR::Operations::Operation::OperationType::MulOp:
            generateMLIR(std::static_pointer_cast<IR::Operations::MulOperation>(operation), frame);
            break;
        case IR::Operations::Operation::OperationType::DivOp:
            generateMLIR(std::static_pointer_cast<IR::Operations::DivOperation>(operation), frame);
            break;
        case IR::Operations::Operation::OperationType::LoadOp:
            generateMLIR(std::static_pointer_cast<IR::Operations::LoadOperation>(operation), frame);
            break;
        case IR::Operations::Operation::OperationType::AddressOp:
            generateMLIR(std::static_pointer_cast<IR::Operations::AddressOperation>(operation), frame);
            break;
        case IR::Operations::Operation::OperationType::StoreOp:
            generateMLIR(std::static_pointer_cast<IR::Operations::StoreOperation>(operation), frame);
            break;
        case IR::Operations::Operation::OperationType::BranchOp:
            generateMLIR(std::static_pointer_cast<IR::Operations::BranchOperation>(operation), frame);
            break;
        case IR::Operations::Operation::OperationType::IfOp:
            generateMLIR(std::static_pointer_cast<IR::Operations::IfOperation>(operation), frame);
            break;
        case IR::Operations::Operation::OperationType::CompareOp:
            generateMLIR(std::static_pointer_cast<IR::Operations::CompareOperation>(operation), frame);
            break;
        case IR::Operations::Operation::OperationType::ProxyCallOp:
            generateMLIR(std::static_pointer_cast<IR::Operations::ProxyCallOperation>(operation), frame);
            break;
        case IR::Operations::Operation::OperationType::ReturnOp:
            generateMLIR(std::static_pointer_cast<IR::Operations::ReturnOperation>(operation), frame);
            break;
        case IR::Operations::Operation::OperationType::MLIR_YIELD:
            generateMLIR(std::static_pointer_cast<IR::Operations::YieldOperation>(operation), frame);
            break;
        case IR::Operations::Operation::OperationType::OrOp:
            generateMLIR(std::static_pointer_cast<IR::Operations::OrOperation>(operation), frame);
            break;
        case IR::Operations::Operation::OperationType::AndOp:
            generateMLIR(std::static_pointer_cast<IR::Operations::AndOperation>(operation), frame);
            break;
        case IR::Operations::Operation::OperationType::NegateOp:
            generateMLIR(std::static_pointer_cast<IR::Operations::NegateOperation>(operation), frame);
            break;
        default: NES_NOT_IMPLEMENTED();
    }
}

void MLIRGenerator::generateMLIR(std::shared_ptr<IR::Operations::NegateOperation> negateOperation, ValueFrame& frame) {
    auto input = frame.getValue(negateOperation->getInput()->getIdentifier());
    auto negate = builder->create<mlir::arith::CmpIOp>(getNameLoc("comparison"),
                                                       mlir::arith::CmpIPredicate::eq,
                                                       input,
                                                       this->getConstInt("const", 1, 0));
    frame.setValue(negateOperation->getIdentifier(), negate);
}

void MLIRGenerator::generateMLIR(std::shared_ptr<IR::Operations::OrOperation> orOperation, ValueFrame& frame) {
    auto leftInput = frame.getValue(orOperation->getLeftInput()->getIdentifier());
    auto rightInput = frame.getValue(orOperation->getRightInput()->getIdentifier());
    auto mlirOrOp = builder->create<mlir::LLVM::OrOp>(getNameLoc("binOpResult"), leftInput, rightInput);
    frame.setValue(orOperation->getIdentifier(), mlirOrOp);
}

void MLIRGenerator::generateMLIR(std::shared_ptr<IR::Operations::AndOperation> andOperation, ValueFrame& frame) {
    auto leftInput = frame.getValue(andOperation->getLeftInput()->getIdentifier());
    auto rightInput = frame.getValue(andOperation->getRightInput()->getIdentifier());
    auto mlirAndOp = builder->create<mlir::LLVM::AndOp>(getNameLoc("binOpResult"), leftInput, rightInput);
    frame.setValue(andOperation->getIdentifier(), mlirAndOp);
}

// Recursion. No dependencies. Does NOT require addressMap insertion.
void MLIRGenerator::generateMLIR(std::shared_ptr<IR::Operations::FunctionOperation> functionOp, ValueFrame& frame) {
    // Generate execute function. Set input/output types and get its entry block.
    llvm::SmallVector<mlir::Type, 4> inputTypes(0);
    for (auto inputArg : functionOp->getFunctionBasicBlock()->getArguments()) {
        inputTypes.emplace_back(getMLIRType(inputArg->getStamp()));
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
    for (int i = 0; i < (int) functionOp->getFunctionBasicBlock()->getArguments().size(); ++i) {
        frame.setValue(functionOp->getFunctionBasicBlock()->getArguments().at(i)->getIdentifier(), valueMapIterator[i]);
    }

    // Generate MLIR for operations in function body (BasicBlock)
    generateMLIR(functionOp->getFunctionBasicBlock(), frame);

    theModule.push_back(mlirFunction);
}

void MLIRGenerator::generateSCFCountedLoop(std::shared_ptr<IR::Operations::LoopOperation> loopOp, ValueFrame& frame) {
    auto parentBlockInsertionPoint = builder->saveInsertionPoint();
    auto loopInfo = std::dynamic_pointer_cast<IR::Operations::CountedLoopInfo>(loopOp->getLoopInfo());
    auto mlirLowerBound = builder->create<mlir::arith::IndexCastOp>(getNameLoc("lowerBound"),
                                                                    builder->getIndexType(),
                                                                    frame.getValue(loopInfo->lowerBound.lock()->getIdentifier()));

    auto mlirUpperBound = builder->create<mlir::arith::IndexCastOp>(getNameLoc("upperBound"),
                                                                    builder->getIndexType(),
                                                                    frame.getValue(loopInfo->upperBound.lock()->getIdentifier()));
    auto mlirStepSize = builder->create<mlir::arith::IndexCastOp>(getNameLoc("stepSize"),
                                                                  builder->getIndexType(),
                                                                  frame.getValue(loopInfo->stepSize.lock()->getIdentifier()));
    std::vector<mlir::Value> iteratorArgs{};
    for (auto argument : loopInfo->loopInitialIteratorArguments) {
        iteratorArgs.emplace_back(frame.getValue(argument->getIdentifier()));
    }
    auto mlirForLoop =
        builder->create<mlir::scf::ForOp>(getNameLoc("forLoop"), mlirLowerBound, mlirUpperBound, mlirStepSize, iteratorArgs);
    builder->setInsertionPointToStart(mlirForLoop.getBody());
    // generate loop body
    {
        auto loopBodyBlock = loopInfo->loopBodyBlock;
        auto branchOp = std::static_pointer_cast<IR::Operations::BranchOperation>(loopBodyBlock->getTerminatorOp());
        loopBodyBlock->popOperation();
        loopBodyBlock->addOperation(
            std::make_shared<IR::Operations::YieldOperation>(branchOp->getNextBlockInvocation().getArguments(), nullptr));
        auto loopBodyArguments = loopBodyBlock->getArguments();
        ValueFrame loopBodyFrame;
        for (uint64_t i = 0; i < loopInfo->loopBodyIteratorArguments.size(); i++) {
            loopBodyFrame.setValue(loopInfo->loopBodyIteratorArguments[i]->getIdentifier(), mlirForLoop.getRegionIterArgs()[i]);
        }
        generateMLIR(loopBodyBlock, loopBodyFrame);
    }
    builder->restoreInsertionPoint(parentBlockInsertionPoint);
    // generate loop merge block
    {
        auto loopEndBlock = loopInfo->loopEndBlock;
        auto loopEndArguments = loopEndBlock->getArguments();
        ValueFrame loopBodyFrame;
        for (uint64_t i = 0; i < loopEndArguments.size(); i++) {
            loopBodyFrame.setValue(loopEndArguments[i]->getIdentifier(), mlirForLoop.getResult(i));
        }
        generateMLIR(loopEndBlock, loopBodyFrame);
    }
}

void MLIRGenerator::generateCFDefaultLoop(std::shared_ptr<IR::Operations::LoopOperation> loopOp, ValueFrame& frame) {
    this->useSCF = false;
    std::vector<mlir::Value> mlirTargetBlockArguments;
    auto loopHeadBlock = loopOp->getLoopHeadBlock();
    for (auto targetBlockArgument : loopHeadBlock.getArguments()) {
        mlirTargetBlockArguments.push_back(frame.getValue(targetBlockArgument->getIdentifier()));
    }
    auto* mlirTargetBlock = generateBasicBlock(loopHeadBlock, frame);
    builder->create<mlir::BranchOp>(getNameLoc("branch"), mlirTargetBlock, mlirTargetBlockArguments);
}

// Recursion. No dependencies. Does NOT require addressMap insertion.
void MLIRGenerator::generateMLIR(std::shared_ptr<IR::Operations::LoopOperation> loopOp, ValueFrame& frame) {
    if (loopOp->getLoopInfo()->isCountedLoop()) {
        generateSCFCountedLoop(loopOp, frame);
        return;
    } else {
        generateCFDefaultLoop(loopOp, frame);
        return;
    }
    /*
    // -=LOOP HEAD=-
    // Loop Head BasicBlock contains CompareOp and IfOperation(thenBlock, elseBlock)
    // thenBlock=loopBodyBB & elseBlock=executeReturnBB
    auto loopHead = loopOp->getLoopHeadBlock();
    // the loop head should always terminate with an if operation
    NES_ASSERT(loopHead->getTerminatorOp()->getOperationType() == IR::Operations::Operation::IfOp,
               "loop head should always terminate with an if operation");
    auto loopIfOp = std::static_pointer_cast<IR::Operations::IfOperation>(loopHead->getOperations().back());
    auto compareOp = std::static_pointer_cast<IR::Operations::CompareOperation>(loopIfOp->getValue());
    auto loopBodyBB = loopIfOp->getTrueBlockInvocation().getBlock();
    auto afterLoopBB = loopIfOp->getFalseBlockInvocation().getBlock();

    std::string loopInductionVarName = compareOp->getLeftInput()->getIdentifier();
    inductionVars.emplace(loopInductionVarName);

    // Define index variables for loop.
    // For now we assume that loops always use the "i" as the induction var and always increase "i" by one each step.
    auto lowerBound = builder->create<mlir::arith::IndexCastOp>(getNameLoc("LowerBound"),
                                                                builder->getIndexType(),
                                                                frame.getValue(compareOp->getLeftInput()->getIdentifier()));
    auto stepSize =
        builder->create<mlir::arith::IndexCastOp>(getNameLoc("Stepsize"), builder->getIndexType(), frame.getValue("const1Op"));

    // Get the arguments of the branchOp, which branches back into loopHeader. Compare with loopHeader args.
    // All args that are different are args which are changed inside the loop and are hence iterator args.
    std::vector<mlir::Value> iteratorArgs{};
    std::vector<IR::Operations::OperationPtr> loopTerminatorArgs;
    std::vector<std::string> yieldOpArgs;
    std::vector<std::string> changedHeaderArgs;
    auto loopTerminatorOp = findLastTerminatorOp(loopBodyBB, loopOp->getLoopHeadBlock()->getScopeLevel());
    if (loopTerminatorOp->getOperationType() == IR::Operations::Operation::BranchOp) {
        loopTerminatorArgs =
            std::static_pointer_cast<IR::Operations::BranchOperation>(loopTerminatorOp)->getNextBlockInvocation().getArguments();
    } else {
        loopTerminatorArgs =
            std::static_pointer_cast<IR::Operations::IfOperation>(loopTerminatorOp)->getFalseBlockInvocation().getArguments();
    }
    auto loopHeadBBArgs = loopOp->getLoopHeadBlock()->getArguments();
    for (int i = 0; i < (int) loopHeadBBArgs.size(); ++i) {
        if (loopHeadBBArgs.at(i)->getIdentifier() != loopTerminatorArgs.at(i)->getIdentifier()
            && loopHeadBBArgs.at(i)->getIdentifier() != loopInductionVarName) {
            iteratorArgs.push_back(frame.getValue(loopHeadBBArgs.at(i)->getIdentifier()));
            yieldOpArgs.push_back(loopTerminatorArgs.at(i)->getIdentifier());
        }
    }
    // Create Loop Operation. Save InsertionPoint(IP) of parent's BasicBlock
    insertComment("// For Loop Start");
    auto upperBound = builder->create<mlir::arith::IndexCastOp>(getNameLoc("UpperBound"),
                                                                builder->getIndexType(),
                                                                frame.getValue(compareOp->getRightInput()->getIdentifier()));

    auto forLoop = builder->create<mlir::scf::ForOp>(getNameLoc("forLoop"), lowerBound, upperBound, stepSize, iteratorArgs);
    //Replace header input args that are changed in body with dynamic iter args from ForOp.
    for (int i = 0; i < (int) changedHeaderArgs.size(); ++i) {
        frame.setValue(changedHeaderArgs.at(i), forLoop.getRegionIterArgs().begin()[i]);
    }
    auto parentBlockInsertionPoint = builder->saveInsertionPoint();

    // -=LOOP BODY=-
    // Set Insertion Point(IP) to loop BasicBlock. Process all Operations in loop BasicBlock. Restore IP to parent's BasicBlock.
    builder->setInsertionPointToStart(forLoop.getBody());
    auto loopBodyArgs = frame;
    auto mlirIndexCast = builder->create<mlir::arith::IndexCastOp>(getNameLoc("InductionVar Cast"),
                                                                   builder->getI64Type(),
                                                                   forLoop.getInductionVar());
    loopBodyArgs.setValue(compareOp->getLeftInput()->getIdentifier(), mlirIndexCast);
    generateMLIR(loopBodyBB, loopBodyArgs);
    if (yieldOpArgs.size() > 0) {
        std::vector<mlir::Value> forYieldOps;
        for (auto yieldOpArg : yieldOpArgs) {
            forYieldOps.push_back(loopBodyArgs.getValue(yieldOpArg));
        }
        builder->create<mlir::scf::YieldOp>(getNameLoc("forYield"), forYieldOps);
    }
    //Todo We have update the values according to the 'afterLoopBB' value names
    for (int i = 0; i < (int) afterLoopBB->getArguments().size(); ++i) {
        // If higher level block arg names are different, add values with new names and delete old entries.
        auto nextArgName = afterLoopBB->getArguments().at(i);
        auto nextframe = std::static_pointer_cast<IR::Operations::IfOperation>(loopOp->getLoopHeadBlock()->getTerminatorOp())
                             ->getFalseBlockInvocation()
                             .getArguments();
        if (!(nextArgName->getIdentifier() == nextframe.at(i)->getIdentifier())
            && !inductionVars.contains(nextArgName->getIdentifier())) {
            // TODO what to do here?
            //auto nameChangeNode = frame.extract(nextframe.at(i)->getIdentifier());
            //nameChangeNode.key() = nextArgName->getIdentifier();
            //frame.insert(std::move(nameChangeNode));
        }
    }
    // Leave LoopOp
    builder->restoreInsertionPoint(parentBlockInsertionPoint);
    // LoopOp might be last Op in e.g. a nested If-Else case. The loopEndBB might then be higher in scope and reached from another Op.
    if (loopOp->getLoopHeadBlock()->getScopeLevel() <= afterLoopBB->getScopeLevel()) {
        generateMLIR(afterLoopBB, frame);
    }
    */
}

//==--------------------------==//
//==-- MEMORY (LOAD, STORE) --==//
//==--------------------------==//
void MLIRGenerator::generateMLIR(std::shared_ptr<IR::Operations::AddressOperation> addressOp, ValueFrame& frame) {
    printf("AddressOp");
    mlir::Value recordOffset;
    if (addressOp->getRecordIdxName() != "") {
        recordOffset = builder->create<mlir::LLVM::MulOp>(getNameLoc("recordOffset"),
                                                          frame.getValue(addressOp->getRecordIdxName()),
                                                          getConstInt("1", 64, addressOp->getRecordWidthInBytes()));
    } else {
        recordOffset = getConstInt("0", 64, 0);
    }
    mlir::Value fieldOffset = builder->create<mlir::LLVM::AddOp>(getNameLoc("fieldOffset"),
                                                                 recordOffset,
                                                                 getConstInt("1", 64, addressOp->getFieldOffsetInBytes()));
    // Return I8* to first byte of field data
    mlir::Value elementAddress = builder->create<mlir::LLVM::GEPOp>(getNameLoc("fieldAccess"),
                                                                    mlir::LLVM::LLVMPointerType::get(builder->getI8Type()),
                                                                    frame.getValue(addressOp->getAddressSourceName()),
                                                                    mlir::ArrayRef<mlir::Value>({fieldOffset}));
    auto mlirBitcast =
        builder->create<mlir::LLVM::BitcastOp>(getNameLoc("Address Bitcasted"),
                                               mlir::LLVM::LLVMPointerType::get(getMLIRType(addressOp->getDataType())),
                                               elementAddress);
    frame.setValue(addressOp->getIdentifier(), mlirBitcast);
}

// No recursion. Dependencies. Requires addressMap insertion.
void MLIRGenerator::generateMLIR(std::shared_ptr<IR::Operations::LoadOperation> loadOp, ValueFrame& frame) {
    printf("LoadOp.\n");
    auto address = frame.getValue(loadOp->getAddress()->getIdentifier());
    auto bitcast = builder->create<mlir::LLVM::BitcastOp>(getNameLoc("Address Bitcasted"),
                                                          mlir::LLVM::LLVMPointerType::get(getMLIRType(loadOp->getStamp())),
                                                          address);
    auto mlirLoadOp = builder->create<mlir::LLVM::LoadOp>(getNameLoc("loadedValue"), bitcast);
    frame.setValue(loadOp->getIdentifier(), mlirLoadOp);
}

// No Recursion. No dependencies. Requires addressMap insertion.
void MLIRGenerator::generateMLIR(std::shared_ptr<IR::Operations::ConstIntOperation> constIntOp, ValueFrame& frame) {
    printf("ConstIntOp");
    if (!frame.contains(constIntOp->getIdentifier())) {
        frame.setValue(constIntOp->getIdentifier(),
                       getConstInt("ConstantOp", constIntOp->getNumBits(), constIntOp->getConstantIntValue()));
    } else {
        frame.setValue(constIntOp->getIdentifier(),
                       getConstInt("ConstantOp", constIntOp->getNumBits(), constIntOp->getConstantIntValue()));
    }
}

// No Recursion. No dependencies. Requires addressMap insertion.
void MLIRGenerator::generateMLIR(std::shared_ptr<IR::Operations::ConstFloatOperation> constFloatOp, ValueFrame& frame) {
    printf("ConstFloatOp");
    auto floatType = (constFloatOp->getNumBits() == 32) ? builder->getF32Type() : builder->getF64Type();
    if (!frame.contains(constFloatOp->getIdentifier())) {
        frame.setValue(
            constFloatOp->getIdentifier(),
            builder->create<mlir::LLVM::ConstantOp>(getNameLoc("constantFloat"),
                                                    floatType,
                                                    builder->getFloatAttr(floatType, constFloatOp->getConstantFloatValue())));
    } else {
        frame.setValue(
            constFloatOp->getIdentifier(),
            builder->create<mlir::LLVM::ConstantOp>(getNameLoc("constantFloat"),
                                                    floatType,
                                                    builder->getFloatAttr(floatType, constFloatOp->getConstantFloatValue())));
    }
}

//==---------------------------==//
//==-- ARITHMETIC OPERATIONS --==//
//==---------------------------==//
void MLIRGenerator::generateMLIR(std::shared_ptr<IR::Operations::AddOperation> addOp, ValueFrame& frame) {
    auto leftInput = frame.getValue(addOp->getLeftInput()->getIdentifier());
    auto rightInput = frame.getValue(addOp->getRightInput()->getIdentifier());
    if (addOp->getStamp() == IR::Operations::PrimitiveStamp::FLOAT
        || addOp->getStamp() == IR::Operations::PrimitiveStamp::DOUBLE) {

        auto mlirAddOp = builder->create<mlir::LLVM::FAddOp>(getNameLoc("binOpResult"),
                                                             leftInput.getType(),
                                                             leftInput,
                                                             rightInput,
                                                             mlir::LLVM::FastmathFlags::fast);
        frame.setValue(addOp->getIdentifier(), mlirAddOp);
    } else {
        if (!inductionVars.contains(addOp->getLeftInput()->getIdentifier())) {
            if (!frame.contains(addOp->getIdentifier())) {
                auto mlirAddOp = builder->create<mlir::LLVM::AddOp>(getNameLoc("binOpResult"), leftInput, rightInput);
                frame.setValue(addOp->getIdentifier(), mlirAddOp);
            } else {
                auto mlirAddOp = builder->create<mlir::LLVM::AddOp>(getNameLoc("binOpResult"), leftInput, rightInput);
                frame.setValue(addOp->getIdentifier(), mlirAddOp);
            }
        }
    }
}

// No recursion. Dependencies. Requires addressMap insertion.
void MLIRGenerator::generateMLIR(std::shared_ptr<IR::Operations::SubOperation> subIntOp, ValueFrame& frame) {
    auto leftInput = frame.getValue(subIntOp->getLeftInput()->getIdentifier());
    auto rightInput = frame.getValue(subIntOp->getRightInput()->getIdentifier());
    if (subIntOp->getStamp() == IR::Operations::PrimitiveStamp::FLOAT
        || subIntOp->getStamp() == IR::Operations::PrimitiveStamp::DOUBLE) {
        auto mlirSubOp = builder->create<mlir::LLVM::FSubOp>(getNameLoc("binOpResult"),
                                                             leftInput,
                                                             rightInput,
                                                             mlir::LLVM::FMFAttr::get(context, mlir::LLVM::FastmathFlags::fast));
        frame.setValue(subIntOp->getIdentifier(), mlirSubOp);
    } else {
        auto mlirSubOp = builder->create<mlir::LLVM::SubOp>(getNameLoc("binOpResult"), leftInput, rightInput);
        frame.setValue(subIntOp->getIdentifier(), mlirSubOp);
    }
}

// No recursion. Dependencies. Requires addressMap insertion.
void MLIRGenerator::generateMLIR(std::shared_ptr<IR::Operations::MulOperation> mulOp, ValueFrame& frame) {
    auto leftInput = frame.getValue(mulOp->getLeftInput()->getIdentifier());
    auto rightInput = frame.getValue(mulOp->getRightInput()->getIdentifier());
    auto resultType = leftInput.getType();
    if (mulOp->getStamp() == IR::Operations::PrimitiveStamp::FLOAT
        || mulOp->getStamp() == IR::Operations::PrimitiveStamp::DOUBLE) {
        auto mlirMulOp = builder->create<mlir::LLVM::FMulOp>(getNameLoc("binOpResult"),
                                                             resultType,
                                                             leftInput,
                                                             rightInput,
                                                             mlir::LLVM::FastmathFlags::fast);
        frame.setValue(mulOp->getIdentifier(), mlirMulOp);
    } else {
        auto mlirMulOp = builder->create<mlir::LLVM::MulOp>(getNameLoc("binOpResult"), resultType, leftInput, rightInput);
        frame.setValue(mulOp->getIdentifier(), mlirMulOp);
    }
}

// No recursion. Dependencies. Requires addressMap insertion.
void MLIRGenerator::generateMLIR(std::shared_ptr<IR::Operations::DivOperation> divIntOp, ValueFrame& frame) {
    auto leftInput = frame.getValue(divIntOp->getLeftInput()->getIdentifier());
    auto rightInput = frame.getValue(divIntOp->getRightInput()->getIdentifier());
    auto resultType = leftInput.getType();
    if (divIntOp->getStamp() == IR::Operations::PrimitiveStamp::FLOAT
        || divIntOp->getStamp() == IR::Operations::PrimitiveStamp::DOUBLE) {
        auto mlirDivOp = builder->create<mlir::LLVM::FDivOp>(getNameLoc("binOpResult"),
                                                             resultType,
                                                             leftInput,
                                                             rightInput,
                                                             mlir::LLVM::FastmathFlags::fast);
        frame.setValue(divIntOp->getIdentifier(), mlirDivOp);
    } else {
        if (resultType.isSignedInteger()) {
            auto mlirDivOp = builder->create<mlir::LLVM::SDivOp>(getNameLoc("binOpResult"), resultType, leftInput, rightInput);
            frame.setValue(divIntOp->getIdentifier(), mlirDivOp);
        } else {
            auto mlirDivOp = builder->create<mlir::LLVM::UDivOp>(getNameLoc("binOpResult"), resultType, leftInput, rightInput);
            frame.setValue(divIntOp->getIdentifier(), mlirDivOp);
        }
    }
}

// No recursion. Dependencies. Does NOT require addressMap insertion.
void MLIRGenerator::generateMLIR(std::shared_ptr<IR::Operations::StoreOperation> storeOp, ValueFrame& frame) {
    printf("StoreOp\n");
    auto value = frame.getValue(storeOp->getValue()->getIdentifier());
    auto address = frame.getValue(storeOp->getAddress()->getIdentifier());
    auto bitcast = builder->create<mlir::LLVM::BitcastOp>(getNameLoc("Address Bitcasted"),
                                                          mlir::LLVM::LLVMPointerType::get(value.getType()),
                                                          address);
    builder->create<mlir::LLVM::StoreOp>(getNameLoc("outputStore"), value, bitcast);
}

// No recursion. Dependencies. Does NOT require addressMap insertion.
void MLIRGenerator::generateMLIR(std::shared_ptr<IR::Operations::ReturnOperation> returnOp, ValueFrame& frame) {
    printf("ReturnOp\n");

    // Insert return into 'execute' function block. This is the FINAL return.
    if (!returnOp->hasReturnValue()) {
        builder->create<mlir::LLVM::ReturnOp>(getNameLoc("return"), mlir::ValueRange());
    } else {
        builder->create<mlir::LLVM::ReturnOp>(getNameLoc("return"), frame.getValue(returnOp->getReturnValue()->getIdentifier()));
    }
}

// No recursion. Dependencies. Does NOT require addressMap insertion.
void MLIRGenerator::generateMLIR(std::shared_ptr<IR::Operations::ProxyCallOperation> proxyCallOp, ValueFrame& frame) {
    printf("ProxyCallOperation identifier: %s\n", proxyCallOp->getIdentifier().c_str());
    printf("ProxyCallOperation first BlockArg name: \n");

    //Todo simplify!!!!
    switch (proxyCallOp->getProxyCallType()) {
        case IR::Operations::Operation::GetDataBuffer: {
            auto callOperation =
                builder
                    ->create<mlir::CallOp>(getNameLoc("memberCall"),
                                           memberFunctions[IR::Operations::Operation::GetDataBuffer],
                                           frame.getValue(proxyCallOp->getInputArguments().at(0)->getIdentifier()))
                    .getResult(0);
            frame.setValue(proxyCallOp->getIdentifier(), callOperation);
            break;
        }
        case IR::Operations::Operation::SetNumTuples: {
            std::vector<mlir::Value> operands = {frame.getValue(proxyCallOp->getInputArguments().at(0)->getIdentifier()),
                                                 frame.getValue(proxyCallOp->getInputArguments().at(1)->getIdentifier())};
            frame.setValue(proxyCallOp->getIdentifier(),
                           builder
                               ->create<mlir::CallOp>(getNameLoc("memberCall"),
                                                      memberFunctions[IR::Operations::Operation::SetNumTuples],
                                                      operands)
                               .getResult(0));
            break;
        }
        case IR::Operations::Operation::GetNumTuples:
            frame.setValue(proxyCallOp->getIdentifier(),
                           builder
                               ->create<mlir::CallOp>(getNameLoc("memberCall"),
                                                      memberFunctions[IR::Operations::Operation::GetNumTuples],
                                                      frame.getValue(proxyCallOp->getInputArguments().at(0)->getIdentifier()))
                               .getResult(0));
            break;
        default:
            mlir::FlatSymbolRefAttr functionRef;
            if (theModule.lookupSymbol<mlir::LLVM::LLVMFuncOp>(proxyCallOp->getFunctionSymbol())) {
                functionRef = mlir::SymbolRefAttr::get(context, proxyCallOp->getFunctionSymbol());
            } else {
                functionRef = insertExternalFunction(proxyCallOp->getFunctionSymbol(),
                                                     proxyCallOp->getFunctionPtr(),
                                                     getMLIRType(proxyCallOp->getStamp()),
                                                     getMLIRType(proxyCallOp->getInputArguments()),
                                                     true);
            }
            std::vector<mlir::Value> functionArgs;
            for (auto arg : proxyCallOp->getInputArguments()) {
                functionArgs.push_back(frame.getValue(arg->getIdentifier()));
            }
            if (proxyCallOp->getStamp() != IR::Operations::VOID) {
                auto res = builder->create<mlir::LLVM::CallOp>(getNameLoc("printFunc"),
                                                               getMLIRType(proxyCallOp->getStamp()),
                                                               functionRef,
                                                               functionArgs);
                frame.setValue(proxyCallOp->getIdentifier(), res.getResult(0));
            } else {
                builder->create<mlir::LLVM::CallOp>(getNameLoc("printFunc"), mlir::None, functionRef, functionArgs);
            }
            break;
    }
}

// No recursion. Dependencies. Does NOT require addressMap insertion.
void MLIRGenerator::generateMLIR(std::shared_ptr<IR::Operations::CompareOperation> compareOp, ValueFrame& frame) {
    printf("CompareOperation identifier: %s\n", compareOp->getIdentifier().c_str());
    // Comparator Enum < 10 is either a signed or unsigned Integer comparison.
    if (compareOp->getComparator() < 10) {
        auto cmpOp = builder->create<mlir::arith::CmpIOp>(getNameLoc("comparison"),
                                                          convertToIntMLIRComparison(compareOp->getComparator()),
                                                          frame.getValue(compareOp->getLeftInput()->getIdentifier()),
                                                          frame.getValue(compareOp->getRightInput()->getIdentifier()));
        frame.setValue(compareOp->getIdentifier(), cmpOp);
    } else {
        auto cmpOp = builder->create<mlir::arith::CmpFOp>(getNameLoc("comparison"),
                                                          convertToFloatMLIRComparison(compareOp->getComparator()),
                                                          frame.getValue(compareOp->getLeftInput()->getIdentifier()),
                                                          frame.getValue(compareOp->getRightInput()->getIdentifier()));
        frame.setValue(compareOp->getIdentifier(), cmpOp);
    }
}

void MLIRGenerator::generateSCFIf(std::shared_ptr<IR::Operations::IfOperation> ifOp, ValueFrame& frame) {
    auto currentInsertionPoint = builder->saveInsertionPoint();
    // get merge block to determine the result values of the if operation
    auto mergeBlock = ifOp->getMergeBlock();

    // create result types for if
    std::vector<mlir::Type> mlirIfOpResultTypes;
    for (auto afterIfBlockArgType : mergeBlock->getArguments()) {
        mlirIfOpResultTypes.push_back(getMLIRType(afterIfBlockArgType->getStamp()));
    }

    auto mlirIfOp = builder->create<mlir::scf::IfOp>(getNameLoc("ifOperation"),
                                                     mlirIfOpResultTypes,
                                                     frame.getValue(ifOp->getValue()->getIdentifier()),
                                                     ifOp->hasFalseCase());

    // for all predecessors of the merger block we delete the final branch operation and replace it with a mlir yield operation.
    for (auto predecessor : mergeBlock->getPredecessors()) {
        NES_ASSERT(!predecessor.expired(), "predecessor of merge block should exists.");
        auto pred = predecessor.lock();
        if (pred->getTerminatorOp()->getOperationType() == IR::Operations::Operation::MLIR_YIELD)
            continue;
        NES_ASSERT(pred->getTerminatorOp()->getOperationType() == IR::Operations::Operation::BranchOp,
                   "A control-flow merge of an if must terminate with a branch op.");
        auto branch = std::static_pointer_cast<IR::Operations::BranchOperation>(pred->getTerminatorOp());
        pred->popOperation();
        pred->addOperation(std::make_shared<IR::Operations::YieldOperation>(branch->getNextBlockInvocation().getArguments(),
                                                                            branch->getNextBlockInvocation().getBlock()));
    }

    // IF case -> Change scope to true case. Finish by setting obligatory YieldOp for true case.
    {
        builder->setInsertionPointToStart(mlirIfOp.getThenBodyBuilder().getInsertionBlock());
        ValueFrame trueCaseFrame = createFrameFromParentBlock(frame, ifOp->getTrueBlockInvocation());
        generateMLIR(ifOp->getTrueBlockInvocation().getBlock(), trueCaseFrame);
    }
    if (ifOp->hasFalseCase()) {
        builder->setInsertionPointToStart(mlirIfOp.getElseBodyBuilder().getInsertionBlock());
        ValueFrame trueCaseFrame = createFrameFromParentBlock(frame, ifOp->getFalseBlockInvocation());
        generateMLIR(ifOp->getFalseBlockInvocation().getBlock(), trueCaseFrame);
    }

    // prepare frame for code generation of merge block
    builder->restoreInsertionPoint(currentInsertionPoint);
    ValueFrame mergeBlockFrame;
    for (size_t i = 0; i < mergeBlock->getArguments().size(); ++i) {
        mergeBlockFrame.setValue(mergeBlock->getArguments().at(i)->getIdentifier(), mlirIfOp.getResult(i));
    }
    generateMLIR(mergeBlock, mergeBlockFrame);
}

void MLIRGenerator::generateCFIf(std::shared_ptr<IR::Operations::IfOperation> ifOp, ValueFrame& frame) {
    auto parentBlockInsertionPoint = builder->saveInsertionPoint();

    std::vector<mlir::Value> trueBlockArgs;
    mlir::Block* trueBlock = generateBasicBlock(ifOp->getTrueBlockInvocation(), frame);
    for (auto blockArg : ifOp->getTrueBlockInvocation().getArguments()) {
        trueBlockArgs.push_back(frame.getValue(blockArg->getIdentifier()));
    }

    std::vector<mlir::Value> elseBlockArgs;
    mlir::Block* elseBlock = generateBasicBlock(ifOp->getFalseBlockInvocation(), frame);
    for (auto blockArg : ifOp->getFalseBlockInvocation().getArguments()) {
        elseBlockArgs.push_back(frame.getValue(blockArg->getIdentifier()));
    }

    builder->restoreInsertionPoint(parentBlockInsertionPoint);
    builder->create<mlir::CondBranchOp>(getNameLoc("branch"),
                                        frame.getValue(ifOp->getValue()->getIdentifier()),
                                        trueBlock,
                                        trueBlockArgs,
                                        elseBlock,
                                        elseBlockArgs);
}

// No recursion. Dependencies. Does NOT require addressMap insertion.
void MLIRGenerator::generateMLIR(std::shared_ptr<IR::Operations::IfOperation> ifOp, ValueFrame& frame) {

    // TODO we have to determine if we can use scf for this if operation
    if (useSCF) {
        generateSCFIf(ifOp, frame);
    } else {
        generateCFIf(ifOp, frame);
    }

    /*
     * // Find next block that is on the same or on a higher scopeLevel than ifOp and get its input args.
    // (could use else Block, if exists -> allows for possibly faster traversal, but we will change this part anyway)
    auto ifOpTerminatorOp = findLastTerminatorOp(ifOp->getTrueBlockInvocation().getBlock(),
                                                 ifOp->getTrueBlockInvocation().getBlock()->getScopeLevel() - 1);
    IR::BasicBlockPtr afterIfOpBlock = (ifOpTerminatorOp->getOperationType() == IR::Operations::Operation::BranchOp)
        ? std::static_pointer_cast<IR::Operations::BranchOperation>(ifOpTerminatorOp)->getNextBlockInvocation().getBlock()
        : std::static_pointer_cast<IR::Operations::IfOperation>(ifOpTerminatorOp)->getFalseBlockInvocation().getBlock();
    auto afterIfOpframe = afterIfOpBlock->getArguments();

    // Create MLIR-IF-Operation
    mlir::scf::IfOp mlirIfOp;
    auto currentInsertionPoint = builder->saveInsertionPoint();
    std::vector<mlir::Type> mlirIfOpResultTypes;
    // Register all frame for the IfOperation as output/yield args. All values that are not modified, will just be returned.
    bool hasElse = (ifOp->getFalseBlockInvocation().getBlock() != nullptr);
    bool requiresYield = false;
    for (auto nextBlockArg : afterIfOpframe) {
        if (!inductionVars.contains(nextBlockArg->getIdentifier())) {
            requiresYield = true;
            break;
        }
    }
    if (requiresYield) {
        for (auto afterIfBlockArgType : afterIfOpBlock->getArguments()) {
            mlirIfOpResultTypes.push_back(getMLIRType(afterIfBlockArgType->getStamp()));
        }
    }
    mlirIfOp = builder->create<mlir::scf::IfOp>(getNameLoc("ifOperation"),
                                                mlirIfOpResultTypes,
                                                frame.getValue(ifOp->getValue()->getIdentifier()),
                                                (hasElse || requiresYield));


    // IF case -> Change scope to Then Branch. Finish by setting obligatory YieldOp for THEN case.
    builder->setInsertionPointToStart(mlirIfOp.getThenBodyBuilder().getInsertionBlock());
    auto ifframe = frame;
    generateMLIR(ifOp->getTrueBlockInvocation().getBlock(), ifframe);

    // An MLIR ifOp MUST have an else case, if it returns values (YieldOp). The else case must return the same arg types.
    // But, an MLIR ifOp might still have an else case, even it does not require a YieldOp.
    if (requiresYield) {
        std::vector<mlir::Value> ifYieldOps;
        for (auto afterIfBlockArg : afterIfOpframe) {
            ifYieldOps.push_back(ifframe.getValue(afterIfBlockArg->getIdentifier()));
        }
        builder->create<mlir::scf::YieldOp>(getNameLoc("ifYield"), ifYieldOps);
        // No else case, but yielding required. Insert hollow else case with duplicated if-yield.
        if (!hasElse) {
            builder->setInsertionPointToStart(mlirIfOp.getElseBodyBuilder().getInsertionBlock());
            builder->create<mlir::scf::YieldOp>(getNameLoc("copiedElseYield"), ifYieldOps);
        }
    }
    if (hasElse) {
        builder->setInsertionPointToStart(mlirIfOp.getElseBodyBuilder().getInsertionBlock());
        auto elseframe = frame;
        if (requiresYield) {
            std::vector<mlir::Value> elseYieldOps;
            if (ifOp->getFalseBlockInvocation().getBlock() != nullptr) {
                generateMLIR(ifOp->getFalseBlockInvocation().getBlock(), elseframe);
                for (auto afterIfBlockArg : afterIfOpframe) {
                    elseYieldOps.push_back(elseframe.getValue(afterIfBlockArg->getIdentifier()));
                }
                builder->create<mlir::scf::YieldOp>(getNameLoc("elseYield"), elseYieldOps);
            }
        }
    }

    // Back to IfOp scope. Get all yielded result values from IfOperation, write to frame and proceed.
    builder->restoreInsertionPoint(currentInsertionPoint);
    if (requiresYield) {
        for (int i = 0; i < (int) afterIfOpBlock->getArguments().size(); ++i) {
            frame.setValue(afterIfOpBlock->getArguments().at(i)->getIdentifier(), mlirIfOp.getResult(i));
        }
    }
    if (afterIfOpBlock->getScopeLevel() == (ifOp->getTrueBlockInvocation().getBlock()->getScopeLevel()) - 1) {
        generateMLIR(afterIfOpBlock, frame);
    }
     */
}

//==--------------------- Dummy OPERATIONS ----------------------==//
// No recursion. Dependencies. Does NOT require addressMap insertion.
void MLIRGenerator::generateMLIR(std::shared_ptr<IR::Operations::BranchOperation> branchOp, ValueFrame& frame) {
    printf("BranchOperation.getNextBlock.getIdentifier: %s\n",
           branchOp->getNextBlockInvocation().getBlock()->getIdentifier().c_str());
    printf("BranchOperation first BlockArg name:\n");
    if (!useSCF) {
        std::vector<mlir::Value> mlirTargetBlockArguments;
        for (auto targetBlockArgument : branchOp->getNextBlockInvocation().getArguments()) {
            mlirTargetBlockArguments.push_back(frame.getValue(targetBlockArgument->getIdentifier()));
        }
        auto* mlirTargetBlock = generateBasicBlock(branchOp->getNextBlockInvocation(), frame);
        builder->create<mlir::BranchOp>(getNameLoc("branch"), mlirTargetBlock, mlirTargetBlockArguments);
    }
}

mlir::Block* MLIRGenerator::generateBasicBlock(IR::Operations::BasicBlockInvocation& blockInvocation, ValueFrame&) {
    auto targetBlock = blockInvocation.getBlock();
    // check if the block already exists
    if (blockMapping.contains(targetBlock->getIdentifier())) {
        return blockMapping[targetBlock->getIdentifier()];
    }

    auto parentBlockInsertionPoint = builder->saveInsertionPoint();
    // create new block
    auto mlirBasicBlock = builder->createBlock(builder->getBlock()->getParent());

    auto targetBlockArguments = targetBlock->getArguments();
    // add attributes as arguments to block
    for (auto headBlockHeadTypes : targetBlockArguments) {
        mlirBasicBlock->addArgument(getMLIRType(headBlockHeadTypes->getStamp()), getNameLoc("arg"));
    }
    ValueFrame blockFrame;
    for (uint32_t i = 0; i < targetBlockArguments.size(); i++) {
        blockFrame.setValue(targetBlock->getArguments()[i]->getIdentifier(), mlirBasicBlock->getArgument(i));
    }

    blockMapping[blockInvocation.getBlock()->getIdentifier()] = mlirBasicBlock;
    builder->setInsertionPointToStart(mlirBasicBlock);
    generateMLIR(targetBlock, blockFrame);
    builder->restoreInsertionPoint(parentBlockInsertionPoint);

    return mlirBasicBlock;
}

std::vector<std::string> MLIRGenerator::getJitProxyFunctionSymbols() { return jitProxyFunctionSymbols; }
std::vector<llvm::JITTargetAddress> MLIRGenerator::getJitProxyTargetAddresses() { return jitProxyFunctionTargetAddresses; }
MLIRGenerator::ValueFrame MLIRGenerator::createFrameFromParentBlock(MLIRGenerator::ValueFrame& frame,
                                                                    IR::Operations::BasicBlockInvocation& invocation) {
    auto invocationArguments = invocation.getArguments();
    auto childBlockArguments = invocation.getBlock()->getArguments();
    NES_ASSERT(invocationArguments.size() == childBlockArguments.size(),
               "the number of invocation parameters has to be the same as the number of block arguments in the invoked block.");
    ValueFrame childFrame;
    // copy all frame values to the child frame that are arguments of the child block
    for (uint64_t i = 0; i < invocationArguments.size(); i++) {
        auto parentOperation = invocationArguments[i];
        auto parentValue = frame.getValue(parentOperation->getIdentifier());
        auto childBlockArgument = childBlockArguments[i];
        childFrame.setValue(childBlockArgument->getIdentifier(), parentValue);
    }
    return childFrame;
}
void MLIRGenerator::generateMLIR(std::shared_ptr<IR::Operations::YieldOperation> yieldOperation,
                                 MLIRGenerator::ValueFrame& frame) {
    std::vector<mlir::Value> ifYieldOps;
    for (auto yieldArgument : yieldOperation->getArguments()) {
        ifYieldOps.push_back(frame.getValue(yieldArgument->getIdentifier()));
    }
    builder->create<mlir::scf::YieldOp>(getNameLoc("yield"), ifYieldOps);
}
}// namespace NES::ExecutionEngine::Experimental::MLIR