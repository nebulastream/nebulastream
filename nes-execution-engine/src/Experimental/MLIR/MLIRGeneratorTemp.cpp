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
#include <Experimental/NESIR/BasicBlocks/BasicBlock.hpp>
#include <Experimental/NESIR/Operations/AddIntOperation.hpp>
#include <Experimental/NESIR/Operations/BranchOperation.hpp>
#include <Experimental/NESIR/Operations/CompareOperation.hpp>
#include <Experimental/NESIR/Operations/IfOperation.hpp>
#include <Experimental/NESIR/Operations/LoopOperation.hpp>
#include <Experimental/NESIR/Operations/MulOperation.hpp>
#include <Experimental/NESIR/Operations/NegateOperation.hpp>
#include <Experimental/NESIR/Operations/Operation.hpp>
#include <Experimental/NESIR/Operations/ReturnOperation.hpp>
#include <cstdint>
#include <exception>
#include <llvm/ADT/StringRef.h>
#include <llvm/Transforms/Utils/Cloning.h>
#include <memory>
#include <mlir/Dialect/Arithmetic/IR/Arithmetic.h>
#include <mlir/Dialect/LLVMIR/LLVMDialect.h>
#include <mlir/Dialect/LLVMIR/LLVMTypes.h>
#include <mlir/Dialect/SCF/SCF.h>
#include <mlir/Dialect/StandardOps/IR/Ops.h>
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
mlir::Type MLIRGenerator::getMLIRType(IR::Operations::Operation::BasicType type) {
    switch (type) {
        case IR::Operations::Operation::INT1:
        case IR::Operations::Operation::BOOLEAN: return builder->getIntegerType(1);
        case IR::Operations::Operation::INT8:
        case IR::Operations::Operation::CHAR: return builder->getIntegerType(8);
        case IR::Operations::Operation::INT16: return builder->getIntegerType(16);
        case IR::Operations::Operation::INT32: return builder->getIntegerType(32);
        case IR::Operations::Operation::INT64: return builder->getIntegerType(64);
        case IR::Operations::Operation::FLOAT: return mlir::Float32Type::get(context);
        case IR::Operations::Operation::DOUBLE: return mlir::Float64Type::get(context);
        case IR::Operations::Operation::INT8PTR: return mlir::LLVM::LLVMPointerType::get(builder->getI8Type());
        case IR::Operations::Operation::VOID: return mlir::LLVM::LLVMVoidType::get(context);

        default: return builder->getIntegerType(32);
    }
}

std::vector<mlir::Type> MLIRGenerator::getMLIRType(std::vector<IR::Operations::Operation::BasicType> types) {
    std::vector<mlir::Type> resultTypes;
    for (auto type : types) {
        resultTypes.push_back(getMLIRType(type));
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
    auto baseLocation = mlir::FileLineColLoc::get(builder->getStringAttr(name), 0, 0);
    return mlir::NameLoc::get(builder->getStringAttr(name), baseLocation);
}

mlir::arith::CmpIPredicate convertToMLIRComparison(IR::Operations::CompareOperation::Comparator comparisonType) {
    //TODO extend to all comparison types
    switch (comparisonType) {
        case (IR::Operations::CompareOperation::Comparator::ISLT): return mlir::arith::CmpIPredicate::slt;
        default: return mlir::arith::CmpIPredicate::slt;
    }
}

// Todo: Problem: loopIfOp->getElseBranchBlock()->getParentBlockLevel would result in correct Op, but is not triggered
// -> we skip correct BranchOp
IR::Operations::OperationPtr MLIRGenerator::findLastTerminatorOp(IR::BasicBlockPtr thenBlock, int ifParentBlockLevel) {
    auto terminatorOp = thenBlock->getOperations().back();
    // Generate Args for next block
    if (terminatorOp->getOperationType() == IR::Operations::Operation::BranchOp) {
        auto branchOp = std::static_pointer_cast<IR::Operations::BranchOperation>(terminatorOp);
        if (branchOp->getNextBlock()->getScopeLevel() <= ifParentBlockLevel) {
            return branchOp;
        } else {
            return findLastTerminatorOp(branchOp->getNextBlock(), ifParentBlockLevel);
        }
    } else if (terminatorOp->getOperationType() == IR::Operations::Operation::LoopOp) {
        auto loopOp = std::static_pointer_cast<IR::Operations::LoopOperation>(terminatorOp);
        auto loopIfOp = std::static_pointer_cast<IR::Operations::IfOperation>(loopOp->getLoopHeadBlock()->getOperations().back());
        if (loopIfOp->getElseBranchBlock()->getScopeLevel() <= ifParentBlockLevel) {
            return loopIfOp;
        } else {
            return findLastTerminatorOp(loopIfOp->getElseBranchBlock(), ifParentBlockLevel);
        }
    } else {
        auto ifOp = std::static_pointer_cast<IR::Operations::IfOperation>(terminatorOp);
        if (ifOp->getElseBranchBlock() != nullptr) {
            return findLastTerminatorOp(ifOp->getElseBranchBlock(), ifParentBlockLevel);
        } else {
            return findLastTerminatorOp(ifOp->getThenBranchBlock(), ifParentBlockLevel);
        }
    }
}

//==-----------------------------------==//
//==-- Create & Insert Functionality --==//
//==-----------------------------------==//
mlir::FlatSymbolRefAttr MLIRGenerator::insertExternalFunction(const std::string& name,
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
    jitProxyFunctionTargetAddresses.push_back(llvm::pointerToJITTargetAddress(ProxyFunctions.getProxyFunctionAddress(name)));
    return mlir::SymbolRefAttr::get(context, name);
}

std::vector<mlir::FuncOp> MLIRGenerator::GetMemberFunctions(mlir::MLIRContext&) {
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
    //auto simpleModule = parseSourceString(moduleStr2, &context);
    //auto proxyOpIterator = simpleModule->getOps().begin()->getBlock()->getOperations().begin();

    //for (; proxyOpIterator != simpleModule->getOps().begin()->getBlock()->getOperations().end(); proxyOpIterator++) {
    //    memberFunctions.push_back(static_cast<mlir::FuncOp>(proxyOpIterator->clone()));
    //}
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
    std::unordered_map<std::string, mlir::Value> firstBlockArgs;
    generateMLIR(nesIR->getRootOperation(), firstBlockArgs);

    theModule->dump();
    if (failed(mlir::verify(theModule))) {
        theModule.emitError("module verification error");
        return nullptr;
    }
    return theModule;
}

void MLIRGenerator::generateMLIR(IR::BasicBlockPtr basicBlock, std::unordered_map<std::string, mlir::Value>& blockArgs) {
    auto terminatorOp = basicBlock->getOperations().back();
    basicBlock->popOperation();
    for (const auto& operation : basicBlock->getOperations()) {
        generateMLIR(operation, blockArgs);
    }
    // Generate Args for next block
    if (terminatorOp->getOperationType() == IR::Operations::Operation::BranchOp) {
        auto branchOp = std::static_pointer_cast<IR::Operations::BranchOperation>(terminatorOp);

        if (this->blockMapping.contains(branchOp->getNextBlock()->getIdentifier())) {
            std::vector<mlir::Value> iteratorArgs;
            for (auto headBlockArgs : branchOp->getNextBlockArgs()) {
                iteratorArgs.push_back(blockArgs[headBlockArgs]);
            }
            builder->create<mlir::BranchOp>(getNameLoc("branch"),
                                            blockMapping[branchOp->getNextBlock()->getIdentifier()],
                                            iteratorArgs);

        } else if (basicBlock->getScopeLevel() <= branchOp->getNextBlock()->getScopeLevel()) {
            generateMLIR(branchOp->getNextBlock(), blockArgs);
        } else {
            auto parentBlockInsertionPoint = builder->saveInsertionPoint();
            auto block = builder->createBlock(builder->getBlock()->getParent());
            blockMapping[branchOp->getNextBlock()->getIdentifier()] = block;
            for (auto headBlockHeadTypes : branchOp->getNextBlock()->getInputArgTypes()) {
                block->addArgument(getMLIRType(headBlockHeadTypes), getNameLoc("arg"));
            }
            std::unordered_map<std::string, mlir::Value> loopHeadFrame;
            for (uint32_t i = 0; i < branchOp->getNextBlockArgs().size(); i++) {
                loopHeadFrame[branchOp->getNextBlock()->getInputArgs()[i]] = block->getArgument(i);
            }
            generateMLIR(branchOp->getNextBlock(), loopHeadFrame);

            builder->restoreInsertionPoint(parentBlockInsertionPoint);

            std::vector<mlir::Value> iteratorArgs;
            for (auto headBlockArgs : branchOp->getNextBlockArgs()) {
                iteratorArgs.push_back(blockArgs[headBlockArgs]);
            }
            builder->create<mlir::BranchOp>(getNameLoc("branch"), block, iteratorArgs);

            /* for (int i = 0; i < (int) branchOp->getNextBlockArgs().size(); ++i) {
                // If higher level block arg names are different, add values with new names and delete old entries.
                auto nextArgName = branchOp->getNextBlock()->getInputArgs().at(i);
                if (!(nextArgName == branchOp->getNextBlockArgs().at(i)) && !inductionVars.contains(nextArgName)) {
                    auto nameChangeNode = blockArgs.extract(branchOp->getNextBlockArgs().at(i));
                    nameChangeNode.key() = nextArgName;
                    blockArgs.insert(std::move(nameChangeNode));
                }
            }
            */
        }
    } else if (terminatorOp->getOperationType() == IR::Operations::Operation::LoopOp) {
        auto loopOp = std::static_pointer_cast<IR::Operations::LoopOperation>(terminatorOp);
        /*
        for (int i = 0; i < (int) loopOp->getLoopBlockArgs().size(); ++i) {
            // If higher level block arg names are different, add values with new names and delete old entries.
            auto nextArgName = loopOp->getLoopHeadBlock()->getInputArgs().at(i);
            if (!(nextArgName == loopOp->getLoopBlockArgs().at(i)) && !inductionVars.contains(nextArgName)) {
                auto nameChangeNode = blockArgs.extract(loopOp->getLoopBlockArgs().at(i));
                nameChangeNode.key() = nextArgName;
                blockArgs.insert(std::move(nameChangeNode));
            }
        }
         */
        generateMLIR(loopOp, blockArgs);
    } else if (terminatorOp->getOperationType() == IR::Operations::Operation::IfOp) {
        auto ifOp = std::static_pointer_cast<IR::Operations::IfOperation>(terminatorOp);
        generateMLIR(ifOp, blockArgs);
    } else if (terminatorOp->getOperationType() == IR::Operations::Operation::ReturnOp) {
        auto returnOp = std::static_pointer_cast<IR::Operations::ReturnOperation>(terminatorOp);
        generateMLIR(returnOp, blockArgs);
    } else {// Currently necessary for LoopOps, where we remove the terminatorOp to prevent recursion.
        generateMLIR(terminatorOp, blockArgs);
    }
}

void MLIRGenerator::generateMLIR(const IR::Operations::OperationPtr& operation,
                                 std::unordered_map<std::string, mlir::Value>& blockArgs) {
    switch (operation->getOperationType()) {
        case IR::Operations::Operation::OperationType::FunctionOp:
            generateMLIR(std::static_pointer_cast<IR::Operations::FunctionOperation>(operation), blockArgs);
            break;
        case IR::Operations::Operation::OperationType::LoopOp:
            generateMLIR(std::static_pointer_cast<IR::Operations::LoopOperation>(operation), blockArgs);
            break;
        case IR::Operations::Operation::OperationType::ConstantOp:
            generateMLIR(std::static_pointer_cast<IR::Operations::ConstantIntOperation>(operation), blockArgs);
            break;
        case IR::Operations::Operation::OperationType::AddOp:
            generateMLIR(std::static_pointer_cast<IR::Operations::AddIntOperation>(operation), blockArgs);
            break;
        case IR::Operations::Operation::OperationType::LoadOp:
            generateMLIR(std::static_pointer_cast<IR::Operations::LoadOperation>(operation), blockArgs);
            break;
        case IR::Operations::Operation::OperationType::AddressOp:
            generateMLIR(std::static_pointer_cast<IR::Operations::AddressOperation>(operation), blockArgs);
            break;
        case IR::Operations::Operation::OperationType::StoreOp:
            generateMLIR(std::static_pointer_cast<IR::Operations::StoreOperation>(operation), blockArgs);
            break;
        case IR::Operations::Operation::OperationType::BranchOp:
            generateMLIR(std::static_pointer_cast<IR::Operations::BranchOperation>(operation), blockArgs);
            break;
        case IR::Operations::Operation::OperationType::IfOp:
            generateMLIR(std::static_pointer_cast<IR::Operations::IfOperation>(operation), blockArgs);
            break;
        case IR::Operations::Operation::OperationType::CompareOp:
            generateMLIR(std::static_pointer_cast<IR::Operations::CompareOperation>(operation), blockArgs);
            break;
        case IR::Operations::Operation::OperationType::ProxyCallOp:
            generateMLIR(std::static_pointer_cast<IR::Operations::ProxyCallOperation>(operation), blockArgs);
            break;
        case IR::Operations::Operation::OperationType::ReturnOp:
            generateMLIR(std::static_pointer_cast<IR::Operations::ReturnOperation>(operation), blockArgs);
            break;
        case IR::Operations::Operation::OperationType::MulOp:
            generateMLIR(std::static_pointer_cast<IR::Operations::MulOperation>(operation), blockArgs);
            break;
        case IR::Operations::Operation::OperationType::NegateOp:
            generateMLIR(std::static_pointer_cast<IR::Operations::NegateOperation>(operation), blockArgs);
            break;
    }
}

// Recursion. No dependencies. Does NOT require addressMap insertion.
void MLIRGenerator::generateMLIR(std::shared_ptr<IR::Operations::FunctionOperation> functionOp,
                                 std::unordered_map<std::string, mlir::Value>& blockArgs) {
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
void MLIRGenerator::generateMLIR(std::shared_ptr<IR::Operations::LoopOperation> loop,
                                 std::unordered_map<std::string, mlir::Value>& frame) {
    auto parentBlockInsertionPoint = builder->saveInsertionPoint();
    auto block = builder->createBlock(builder->getBlock()->getParent());
    blockMapping[loop->getLoopHeadBlock()->getIdentifier()] = block;
    for (auto headBlockHeadTypes : loop->getLoopHeadBlock()->getInputArgTypes()) {
        block->addArgument(getMLIRType(headBlockHeadTypes), getNameLoc("arg"));
    }
    std::unordered_map<std::string, mlir::Value> loopHeadFrame;
    for (uint32_t i = 0; i < loop->getLoopBlockArgs().size(); i++) {
        loopHeadFrame[loop->getLoopHeadBlock()->getInputArgs()[i]] = block->getArgument(i);
    }
    generateMLIR(loop->getLoopHeadBlock(), loopHeadFrame);

    builder->restoreInsertionPoint(parentBlockInsertionPoint);

    std::vector<mlir::Value> iteratorArgs;
    for (auto headBlockArgs : loop->getLoopBlockArgs()) {
        iteratorArgs.push_back(frame[headBlockArgs]);
    }
    builder->create<mlir::BranchOp>(getNameLoc("branch"), block, iteratorArgs);

    return;
    /*
  //  block->addArgument();

    auto inductionVariable = "3_3";
    auto inductionVariableBody = "1_5";

    auto loopIfOp = std::static_pointer_cast<IR::Operations::IfOperation>(loopOp->getLoopHeadBlock()->getOperations().at(2));
    auto loopBodyBlock = loopIfOp->getThenBranchBlock();


    // create list of iteration arguments
    std::vector<mlir::Value> iteratorArgs;
    for(auto loopBodyArgument : loopIfOp->getThenBlockArgs()){
        if(loopBodyArgument != inductionVariable){
            iteratorArgs.push_back(frame["0_2"]);
        }
    }

    auto lowerBound = builder->create<mlir::arith::IndexCastOp>(getNameLoc("lowerBound"),
                                                              builder->getIndexType(),
                                                              this->getConstInt("lowerBound", 64, 0));
    auto upperBound = builder->create<mlir::arith::IndexCastOp>(getNameLoc("upperBound"),
                                                                builder->getIndexType(),
                                                                this->getConstInt("upperBound", 64, 10));
    auto stepSize = builder->create<mlir::arith::IndexCastOp>(getNameLoc("Stepsize"),
                                              builder->getIndexType(),
                                              this->getConstInt("test", 64, 1));

    auto forLoop = builder->create<mlir::scf::ForOp>(getNameLoc("forLoop"), lowerBound, upperBound, stepSize, iteratorArgs);


    // discard  the last operation
    loopBodyBlock->getOperations().pop_back();
    loopBodyBlock->getOperations().pop_back();
    loopBodyBlock->getOperations().pop_back();
    std::unordered_map<std::string, mlir::Value> loopBodyFrame;
    loopBodyFrame[inductionVariableBody] = forLoop.getInductionVar();
    loopBodyFrame["1_6"] = forLoop.getRegionIterArgs()[0];
    auto parentBlockInsertionPoint = builder->saveInsertionPoint();
     builder->setInsertionPointToStart(forLoop.getBody());
    generateMLIR(loopBodyBlock, loopBodyFrame);
    builder->create<mlir::scf::YieldOp>(getNameLoc("forYield"), loopBodyFrame["1_1"]);

    builder->restoreInsertionPoint(parentBlockInsertionPoint);
    auto afterLoopBlock = loopIfOp->getElseBranchBlock();
    std::unordered_map<std::string, mlir::Value> afterLoopFrame;
    for(auto inputArg : afterLoopBlock->getInputArgs()){
        afterLoopFrame[inputArg] = forLoop.getResult(0);
    }
    generateMLIR(afterLoopBlock, afterLoopFrame);
*/
    /*
    // -=LOOP HEAD=-
    // Loop Head BasicBlock contains CompareOp and IfOperation(thenBlock, elseBlock)
    // thenBlock=loopBodyBB & elseBlock=executeReturnBB
    auto compareOp =
        std::static_pointer_cast<IR::Operations::CompareOperation>(loopOp->getLoopHeadBlock()->getOperations().at(1));
    auto loopIfOp = std::static_pointer_cast<IR::Operations::IfOperation>(loopOp->getLoopHeadBlock()->getOperations().at(2));
    auto loopBodyBB = loopIfOp->getThenBranchBlock();
    auto afterLoopBB = loopIfOp->getElseBranchBlock();

    std::string loopInductionVarName = compareOp->getLeftArgName();
    inductionVars.emplace(loopInductionVarName);

    // Define index variables for loop.
    // For now we assume that loops always use the "i" as the induction var and always increase "i" by one each step.
    auto lowerBound = builder->create<mlir::arith::IndexCastOp>(getNameLoc("LowerBound"),
                                                                builder->getIndexType(),
                                                                blockArgs[compareOp->getLeftArgName()]);
    auto stepSize =
        // builder->create<mlir::arith::IndexCastOp>(getNameLoc("Stepsize"), builder->getIndexType(), blockArgs["const1Op"]);

        builder->create<mlir::arith::IndexCastOp>(getNameLoc("Stepsize"),
                                                  builder->getIndexType(),
                                                  this->getConstInt("test", 64, 1));

    // Get the arguments of the branchOp, which branches back into loopHeader. Compare with loopHeader args.
    // All args that are different are args which are changed inside the loop and are hence iterator args.
    std::vector<mlir::Value> iteratorArgs{};
    std::vector<std::string> loopTerminatorArgs;
    std::vector<std::string> yieldOpArgs;
    std::vector<std::string> changedHeaderArgs;
    auto loopTerminatorOp = findLastTerminatorOp(loopBodyBB, loopOp->getLoopHeadBlock()->getScopeLevel());
    if (loopTerminatorOp->getOperationType() == IR::Operations::Operation::BranchOp) {
        loopTerminatorArgs = std::static_pointer_cast<IR::Operations::BranchOperation>(loopTerminatorOp)->getNextBlockArgs();
    } else {
        loopTerminatorArgs = std::static_pointer_cast<IR::Operations::IfOperation>(loopTerminatorOp)->getElseBlockArgs();
    }
    auto loopHeadBBArgs = loopOp->getLoopHeadBlock()->getInputArgs();
    for (int i = 0; i < (int) loopHeadBBArgs.size(); ++i) {
        if (loopHeadBBArgs.at(i) != loopTerminatorArgs.at(i) && loopHeadBBArgs.at(i) != loopInductionVarName) {
            iteratorArgs.push_back(blockArgs[loopHeadBBArgs.at(i)]);
            yieldOpArgs.push_back(loopTerminatorArgs.at(i));
        }
    }
    // Create Loop Operation. Save InsertionPoint(IP) of parent's BasicBlock
    insertComment("// For Loop Start");
    //auto upperBound = builder->create<mlir::arith::IndexCastOp>(getNameLoc("UpperBound"),
    //                                                            builder->getIndexType(),
    //                                                            blockArgs[compareOp->getRightArgName()]);
    auto upperBound = builder->create<mlir::arith::IndexCastOp>(getNameLoc("UpperBound"),
                                                                builder->getIndexType(),
                                                                this->getConstInt("test", 64, 10));

    auto forLoop = builder->create<mlir::scf::ForOp>(getNameLoc("forLoop"), lowerBound, upperBound, stepSize, iteratorArgs);
    //Replace header input args that are changed in body with dynamic iter args from ForOp.
    for (int i = 0; i < (int) changedHeaderArgs.size(); ++i) {
        blockArgs[changedHeaderArgs.at(i)] = forLoop.getRegionIterArgs().begin()[i];
    }
    auto parentBlockInsertionPoint = builder->saveInsertionPoint();

    // -=LOOP BODY=-
    // Set Insertion Point(IP) to loop BasicBlock. Process all Operations in loop BasicBlock. Restore IP to parent's BasicBlock.
    builder->setInsertionPointToStart(forLoop.getBody());
    std::unordered_map<std::string, mlir::Value> loopBodyArgs = blockArgs;
    loopBodyArgs[compareOp->getLeftArgName()] = builder->create<mlir::arith::IndexCastOp>(getNameLoc("InductionVar Cast"),
                                                                                          builder->getI64Type(),
                                                                                          forLoop.getInductionVar());
    std::unordered_map<std::string, mlir::Value> trueBlockFrame;
    for (uint32_t i = 0; i < loopBodyBB->getInputArgs().size(); i++) {
        auto parentFrameArgIdentifier = loopOp->getLoopBlockArgs()[i];
        auto trueCaseInputArgIdentifier = loopBodyBB->getInputArgs()[i];
        // each block introduces new argument names, whereby the index establishes the mapping between the child and the parent frame.
        loopBodyArgs[trueCaseInputArgIdentifier] = loopBodyArgs[parentFrameArgIdentifier];
    }
    generateMLIR(loopBodyBB, loopBodyArgs);
    if (yieldOpArgs.size() > 0) {
        std::vector<mlir::Value> forYieldOps;
        for (auto yieldOpArg : yieldOpArgs) {
            forYieldOps.push_back(loopBodyArgs[yieldOpArg]);
        }
        builder->create<mlir::scf::YieldOp>(getNameLoc("forYield"), forYieldOps);
    }
    //Todo We have update the values according to the 'afterLoopBB' value names
    for (int i = 0; i < (int) afterLoopBB->getInputArgs().size(); ++i) {
        // If higher level block arg names are different, add values with new names and delete old entries.
        auto nextArgName = afterLoopBB->getInputArgs().at(i);
        auto nextBlockArgs = std::static_pointer_cast<IR::Operations::IfOperation>(loopOp->getLoopHeadBlock()->getTerminatorOp())
                                 ->getElseBlockArgs();
        if (!(nextArgName == nextBlockArgs.at(i)) && !inductionVars.contains(nextArgName)) {
            auto nameChangeNode = blockArgs.extract(nextBlockArgs.at(i));
            nameChangeNode.key() = nextArgName;
            blockArgs.insert(std::move(nameChangeNode));
        }
    }
    // Leave LoopOp
    builder->restoreInsertionPoint(parentBlockInsertionPoint);
    // LoopOp might be last Op in e.g. a nested If-Else case. The loopEndBB might then be higher in scope and reached from another Op.
    if (loopOp->getLoopHeadBlock()->getScopeLevel() <= afterLoopBB->getScopeLevel()) {
        generateMLIR(afterLoopBB, blockArgs);
    }
     */
}

// No Recursion. No dependencies. Requires addressMap insertion.
void MLIRGenerator::generateMLIR(std::shared_ptr<IR::Operations::AddressOperation> addressOp,
                                 std::unordered_map<std::string, mlir::Value>& blockArgs) {
    printf("AddressOp");
    mlir::Value recordOffset = builder->create<mlir::LLVM::MulOp>(getNameLoc("recordOffset"),
                                                                  blockArgs[addressOp->getRecordIdxName()],
                                                                  getConstInt("1", 64, addressOp->getRecordWidth()));
    mlir::Value fieldOffset = builder->create<mlir::LLVM::AddOp>(getNameLoc("fieldOffset"),
                                                                 recordOffset,
                                                                 getConstInt("1", 64, addressOp->getFieldOffset()));
    // Return I8* to first byte of field data
    mlir::Value elementAddress = builder->create<mlir::LLVM::GEPOp>(getNameLoc("fieldAccess"),
                                                                    mlir::LLVM::LLVMPointerType::get(builder->getI8Type()),
                                                                    blockArgs[addressOp->getAddressSourceName()],
                                                                    mlir::ArrayRef<mlir::Value>({fieldOffset}));
    blockArgs.emplace(
        std::pair{addressOp->getIdentifier(),
                  builder->create<mlir::LLVM::BitcastOp>(getNameLoc("Address Bitcasted"),
                                                         mlir::LLVM::LLVMPointerType::get(getMLIRType(addressOp->getDataType())),
                                                         elementAddress)});
}

// No recursion. Dependencies. Requires addressMap insertion.
void MLIRGenerator::generateMLIR(std::shared_ptr<IR::Operations::LoadOperation> loadOp,
                                 std::unordered_map<std::string, mlir::Value>& blockArgs) {
    printf("LoadOp.\n");

    //  builder->create<mlir::LLVM::BitcastOp>(getNameLoc("Address Bitcasted"),
    //                                         mlir::LLVM::LLVMPointerType::get(getMLIRType()),
    //                                          elementAddress)
    //})

    blockArgs.emplace(std::pair{loadOp->getIdentifier(),
                                builder->create<mlir::LLVM::LoadOp>(getNameLoc("loadedValue"), blockArgs[loadOp->getArgName()])});
}

// No Recursion. No dependencies. Requires addressMap insertion.
void MLIRGenerator::generateMLIR(std::shared_ptr<IR::Operations::ConstantIntOperation> constIntOp,
                                 std::unordered_map<std::string, mlir::Value>& blockArgs) {
    printf("ConstIntOp");
    if (!blockArgs.contains(constIntOp->getIdentifier())) {
        blockArgs.emplace(std::pair{constIntOp->getIdentifier(),
                                    getConstInt("ConstantOp", constIntOp->getNumBits(), constIntOp->getConstantIntValue())});
    } else {
        blockArgs[constIntOp->getIdentifier()] =
            getConstInt("ConstantOp", constIntOp->getNumBits(), constIntOp->getConstantIntValue());
    }
}

void MLIRGenerator::generateMLIR(std::shared_ptr<IR::Operations::MulOperation> mullOp,
                                 std::unordered_map<std::string, mlir::Value>& blockArgs) {
    blockArgs[mullOp->getIdentifier()] = builder->create<mlir::LLVM::MulOp>(getNameLoc("binOpResult"),
                                                                            blockArgs[mullOp->getLeftArgName()],
                                                                            blockArgs[mullOp->getRightArgName()]);
    return;
}

void MLIRGenerator::generateMLIR(std::shared_ptr<IR::Operations::NegateOperation> negateOp,
                                 std::unordered_map<std::string, mlir::Value>& blockArgs) {
    blockArgs[negateOp->getIdentifier()] = builder->create<mlir::arith::CmpIOp>(getNameLoc("binOpResult"),
                                                                                mlir::arith::CmpIPredicate::eq,
                                                                                blockArgs[negateOp->getArgName()],
                                                                                getConstInt("binOpResult", 1, 0));
    return;
}

// No recursion. Dependencies. Requires addressMap insertion.
void MLIRGenerator::generateMLIR(std::shared_ptr<IR::Operations::AddIntOperation> addOp,
                                 std::unordered_map<std::string, mlir::Value>& blockArgs) {
    printf("AddOp");

    auto leftValue = blockArgs[addOp->getLeftArgName()];
    auto rightValue = blockArgs[addOp->getRightArgName()];
    // builder->create<mlir::LLVM::BitcastOp>(getNameLoc("Address Bitcasted"),
    //                                        mlir::LLVM::LLVMPointerType::get(getMLIRType(addressOp->getDataType())),
    //                                        elementAddress)})

    if (leftValue.getType().isa<mlir::LLVM::LLVMPointerType>()) {

        mlir::Value elementAddress = builder->create<mlir::LLVM::GEPOp>(getNameLoc("add_" + addOp->getIdentifier()),
                                                                        mlir::LLVM::LLVMPointerType::get(builder->getI8Type()),
                                                                        leftValue,
                                                                        mlir::ArrayRef<mlir::Value>({rightValue}));

        blockArgs[addOp->getIdentifier()] = elementAddress;
    } else {
        blockArgs[addOp->getIdentifier()] =
            builder->create<mlir::LLVM::AddOp>(getNameLoc("add_" + addOp->getIdentifier()), leftValue, rightValue);
    }
    return;
    // Only generate Add, if not loop induction variable ++ operation (only loopInduction vars have length 1).
    if (!inductionVars.contains(addOp->getLeftArgName())) {
        if (!blockArgs.contains(addOp->getIdentifier())) {
            blockArgs.emplace(std::pair{addOp->getIdentifier(),
                                        builder->create<mlir::LLVM::AddOp>(getNameLoc("binOpResult"),
                                                                           blockArgs[addOp->getLeftArgName()],
                                                                           blockArgs[addOp->getRightArgName()])});
        } else {
            blockArgs[addOp->getIdentifier()] = builder->create<mlir::LLVM::AddOp>(getNameLoc("binOpResult"),
                                                                                   blockArgs[addOp->getLeftArgName()],
                                                                                   blockArgs[addOp->getRightArgName()]);
        }
    }
}

// No recursion. Dependencies. Does NOT require addressMap insertion.
void MLIRGenerator::generateMLIR(std::shared_ptr<IR::Operations::StoreOperation> storeOp,
                                 std::unordered_map<std::string, mlir::Value>& blockArgs) {
    printf("StoreOp\n");
    auto value = storeOp->getValueArgName();
    auto address = storeOp->getAddressArgName();
    builder->create<mlir::LLVM::StoreOp>(getNameLoc("outputStore"), blockArgs[value], blockArgs[address]);
}

// No recursion. Dependencies. Does NOT require addressMap insertion.
void MLIRGenerator::generateMLIR(std::shared_ptr<IR::Operations::ReturnOperation> returnOp,
                                 std::unordered_map<std::string, mlir::Value>& blockArgs) {
    printf("ReturnOp\n");
    printf("ReturnOp blockArgs size: %d\n", (int) blockArgs.size());

    // Insert return into 'execute' function block. This is the FINAL return.
    builder->create<mlir::LLVM::ReturnOp>(getNameLoc("return"), getConstInt("return", 64, returnOp->getReturnOpCode()));
}

// No recursion. Dependencies. Does NOT require addressMap insertion.
void MLIRGenerator::generateMLIR(std::shared_ptr<IR::Operations::ProxyCallOperation> proxyCallOp,
                                 std::unordered_map<std::string, mlir::Value>& blockArgs) {
    printf("ProxyCallOperation identifier: %s\n", proxyCallOp->getIdentifier().c_str());
    printf("ProxyCallOperation first BlockArg name: %s\n", blockArgs.begin()->first.c_str());

    /*  switch (proxyCallOp->getProxyCallType()) {
        case IR::Operations::Operation::GetDataBuffer:
            blockArgs.emplace(std::pair{proxyCallOp->getIdentifier(),
                                        builder
                                            ->create<mlir::CallOp>(getNameLoc("memberCall"),
                                                                   memberFunctions[IR::Operations::Operation::GetDataBuffer],
                                                                   blockArgs[proxyCallOp->getInputArgNames().at(0)])
                                            .getResult(0)});
            break;
        case IR::Operations::Operation::GetNumTuples:
            blockArgs.emplace(std::pair{proxyCallOp->getIdentifier(),
                                        builder
                                            ->create<mlir::CallOp>(getNameLoc("memberCall"),
                                                                   memberFunctions[IR::Operations::Operation::GetNumTuples],
                                                                   blockArgs[proxyCallOp->getInputArgNames().at(0)])
                                            .getResult(0)});
            break;
        default:
        */
    //Todo probably have create new function call map, because Functions are not representable by mlir::Value
    mlir::FlatSymbolRefAttr functionRef;
    if (theModule.lookupSymbol<mlir::LLVM::LLVMFuncOp>(proxyCallOp->getFunctionName())) {
        functionRef = mlir::SymbolRefAttr::get(context, proxyCallOp->getFunctionName());
    } else {
        functionRef = insertExternalFunction(proxyCallOp->getFunctionName(),
                                             getMLIRType(proxyCallOp->getResultType()),
                                             getMLIRType(proxyCallOp->getInputArgTypes()),
                                             true);
    }
    std::vector<mlir::Value> functionArgs;
    for (auto arg : proxyCallOp->getInputArgNames()) {
        functionArgs.push_back(blockArgs[arg]);
    }
    if (proxyCallOp->getResultType() != IR::Operations::Operation::VOID) {
        auto call = builder->create<mlir::LLVM::CallOp>(getNameLoc("printFunc"),
                                                        getMLIRType(proxyCallOp->getResultType()),
                                                        functionRef,
                                                        functionArgs);
        blockArgs[proxyCallOp->getIdentifier()] = call.getResult(0);
    } else {
        builder->create<mlir::LLVM::CallOp>(getNameLoc("printFunc"), mlir::None, functionRef, functionArgs);
    }
    //  break;
    //}
    //Todo Insert function call. Use blockArgs is input args, if function has args.
}

// No recursion. Dependencies. Does NOT require addressMap insertion.
void MLIRGenerator::generateMLIR(std::shared_ptr<IR::Operations::CompareOperation> compareOp,
                                 std::unordered_map<std::string, mlir::Value>& blockArgs) {
    printf("CompareOperation identifier: %s\n", compareOp->getIdentifier().c_str());
    printf("CompareOperation first BlockArg name: %s\n", blockArgs.begin()->first.c_str());
    blockArgs.emplace(std::pair{compareOp->getIdentifier(),
                                builder->create<mlir::arith::CmpIOp>(getNameLoc("comparison"),
                                                                     convertToMLIRComparison(compareOp->getComparator()),
                                                                     blockArgs[compareOp->getLeftArgName()],
                                                                     blockArgs[compareOp->getRightArgName()])});
}

// No recursion. Dependencies. Does NOT require addressMap insertion.
void MLIRGenerator::generateMLIR(std::shared_ptr<IR::Operations::IfOperation> ifOp,
                                 std::unordered_map<std::string, mlir::Value>& frame) {

    auto targetBlock = ifOp->getThenBranchBlock();
    auto targetBlockArgs = ifOp->getThenBlockArgs();
    std::vector<mlir::Value> trueBlockArgs;
    mlir::Block* trueBlock;
    {
        auto parentBlockInsertionPoint = builder->saveInsertionPoint();
        trueBlock = builder->createBlock(builder->getBlock()->getParent());
        blockMapping[targetBlock->getIdentifier()] = trueBlock;
        for (auto headBlockHeadTypes : targetBlock->getInputArgTypes()) {
            trueBlock->addArgument(getMLIRType(headBlockHeadTypes), getNameLoc("arg"));
        }
        std::unordered_map<std::string, mlir::Value> loopHeadFrame;
        for (uint32_t i = 0; i < targetBlockArgs.size(); i++) {

            loopHeadFrame[targetBlock->getInputArgs()[i]] = trueBlock->getArgument(i);
        }
        generateMLIR(targetBlock, loopHeadFrame);
        builder->restoreInsertionPoint(parentBlockInsertionPoint);

        for (auto headBlockArgs : targetBlockArgs) {
            trueBlockArgs.push_back(frame[headBlockArgs]);
        }
    }

    targetBlock = ifOp->getElseBranchBlock();
    targetBlockArgs = ifOp->getElseBlockArgs();

    std::vector<mlir::Value> elseBlockArgs;
    mlir::Block* elseBlock;
    {
        auto parentBlockInsertionPoint = builder->saveInsertionPoint();
        elseBlock = builder->createBlock(builder->getBlock()->getParent());
        blockMapping[targetBlock->getIdentifier()] = elseBlock;
        for (auto headBlockHeadTypes : targetBlock->getInputArgTypes()) {
            elseBlock->addArgument(getMLIRType(headBlockHeadTypes), getNameLoc("arg"));
        }
        std::unordered_map<std::string, mlir::Value> loopHeadFrame;
        for (uint32_t i = 0; i < targetBlockArgs.size(); i++) {
            loopHeadFrame[targetBlock->getInputArgs()[i]] = elseBlock->getArgument(i);
        }
        generateMLIR(targetBlock, loopHeadFrame);
        builder->restoreInsertionPoint(parentBlockInsertionPoint);

        for (auto headBlockArgs : targetBlockArgs) {
            elseBlockArgs.push_back(frame[headBlockArgs]);
        }
    }

    builder->create<mlir::CondBranchOp>(getNameLoc("branch"),
                                        frame[ifOp->getBoolArgName()],
                                        trueBlock,
                                        trueBlockArgs,
                                        elseBlock,
                                        elseBlockArgs);

    return;

    auto ifOpTerminatorOp =
        findLastTerminatorOp(ifOp->getThenBranchBlock(),
                             ifOp->getThenBranchBlock()->getScopeLevel() - 1);// Todo change to ifOp->getElseBranchBlock()
    auto afterIfOpBlock = (ifOpTerminatorOp->getOperationType() == IR::Operations::Operation::BranchOp)
        ? std::static_pointer_cast<IR::Operations::BranchOperation>(ifOpTerminatorOp)->getNextBlock()
        : std::static_pointer_cast<IR::Operations::IfOperation>(ifOpTerminatorOp)->getElseBranchBlock();

    std::vector<mlir::Type> mlirIfOpResultTypes;
    for (auto afterIfBlockArgType : afterIfOpBlock->getInputArgTypes()) {
        mlirIfOpResultTypes.push_back(getMLIRType(afterIfBlockArgType));
    }
    auto mlirIfOp =
        builder->create<mlir::scf::IfOp>(getNameLoc("ifOperation"), mlirIfOpResultTypes, frame[ifOp->getBoolArgName()], true);
    auto currentInsertionPoint = builder->saveInsertionPoint();

    // generate code for true case
    builder->setInsertionPointToStart(mlirIfOp.getThenBodyBuilder().getInsertionBlock());
    MLIRGenerator::generateIfCase(mlirIfOp, ifOp, ifOp->getThenBranchBlock(), ifOp->getThenBlockArgs(), ifOpTerminatorOp, frame);
    builder->setInsertionPointToStart(mlirIfOp.getElseBodyBuilder().getInsertionBlock());
    MLIRGenerator::generateIfCase(mlirIfOp, ifOp, ifOp->getElseBranchBlock(), ifOp->getElseBlockArgs(), ifOpTerminatorOp, frame);

    // generate code for control flow merge (the block which combines the true and the false case again.
    {
        builder->restoreInsertionPoint(currentInsertionPoint);
        std::unordered_map<std::string, mlir::Value> subFrame;
        for (uint32_t i = 0; i < afterIfOpBlock->getInputArgs().size(); i++) {
            subFrame[afterIfOpBlock->getInputArgs()[i]] = mlirIfOp.getResult(i);
        }
        generateMLIR(afterIfOpBlock, subFrame);
    }
    /*
    // Find next block that is on the same or on a higher scopeLevel than ifOp and get its input args.
    auto ifOpTerminatorOp =
        findLastTerminatorOp(ifOp->getThenBranchBlock(),
                             ifOp->getThenBranchBlock()->getScopeLevel() - 1);// Todo change to ifOp->getElseBranchBlock()
    IR::BasicBlockPtr afterIfOpBlock = (ifOpTerminatorOp->getOperationType() == IR::Operations::Operation::BranchOp)
        ? std::static_pointer_cast<IR::Operations::BranchOperation>(ifOpTerminatorOp)->getNextBlock()
        : std::static_pointer_cast<IR::Operations::IfOperation>(ifOpTerminatorOp)->getElseBranchBlock();
    std::vector<std::string> afterIfOpBlockArgs = afterIfOpBlock->getInputArgs();

    // Create MLIR-IF-Operation
    mlir::scf::IfOp mlirIfOp;
    auto currentInsertionPoint = builder->saveInsertionPoint();
    std::vector<mlir::Type> mlirIfOpResultTypes;
    // Register all blockArgs for the IfOperation as output/yield args. All values that are not modified, will just be returned.
    for (auto afterIfBlockArgType : afterIfOpBlock->getInputArgTypes()) {
        mlirIfOpResultTypes.push_back(getMLIRType(afterIfBlockArgType));
    }
    mlirIfOp =
        builder->create<mlir::scf::IfOp>(getNameLoc("ifOperation"), mlirIfOpResultTypes, blockArgs[ifOp->getBoolArgName()], true);

    // IF case -> Change scope to Then Branch. Finish by setting obligatory YieldOp for THEN case.
    builder->setInsertionPointToStart(mlirIfOp.getThenBodyBuilder().getInsertionBlock());
    std::unordered_map<std::string, mlir::Value> ifBlockArgs = blockArgs;
    generateMLIR(ifOp->getThenBranchBlock(), ifBlockArgs);

    std::vector<mlir::Value> ifYieldOps;
    for (auto afterIfBlockArg : afterIfOpBlockArgs) {
        ifYieldOps.push_back(ifBlockArgs[afterIfBlockArg]);
    }
    builder->create<mlir::scf::YieldOp>(getNameLoc("ifYield"), ifYieldOps);

    // ELSE case -> Change scope to ELSE Branch. Finish by setting obligatory YieldOp for ELSE case.
    builder->setInsertionPointToStart(mlirIfOp.getElseBodyBuilder().getInsertionBlock());
    std::unordered_map<std::string, mlir::Value> elseBlockArgs = blockArgs;

    std::vector<mlir::Value> elseYieldOps;
    if (ifOp->getElseBranchBlock() != nullptr) {
        generateMLIR(ifOp->getElseBranchBlock(), elseBlockArgs);
        for (auto afterIfBlockArg : afterIfOpBlockArgs) {//Todo MUST use elseOpTerminator outArgs
            elseYieldOps.push_back(elseBlockArgs[afterIfBlockArg]);
        }
        builder->create<mlir::scf::YieldOp>(getNameLoc("elseYield"), elseYieldOps);
    } else {
        builder->create<mlir::scf::YieldOp>(getNameLoc("copiedElseYield"), ifYieldOps);
    }

    // Back to IfOp scope. Get all yielded result values from IfOperation, write to blockArgs and proceed.
    builder->restoreInsertionPoint(currentInsertionPoint);
    for (int i = 0; i < (int) afterIfOpBlock->getInputArgs().size(); ++i) {
        blockArgs[afterIfOpBlock->getInputArgs().at(i)] = mlirIfOp.getResult(i);
    }
    if (afterIfOpBlock->getScopeLevel() == (ifOp->getThenBranchBlock()->getScopeLevel()) - 1) {
        generateMLIR(afterIfOpBlock, blockArgs);
    }
     */
}

//==--------------------- Dummy OPERATIONS ----------------------==//
// No recursion. Dependencies. Does NOT require addressMap insertion.
void MLIRGenerator::generateMLIR(std::shared_ptr<IR::Operations::BranchOperation> branchOp,
                                 std::unordered_map<std::string, mlir::Value>& blockArgs) {
    printf("BranchOperation.getNextBlock.getIdentifier: %s\n", branchOp->getNextBlock()->getIdentifier().c_str());
    printf("BranchOperation first BlockArg name: %s\n", blockArgs.begin()->first.c_str());
}

std::vector<std::string> MLIRGenerator::getJitProxyFunctionSymbols() { return jitProxyFunctionSymbols; }
std::vector<llvm::JITTargetAddress> MLIRGenerator::getJitProxyTargetAddresses() { return jitProxyFunctionTargetAddresses; }
void MLIRGenerator::generateIfCase(mlir::scf::IfOp,
                                   std::shared_ptr<IR::Operations::IfOperation>,
                                   IR::BasicBlockPtr caseBlock,
                                   std::vector<std::string> caseBlockArguments,
                                   IR::Operations::OperationPtr,
                                   std::unordered_map<std::string, mlir::Value>& frame) {
    //Change scope to Then Branch. Finish by setting obligatory YieldOp for THEN case.
    auto ifOpTerminatorOp = findLastTerminatorOp(caseBlock, caseBlock->getScopeLevel() - 1);
    std::unordered_map<std::string, mlir::Value> trueBlockFrame;
    for (uint32_t i = 0; i < caseBlock->getInputArgs().size(); i++) {
        auto parentFrameArgIdentifier = caseBlockArguments[i];
        auto trueCaseInputArgIdentifier = caseBlock->getInputArgs()[i];
        // each block introduces new argument names, whereby the index establishes the mapping between the child and the parent frame.
        trueBlockFrame[trueCaseInputArgIdentifier] = frame[parentFrameArgIdentifier];
    }
    generateMLIR(caseBlock, trueBlockFrame);

    auto nextBlockArgs = std::static_pointer_cast<IR::Operations::BranchOperation>(ifOpTerminatorOp)->getNextBlockArgs();

    std::vector<mlir::Value> ifYieldOps;
    for (auto afterIfBlockArg : nextBlockArgs) {
        ifYieldOps.push_back(trueBlockFrame[afterIfBlockArg]);
    }
    builder->create<mlir::scf::YieldOp>(getNameLoc("ifYield"), ifYieldOps);
}
}// namespace NES::ExecutionEngine::Experimental::MLIR