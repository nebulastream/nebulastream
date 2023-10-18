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

#include <Nautilus/Backends/MLIR/MLIRLoweringProvider.hpp>
#include <Nautilus/IR/Types/FloatStamp.hpp>
#include <Nautilus/IR/Types/IntegerStamp.hpp>
#include <Nautilus/IR/Types/StampFactory.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <llvm/ADT/StringRef.h>
#include <llvm/Transforms/Utils/Cloning.h>
#include <mlir/Dialect/Arith/IR/Arith.h>
#include <mlir/Dialect/ControlFlow/IR/ControlFlow.h>
#include <mlir/Dialect/ControlFlow/IR/ControlFlowOps.h>
#include <mlir/Dialect/Func/IR/FuncOps.h>
#include <mlir/Dialect/Func/Transforms/FuncConversions.h>
#include <mlir/Dialect/LLVMIR/LLVMDialect.h>
#include <mlir/Dialect/LLVMIR/LLVMTypes.h>
#include <mlir/Dialect/SCF/IR/SCF.h>
#include <mlir/IR/Attributes.h>
#include <mlir/IR/Builders.h>
#include <mlir/IR/BuiltinAttributes.h>
#include <mlir/IR/BuiltinTypes.h>
#include <mlir/IR/Location.h>
#include <mlir/IR/Operation.h>
#include <mlir/IR/PatternMatch.h>
#include <mlir/IR/TypeRange.h>
#include <mlir/IR/Value.h>
#include <mlir/IR/Verifier.h>
#include <mlir/Support/LLVM.h>

namespace NES::Nautilus::Backends::MLIR {

//==-----------------------==//
//==-- UTILITY FUNCTIONS --==//
//==-----------------------==//
mlir::Type MLIRLoweringProvider::getMLIRType(IR::Types::StampPtr type) {
    if (type->isVoid()) {
        return mlir::LLVM::LLVMVoidType::get(context);
    } else if (type->isBoolean()) {
        return builder->getIntegerType(1);
    } else if (type->isInteger()) {
        auto value = cast<IR::Types::IntegerStamp>(type);
        return builder->getIntegerType(value->getNumberOfBits());
    } else if (type->isFloat()) {
        auto value = cast<IR::Types::FloatStamp>(type);
        switch (value->getBitWidth()) {
            case IR::Types::FloatStamp::BitWidth::F32: return mlir::Float32Type::get(context);
            case IR::Types::FloatStamp::BitWidth::F64: return mlir::Float64Type::get(context);
        }
    } else if (type->isAddress()) {
        return mlir::LLVM::LLVMPointerType::get(builder->getI8Type());
    }
    NES_THROW_RUNTIME_ERROR("No matching type for stamp " << type);
}

std::vector<mlir::Type> MLIRLoweringProvider::getMLIRType(std::vector<IR::Operations::OperationPtr> types) {
    std::vector<mlir::Type> resultTypes;
    for (auto& type : types) {
        resultTypes.push_back(getMLIRType(type->getStamp()));
    }
    return resultTypes;
}

mlir::Value MLIRLoweringProvider::getConstInt(const std::string& location, IR::Types::StampPtr stamp, int64_t value) {
    auto type = getMLIRType(stamp);
    return builder->create<mlir::arith::ConstantOp>(getNameLoc(location), type, builder->getIntegerAttr(type, value));
}

mlir::Value MLIRLoweringProvider::getConstBool(const std::string& location, bool value) {
    return builder->create<mlir::LLVM::ConstantOp>(getNameLoc(location),
                                                   builder->getI1Type(),
                                                   builder->getIntegerAttr(builder->getIndexType(), value));
}

// Todo Issue #3004: Currently, we are simply adding 'Query_1' as the FileLineLoc name. Moreover,
//      the provided 'name' often is not meaningful either.
mlir::Location MLIRLoweringProvider::getNameLoc(const std::string& name) {
    auto baseLocation = mlir::FileLineColLoc::get(builder->getStringAttr("Query_1"), 0, 0);
    return mlir::NameLoc::get(builder->getStringAttr(name), baseLocation);
}

mlir::arith::CmpIPredicate convertToIntMLIRComparison(IR::Operations::CompareOperation::Comparator comparisonType,
                                                      IR::Types::StampPtr& stamp) {
    auto integerStamp = std::dynamic_pointer_cast<IR::Types::IntegerStamp>(stamp);
    if (integerStamp->isSigned()) {
        switch (comparisonType) {
            case (IR::Operations::CompareOperation::Comparator::EQ): return mlir::arith::CmpIPredicate::eq;
            case (IR::Operations::CompareOperation::Comparator::NE): return mlir::arith::CmpIPredicate::ne;
            case (IR::Operations::CompareOperation::Comparator::LT): return mlir::arith::CmpIPredicate::slt;
            case (IR::Operations::CompareOperation::Comparator::LE): return mlir::arith::CmpIPredicate::sle;
            case (IR::Operations::CompareOperation::Comparator::GT): return mlir::arith::CmpIPredicate::sgt;
            case (IR::Operations::CompareOperation::Comparator::GE): return mlir::arith::CmpIPredicate::sge;
            default: return mlir::arith::CmpIPredicate::slt;
        }
    } else {
        switch (comparisonType) {
            case (IR::Operations::CompareOperation::Comparator::EQ): return mlir::arith::CmpIPredicate::eq;
            case (IR::Operations::CompareOperation::Comparator::NE): return mlir::arith::CmpIPredicate::ne;
            case (IR::Operations::CompareOperation::Comparator::LT): return mlir::arith::CmpIPredicate::ult;
            case (IR::Operations::CompareOperation::Comparator::LE): return mlir::arith::CmpIPredicate::ule;
            case (IR::Operations::CompareOperation::Comparator::GT): return mlir::arith::CmpIPredicate::ugt;
            case (IR::Operations::CompareOperation::Comparator::GE): return mlir::arith::CmpIPredicate::uge;
            default: return mlir::arith::CmpIPredicate::ult;
        }
    }
}
mlir::arith::CmpFPredicate convertToFloatMLIRComparison(IR::Operations::CompareOperation::Comparator comparisonType) {
    switch (comparisonType) {
        // the U in U(LT/LE/..) stands for unordered, not unsigned! Float comparisons are always signed.
        case (IR::Operations::CompareOperation::Comparator::LT): return mlir::arith::CmpFPredicate::OLT;
        case (IR::Operations::CompareOperation::Comparator::LE): return mlir::arith::CmpFPredicate::OLE;
        case (IR::Operations::CompareOperation::Comparator::EQ): return mlir::arith::CmpFPredicate::OEQ;
        case (IR::Operations::CompareOperation::Comparator::GT): return mlir::arith::CmpFPredicate::OGT;
        case (IR::Operations::CompareOperation::Comparator::GE): return mlir::arith::CmpFPredicate::OGE;
        case (IR::Operations::CompareOperation::Comparator::NE): return mlir::arith::CmpFPredicate::ONE;
        default: return mlir::arith::CmpFPredicate::OLT;
    }
}

mlir::FlatSymbolRefAttr MLIRLoweringProvider::insertExternalFunction(const std::string& name,
                                                                     void* functionPtr,
                                                                     mlir::Type resultType,
                                                                     std::vector<mlir::Type> argTypes,
                                                                     bool varArgs) {
    // Create function arg & result types (currently only int for result).
    mlir::LLVM::LLVMFunctionType llvmFnType = mlir::LLVM::LLVMFunctionType::get(resultType, argTypes, varArgs);

    // The InsertionGuard saves the current insertion point (IP) and restores it after scope is left.
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

//==---------------------------------==//
//==-- MAIN WORK - Generating MLIR --==//
//==---------------------------------==//
MLIRLoweringProvider::MLIRLoweringProvider(mlir::MLIRContext& context) : context(&context) {
    // Create builder object, which helps to generate MLIR. Create Module, which contains generated MLIR.
    builder = std::make_unique<mlir::OpBuilder>(&context);
    builder->getContext()->loadDialect<mlir::cf::ControlFlowDialect>();
    builder->getContext()->loadDialect<mlir::LLVM::LLVMDialect>();
    builder->getContext()->loadDialect<mlir::func::FuncDialect>();
    builder->getContext()->loadDialect<mlir::scf::SCFDialect>();
    this->theModule = mlir::ModuleOp::create(getNameLoc("module"));
    // Store InsertPoint for inserting globals such as Strings or TupleBuffers.
    globalInsertPoint = new mlir::RewriterBase::InsertPoint(theModule.getBody(), theModule.begin());
};

MLIRLoweringProvider::~MLIRLoweringProvider() {
    NES_DEBUG("~MLIRLoweringProvider");
    delete globalInsertPoint;
}

mlir::OwningOpRef<mlir::ModuleOp> MLIRLoweringProvider::generateModuleFromIR(std::shared_ptr<IR::IRGraph> ir) {
    ValueFrame firstFrame;
    generateMLIR(ir->getRootOperation(), firstFrame);
    // If MLIR module creation is incorrect, gracefully emit error message, return nullptr, and continue.
    if (failed(mlir::verify(theModule))) {
        theModule.emitError("module verification error");
        return nullptr;
    }
    return theModule;
}

void MLIRLoweringProvider::generateMLIR(IR::BasicBlockPtr basicBlock, ValueFrame& frame) {
    for (const auto& operation : basicBlock->getOperations()) {
        generateMLIR(operation, frame);
    }
}

void MLIRLoweringProvider::generateMLIR(const IR::Operations::OperationPtr& operation, ValueFrame& frame) {
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
        case IR::Operations::Operation::OperationType::ModOp:
            generateMLIR(std::static_pointer_cast<IR::Operations::ModOperation>(operation), frame);
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
        case IR::Operations::Operation::OperationType::OrOp:
            generateMLIR(std::static_pointer_cast<IR::Operations::OrOperation>(operation), frame);
            break;
        case IR::Operations::Operation::OperationType::AndOp:
            generateMLIR(std::static_pointer_cast<IR::Operations::AndOperation>(operation), frame);
            break;
        case IR::Operations::Operation::OperationType::BitWiseAnd:
            generateMLIR(std::static_pointer_cast<IR::Operations::BitWiseAndOperation>(operation), frame);
            break;
        case IR::Operations::Operation::OperationType::BitWiseOr:
            generateMLIR(std::static_pointer_cast<IR::Operations::BitWiseOrOperation>(operation), frame);
            break;
        case IR::Operations::Operation::OperationType::BitWiseXor:
            generateMLIR(std::static_pointer_cast<IR::Operations::BitWiseXorOperation>(operation), frame);
            break;
        case IR::Operations::Operation::OperationType::BitWiseLeftShift:
            generateMLIR(std::static_pointer_cast<IR::Operations::BitWiseLeftShiftOperation>(operation), frame);
            break;
        case IR::Operations::Operation::OperationType::BitWiseRightShift:
            generateMLIR(std::static_pointer_cast<IR::Operations::BitWiseRightShiftOperation>(operation), frame);
            break;
        case IR::Operations::Operation::OperationType::NegateOp:
            generateMLIR(std::static_pointer_cast<IR::Operations::NegateOperation>(operation), frame);
            break;
        case IR::Operations::Operation::OperationType::CastOp:
            generateMLIR(std::static_pointer_cast<IR::Operations::CastOperation>(operation), frame);
            break;
        case IR::Operations::Operation::OperationType::ConstBooleanOp:
            generateMLIR(std::static_pointer_cast<IR::Operations::ConstBooleanOperation>(operation), frame);
            break;
        default: NES_NOT_IMPLEMENTED();
    }
}

void MLIRLoweringProvider::generateMLIR(std::shared_ptr<IR::Operations::NegateOperation> negateOperation, ValueFrame& frame) {
    auto input = frame.getValue(negateOperation->getInput()->getIdentifier());
    auto negate = builder->create<mlir::arith::CmpIOp>(getNameLoc("comparison"),
                                                       mlir::arith::CmpIPredicate::eq,
                                                       input,
                                                       getConstBool("bool", false));
    frame.setValue(negateOperation->getIdentifier(), negate);
}

void MLIRLoweringProvider::generateMLIR(std::shared_ptr<IR::Operations::OrOperation> orOperation, ValueFrame& frame) {
    auto leftInput = frame.getValue(orOperation->getLeftInput()->getIdentifier());
    auto rightInput = frame.getValue(orOperation->getRightInput()->getIdentifier());
    auto mlirOrOp = builder->create<mlir::LLVM::OrOp>(getNameLoc("binOpResult"), leftInput, rightInput);
    frame.setValue(orOperation->getIdentifier(), mlirOrOp);
}

void MLIRLoweringProvider::generateMLIR(std::shared_ptr<IR::Operations::AndOperation> andOperation, ValueFrame& frame) {
    auto leftInput = frame.getValue(andOperation->getLeftInput()->getIdentifier());
    auto rightInput = frame.getValue(andOperation->getRightInput()->getIdentifier());
    auto mlirAndOp = builder->create<mlir::LLVM::AndOp>(getNameLoc("binOpResult"), leftInput, rightInput);
    frame.setValue(andOperation->getIdentifier(), mlirAndOp);
}

void MLIRLoweringProvider::generateMLIR(std::shared_ptr<IR::Operations::BitWiseAndOperation> andOperation, ValueFrame& frame) {
    auto leftInput = frame.getValue(andOperation->getLeftInput()->getIdentifier());
    auto rightInput = frame.getValue(andOperation->getRightInput()->getIdentifier());
    auto mlirBitwiseAndOp = builder->create<mlir::arith::AndIOp>(getNameLoc("binOpResult"), leftInput, rightInput);
    frame.setValue(andOperation->getIdentifier(), mlirBitwiseAndOp);
}

void MLIRLoweringProvider::generateMLIR(std::shared_ptr<IR::Operations::BitWiseOrOperation> orOperation, ValueFrame& frame) {
    auto leftInput = frame.getValue(orOperation->getLeftInput()->getIdentifier());
    auto rightInput = frame.getValue(orOperation->getRightInput()->getIdentifier());
    auto mlirBitwiseOrOp = builder->create<mlir::arith::OrIOp>(getNameLoc("binOpResult"), leftInput, rightInput);
    frame.setValue(orOperation->getIdentifier(), mlirBitwiseOrOp);
}

void MLIRLoweringProvider::generateMLIR(std::shared_ptr<IR::Operations::BitWiseXorOperation> xorOperation, ValueFrame& frame) {
    auto leftInput = frame.getValue(xorOperation->getLeftInput()->getIdentifier());
    auto rightInput = frame.getValue(xorOperation->getRightInput()->getIdentifier());
    auto mlirBitwiseXorOp = builder->create<mlir::arith::XOrIOp>(getNameLoc("binOpResult"), leftInput, rightInput);
    frame.setValue(xorOperation->getIdentifier(), mlirBitwiseXorOp);
}

void MLIRLoweringProvider::generateMLIR(std::shared_ptr<IR::Operations::BitWiseLeftShiftOperation> leftShiftOperation,
                                        ValueFrame& frame) {
    auto leftInput = frame.getValue(leftShiftOperation->getLeftInput()->getIdentifier());
    auto rightInput = frame.getValue(leftShiftOperation->getRightInput()->getIdentifier());
    auto mlirBitwiseLeftShiftOp = builder->create<mlir::arith::ShLIOp>(getNameLoc("binOpResult"), leftInput, rightInput);
    frame.setValue(leftShiftOperation->getIdentifier(), mlirBitwiseLeftShiftOp);
}

void MLIRLoweringProvider::generateMLIR(std::shared_ptr<IR::Operations::BitWiseRightShiftOperation> rightShiftOperation,
                                        ValueFrame& frame) {
    auto leftInput = frame.getValue(rightShiftOperation->getLeftInput()->getIdentifier());
    auto rightInput = frame.getValue(rightShiftOperation->getRightInput()->getIdentifier());
    auto mlirBitwiseRightShiftOp = builder->create<mlir::arith::ShRSIOp>(getNameLoc("binOpResult"), leftInput, rightInput);
    frame.setValue(rightShiftOperation->getIdentifier(), mlirBitwiseRightShiftOp);
}

void MLIRLoweringProvider::generateMLIR(std::shared_ptr<IR::Operations::FunctionOperation> functionOp, ValueFrame& frame) {
    // Generate execute function. Set input/output types and get its entry block.
    llvm::SmallVector<mlir::Type, 4> inputTypes(0);
    for (auto inputArg : functionOp->getFunctionBasicBlock()->getArguments()) {
        inputTypes.emplace_back(getMLIRType(inputArg->getStamp()));
    }
    llvm::SmallVector<mlir::Type, 4> outputTypes(1, getMLIRType(functionOp->getOutputArg()));
    auto functionInOutTypes = builder->getFunctionType(inputTypes, outputTypes);

    auto mlirFunction = builder->create<mlir::func::FuncOp>(getNameLoc("EntryPoint"), functionOp->getName(), functionInOutTypes);

    // Avoid function name mangling.
    mlirFunction->setAttr("llvm.emit_c_interface", mlir::UnitAttr::get(context));
    mlirFunction.addEntryBlock();

    // Set InsertPoint to beginning of the execute function.
    builder->setInsertionPointToStart(&mlirFunction.getBody().front());

    // Store references to function args in the valueMap map.
    auto valueMapIterator = mlirFunction.args_begin();
    for (int i = 0; i < (int) functionOp->getFunctionBasicBlock()->getArguments().size(); ++i) {
        frame.setValue(functionOp->getFunctionBasicBlock()->getArguments().at(i)->getIdentifier(), valueMapIterator[i]);
    }

    // Generate MLIR for operations in function body (BasicBlock).
    generateMLIR(functionOp->getFunctionBasicBlock(), frame);

    theModule.push_back(mlirFunction);
}

void MLIRLoweringProvider::generateMLIR(std::shared_ptr<IR::Operations::LoopOperation> loopOp, ValueFrame& frame) {
    std::vector<mlir::Value> mlirTargetBlockArguments;
    auto loopHeadBlock = loopOp->getLoopHeadBlock();
    for (auto targetBlockArgument : loopHeadBlock.getArguments()) {
        mlirTargetBlockArguments.push_back(frame.getValue(targetBlockArgument->getIdentifier()));
    }
    auto* mlirTargetBlock = generateBasicBlock(loopHeadBlock, frame);
    builder->create<mlir::cf::BranchOp>(getNameLoc("branch"), mlirTargetBlock, mlirTargetBlockArguments);
}

//==--------------------------==//
//==-- MEMORY (LOAD, STORE) --==//
//==--------------------------==//
void MLIRLoweringProvider::generateMLIR(std::shared_ptr<IR::Operations::AddressOperation> addressOp, ValueFrame& frame) {
    mlir::Value recordOffset;
    if (addressOp->getRecordIdxName() != "") {
        recordOffset = builder->create<mlir::LLVM::MulOp>(
            getNameLoc("recordOffset"),
            frame.getValue(addressOp->getRecordIdxName()),
            getConstInt("1", IR::Types::StampFactory::createInt64Stamp(), addressOp->getRecordWidthInBytes()));
    } else {
        recordOffset = getConstInt("0", IR::Types::StampFactory::createInt64Stamp(), 0);
    }
    mlir::Value fieldOffset = builder->create<mlir::LLVM::AddOp>(
        getNameLoc("fieldOffset"),
        recordOffset,
        getConstInt("1", IR::Types::StampFactory::createInt64Stamp(), addressOp->getFieldOffsetInBytes()));
    // Return I8* to first byte of field data
    mlir::Value elementAddress = builder->create<mlir::LLVM::GEPOp>(getNameLoc("fieldAccess"),
                                                                    mlir::LLVM::LLVMPointerType::get(builder->getI8Type()),
                                                                    frame.getValue(addressOp->getAddressSourceName()),
                                                                    mlir::ArrayRef<mlir::Value>({fieldOffset}));
    auto mlirBitcast =
        builder->create<mlir::LLVM::BitcastOp>(getNameLoc("Address Bitcasted"),
                                               mlir::LLVM::LLVMPointerType::get(getMLIRType(addressOp->getStamp())),
                                               elementAddress);
    frame.setValue(addressOp->getIdentifier(), mlirBitcast);
}

void MLIRLoweringProvider::generateMLIR(std::shared_ptr<IR::Operations::LoadOperation> loadOp, ValueFrame& frame) {

    auto address = frame.getValue(loadOp->getAddress()->getIdentifier());

    auto bitcast = builder->create<mlir::LLVM::BitcastOp>(getNameLoc("Bitcasted address"),
                                                          mlir::LLVM::LLVMPointerType::get(getMLIRType(loadOp->getStamp())),
                                                          address);
    auto mlirLoadOp = builder->create<mlir::LLVM::LoadOp>(getNameLoc("loadedValue"), bitcast);
    frame.setValue(loadOp->getIdentifier(), mlirLoadOp);
}

void MLIRLoweringProvider::generateMLIR(std::shared_ptr<IR::Operations::ConstIntOperation> constIntOp, ValueFrame& frame) {
    if (!frame.contains(constIntOp->getIdentifier())) {
        frame.setValue(constIntOp->getIdentifier(), getConstInt("ConstantOp", constIntOp->getStamp(), constIntOp->getValue()));
    } else {
        frame.setValue(constIntOp->getIdentifier(), getConstInt("ConstantOp", constIntOp->getStamp(), constIntOp->getValue()));
    }
}

void MLIRLoweringProvider::generateMLIR(std::shared_ptr<IR::Operations::ConstFloatOperation> constFloatOp, ValueFrame& frame) {
    if (auto floatStamp = cast_if<IR::Types::FloatStamp>(constFloatOp->getStamp().get())) {
        auto floatType =
            (floatStamp->getBitWidth() == IR::Types::FloatStamp::BitWidth::F32) ? builder->getF32Type() : builder->getF64Type();
        frame.setValue(constFloatOp->getIdentifier(),
                       builder->create<mlir::LLVM::ConstantOp>(getNameLoc("constantFloat"),
                                                               floatType,
                                                               builder->getFloatAttr(floatType, constFloatOp->getValue())));
    }
}

//==---------------------------==//
//==-- ARITHMETIC OPERATIONS --==//
//==---------------------------==//
void MLIRLoweringProvider::generateMLIR(std::shared_ptr<IR::Operations::AddOperation> addOp, ValueFrame& frame) {
    auto leftInput = frame.getValue(addOp->getLeftInput()->getIdentifier());
    auto rightInput = frame.getValue(addOp->getRightInput()->getIdentifier());
    if (addOp->getLeftInput()->getStamp()->isAddress()) {
        // if we add something to a ptr we have to use a llvm getelementptr
        mlir::Value elementAddress = builder->create<mlir::LLVM::GEPOp>(getNameLoc("fieldAccess"),
                                                                        mlir::LLVM::LLVMPointerType::get(builder->getI8Type()),
                                                                        leftInput,
                                                                        mlir::ArrayRef<mlir::Value>({rightInput}));
        frame.setValue(addOp->getIdentifier(), elementAddress);

    } else if (addOp->getStamp()->isFloat()) {
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

void MLIRLoweringProvider::generateMLIR(std::shared_ptr<IR::Operations::SubOperation> subIntOp, ValueFrame& frame) {
    auto leftInput = frame.getValue(subIntOp->getLeftInput()->getIdentifier());
    auto rightInput = frame.getValue(subIntOp->getRightInput()->getIdentifier());
    if (subIntOp->getStamp()->isFloat()) {
        auto mlirSubOp =
            builder->create<mlir::LLVM::FSubOp>(getNameLoc("binOpResult"),
                                                leftInput,
                                                rightInput,
                                                mlir::LLVM::FastmathFlagsAttr::get(context, mlir::LLVM::FastmathFlags::fast));
        frame.setValue(subIntOp->getIdentifier(), mlirSubOp);
    } else {
        auto mlirSubOp = builder->create<mlir::LLVM::SubOp>(getNameLoc("binOpResult"), leftInput, rightInput);
        frame.setValue(subIntOp->getIdentifier(), mlirSubOp);
    }
}

void MLIRLoweringProvider::generateMLIR(std::shared_ptr<IR::Operations::MulOperation> mulOp, ValueFrame& frame) {
    auto leftInput = frame.getValue(mulOp->getLeftInput()->getIdentifier());
    auto rightInput = frame.getValue(mulOp->getRightInput()->getIdentifier());
    auto resultType = leftInput.getType();
    if (mulOp->getStamp()->isFloat()) {
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

void MLIRLoweringProvider::generateMLIR(std::shared_ptr<IR::Operations::DivOperation> divIntOp, ValueFrame& frame) {
    auto leftInput = frame.getValue(divIntOp->getLeftInput()->getIdentifier());
    auto rightInput = frame.getValue(divIntOp->getRightInput()->getIdentifier());
    auto resultType = leftInput.getType();
    if (divIntOp->getStamp()->isFloat()) {
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

void MLIRLoweringProvider::generateMLIR(std::shared_ptr<IR::Operations::ModOperation> modIntOp, ValueFrame& frame) {
    auto leftInput = frame.getValue(modIntOp->getLeftInput()->getIdentifier());
    auto rightInput = frame.getValue(modIntOp->getRightInput()->getIdentifier());
    auto resultType = leftInput.getType();
    if (modIntOp->getStamp()->isInteger()) {
        if (resultType.isSignedInteger()) {
            auto mlirModOp = builder->create<mlir::LLVM::SRemOp>(getNameLoc("binOpResult"), resultType, leftInput, rightInput);
            frame.setValue(modIntOp->getIdentifier(), mlirModOp);
        } else {
            auto mlirModOp = builder->create<mlir::LLVM::URemOp>(getNameLoc("binOpResult"), resultType, leftInput, rightInput);
            frame.setValue(modIntOp->getIdentifier(), mlirModOp);
        }
    } else {
        NES_NOT_IMPLEMENTED();
    }
}

void MLIRLoweringProvider::generateMLIR(std::shared_ptr<IR::Operations::StoreOperation> storeOp, ValueFrame& frame) {
    auto value = frame.getValue(storeOp->getValue()->getIdentifier());
    auto address = frame.getValue(storeOp->getAddress()->getIdentifier());
    auto bitcast = builder->create<mlir::LLVM::BitcastOp>(getNameLoc("Address Bitcasted"),
                                                          mlir::LLVM::LLVMPointerType::get(value.getType()),
                                                          address);
    builder->create<mlir::LLVM::StoreOp>(getNameLoc("outputStore"), value, bitcast);
}

void MLIRLoweringProvider::generateMLIR(std::shared_ptr<IR::Operations::ReturnOperation> returnOp, ValueFrame& frame) {
    // Insert return into 'execute' function block. This is the FINAL return.
    if (!returnOp->hasReturnValue()) {
        builder->create<mlir::LLVM::ReturnOp>(getNameLoc("return"), mlir::ValueRange());
    } else {
        builder->create<mlir::LLVM::ReturnOp>(getNameLoc("return"), frame.getValue(returnOp->getReturnValue()->getIdentifier()));
    }
}

void MLIRLoweringProvider::generateMLIR(std::shared_ptr<IR::Operations::ProxyCallOperation> proxyCallOp, ValueFrame& frame) {
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
    if (!proxyCallOp->getStamp()->isVoid()) {
        auto res = builder->create<mlir::LLVM::CallOp>(getNameLoc(proxyCallOp->getFunctionSymbol()),
                                                       getMLIRType(proxyCallOp->getStamp()),
                                                       functionRef,
                                                       functionArgs);
        frame.setValue(proxyCallOp->getIdentifier(), res.getResult());
    } else {
        builder->create<mlir::LLVM::CallOp>(builder->getUnknownLoc(), mlir::TypeRange(), functionRef, functionArgs);
    }
}

void MLIRLoweringProvider::generateMLIR(std::shared_ptr<IR::Operations::CompareOperation> compareOp, ValueFrame& frame) {
    auto leftStamp = compareOp->getLeftInput()->getStamp();
    auto rightStamp = compareOp->getRightInput()->getStamp();

    if ((leftStamp->isInteger() && leftStamp->isFloat()) || (leftStamp->isFloat() && rightStamp->isInteger())) {
        // Avoid comparing integer to float
        NES_THROW_RUNTIME_ERROR("Type missmatch: cannot compare " << leftStamp->toString() << " to " << rightStamp->toString());
    } else if (compareOp->getComparator() == IR::Operations::CompareOperation::EQ
               && compareOp->getLeftInput()->getStamp()->isAddress() && compareOp->getRightInput()->getStamp()->isInteger()) {
        // add null check
        auto null =
            builder->create<mlir::LLVM::NullOp>(getNameLoc("null"), mlir::LLVM::LLVMPointerType::get(builder->getI8Type()));
        auto cmpOp = builder->create<mlir::LLVM::ICmpOp>(getNameLoc("comparison"),
                                                         mlir::LLVM::ICmpPredicate::eq,
                                                         frame.getValue(compareOp->getLeftInput()->getIdentifier()),
                                                         null);
        frame.setValue(compareOp->getIdentifier(), cmpOp);
    } else if (compareOp->getComparator() == IR::Operations::CompareOperation::EQ
               && compareOp->getLeftInput()->getStamp()->isAddress() && compareOp->getRightInput()->getStamp()->isAddress()) {
        auto cmpOp = builder->create<mlir::LLVM::ICmpOp>(getNameLoc("comparison"),
                                                         mlir::LLVM::ICmpPredicate::eq,
                                                         frame.getValue(compareOp->getLeftInput()->getIdentifier()),
                                                         frame.getValue(compareOp->getRightInput()->getIdentifier()));
        frame.setValue(compareOp->getIdentifier(), cmpOp);
    } else if (leftStamp->isInteger() && rightStamp->isInteger()) {
        // handle integer
        auto cmpOp = builder->create<mlir::arith::CmpIOp>(getNameLoc("comparison"),
                                                          convertToIntMLIRComparison(compareOp->getComparator(), leftStamp),
                                                          frame.getValue(compareOp->getLeftInput()->getIdentifier()),
                                                          frame.getValue(compareOp->getRightInput()->getIdentifier()));
        frame.setValue(compareOp->getIdentifier(), cmpOp);
    } else if (leftStamp->isFloat() && rightStamp->isFloat()) {
        // handle float comparison
        auto cmpOp = builder->create<mlir::arith::CmpFOp>(getNameLoc("comparison"),
                                                          convertToFloatMLIRComparison(compareOp->getComparator()),
                                                          frame.getValue(compareOp->getLeftInput()->getIdentifier()),
                                                          frame.getValue(compareOp->getRightInput()->getIdentifier()));
        frame.setValue(compareOp->getIdentifier(), cmpOp);
    } else if (leftStamp->isBoolean() && rightStamp->isBoolean()) {
        // handle boolean comparison
        auto cmpOp = builder->create<mlir::arith::CmpIOp>(getNameLoc("comparison"),
                                                          mlir::arith::CmpIPredicate::eq,
                                                          frame.getValue(compareOp->getLeftInput()->getIdentifier()),
                                                          frame.getValue(compareOp->getRightInput()->getIdentifier()));
        frame.setValue(compareOp->getIdentifier(), cmpOp);
    } else {
        NES_THROW_RUNTIME_ERROR("Unknown type to compare: " << leftStamp->toString() << " and " << rightStamp->toString());
    }
}

void MLIRLoweringProvider::generateMLIR(std::shared_ptr<IR::Operations::IfOperation> ifOp, ValueFrame& frame) {
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
    builder->create<mlir::cf::CondBranchOp>(getNameLoc("branch"),
                                            frame.getValue(ifOp->getValue()->getIdentifier()),
                                            trueBlock,
                                            trueBlockArgs,
                                            elseBlock,
                                            elseBlockArgs);
}

void MLIRLoweringProvider::generateMLIR(std::shared_ptr<IR::Operations::BranchOperation> branchOp, ValueFrame& frame) {
    std::vector<mlir::Value> mlirTargetBlockArguments;
    for (auto targetBlockArgument : branchOp->getNextBlockInvocation().getArguments()) {
        mlirTargetBlockArguments.push_back(frame.getValue(targetBlockArgument->getIdentifier()));
    }
    auto* mlirTargetBlock = generateBasicBlock(branchOp->getNextBlockInvocation(), frame);
    builder->create<mlir::cf::BranchOp>(getNameLoc("branch"), mlirTargetBlock, mlirTargetBlockArguments);
}

mlir::Block* MLIRLoweringProvider::generateBasicBlock(IR::Operations::BasicBlockInvocation& blockInvocation, ValueFrame&) {
    auto targetBlock = blockInvocation.getBlock();
    // Check if the block already exists.
    if (blockMapping.contains(targetBlock->getIdentifier())) {
        return blockMapping[targetBlock->getIdentifier()];
    }

    auto parentBlockInsertionPoint = builder->saveInsertionPoint();
    // Create new block.
    auto mlirBasicBlock = builder->createBlock(builder->getBlock()->getParent());

    auto targetBlockArguments = targetBlock->getArguments();
    // Add attributes as arguments to block.
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

MLIRLoweringProvider::ValueFrame
MLIRLoweringProvider::createFrameFromParentBlock(MLIRLoweringProvider::ValueFrame& frame,
                                                 IR::Operations::BasicBlockInvocation& invocation) {
    auto invocationArguments = invocation.getArguments();
    auto childBlockArguments = invocation.getBlock()->getArguments();
    NES_ASSERT(invocationArguments.size() == childBlockArguments.size(),
               "the number of invocation parameters has to be the same as the number of block arguments in the invoked block.");
    ValueFrame childFrame;
    // Copy all frame values to the child frame that are arguments of the child block.
    for (uint64_t i = 0; i < invocationArguments.size(); i++) {
        auto parentOperation = invocationArguments[i];
        auto parentValue = frame.getValue(parentOperation->getIdentifier());
        auto childBlockArgument = childBlockArguments[i];
        childFrame.setValue(childBlockArgument->getIdentifier(), parentValue);
    }
    return childFrame;
}

void MLIRLoweringProvider::generateMLIR(std::shared_ptr<IR::Operations::CastOperation> castOperation,
                                        MLIRLoweringProvider::ValueFrame& frame) {
    auto inputStamp = castOperation->getInput()->getStamp();
    auto outputStamp = castOperation->getStamp();

    if (inputStamp->isInteger() && outputStamp->isInteger()) {
        auto inputIntegerStamp = cast<IR::Types::IntegerStamp>(inputStamp);
        auto outputIntegerStamp = cast<IR::Types::IntegerStamp>(outputStamp);
        auto mlirInput = frame.getValue(castOperation->getInput()->getIdentifier());
        if (inputIntegerStamp->getBitWidth() == outputIntegerStamp->getBitWidth()) {
            // we skip the cast if input bit width are the same.
            frame.setValue(castOperation->getIdentifier(), mlirInput);
        } else if (outputIntegerStamp->isSigned()) {
            auto mlirCast = builder->create<mlir::arith::ExtSIOp>(getNameLoc("location"), getMLIRType(outputStamp), mlirInput);
            frame.setValue(castOperation->getIdentifier(), mlirCast);
        } else {
            auto mlirCast = builder->create<mlir::arith::ExtUIOp>(getNameLoc("location"), getMLIRType(outputStamp), mlirInput);
            frame.setValue(castOperation->getIdentifier(), mlirCast);
        }
        return;
    } else if (inputStamp->isFloat() && outputStamp->isFloat()) {
        auto inputFloatStamp = cast<IR::Types::FloatStamp>(inputStamp);
        auto outputFloatStamp = cast<IR::Types::FloatStamp>(outputStamp);
        auto mlirInput = frame.getValue(castOperation->getInput()->getIdentifier());
        auto mlirCast = builder->create<mlir::arith::ExtFOp>(getNameLoc("location"), getMLIRType(outputFloatStamp), mlirInput);
        frame.setValue(castOperation->getIdentifier(), mlirCast);
        return;
    } else if (inputStamp->isInteger() && outputStamp->isFloat()) {
        auto inputIntegerStamp = cast<IR::Types::IntegerStamp>(inputStamp);
        auto outputFloatStamp = cast<IR::Types::FloatStamp>(outputStamp);
        auto mlirInput = frame.getValue(castOperation->getInput()->getIdentifier());
        if (inputIntegerStamp->isSigned()) {
            auto mlirCast = builder->create<mlir::arith::SIToFPOp>(getNameLoc("location"), getMLIRType(outputStamp), mlirInput);
            frame.setValue(castOperation->getIdentifier(), mlirCast);
        } else {
            auto mlirCast = builder->create<mlir::arith::UIToFPOp>(getNameLoc("location"), getMLIRType(outputStamp), mlirInput);
            frame.setValue(castOperation->getIdentifier(), mlirCast);
        }
        return;
    } else {
        NES_THROW_RUNTIME_ERROR("Cast from " << inputStamp->toString() << " to " << outputStamp->toString()
                                             << " is not supported.");
    }
}

void MLIRLoweringProvider::generateMLIR(std::shared_ptr<IR::Operations::ConstBooleanOperation> constBooleanOp,
                                        MLIRLoweringProvider::ValueFrame& frame) {
    auto constOp =
        builder->create<mlir::arith::ConstantOp>(getNameLoc("location"),
                                                 builder->getI1Type(),
                                                 builder->getIntegerAttr(builder->getI1Type(), constBooleanOp->getValue()));
    frame.setValue(constBooleanOp->getIdentifier(), constOp);
}

std::vector<std::string> MLIRLoweringProvider::getJitProxyFunctionSymbols() { return std::move(jitProxyFunctionSymbols); }
std::vector<llvm::JITTargetAddress> MLIRLoweringProvider::getJitProxyTargetAddresses() {
    return std::move(jitProxyFunctionTargetAddresses);
}
}// namespace NES::Nautilus::Backends::MLIR