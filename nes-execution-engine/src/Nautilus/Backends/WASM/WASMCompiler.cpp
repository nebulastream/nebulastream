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

#include "Nautilus/IR/Types/FloatStamp.hpp"
#include "Nautilus/IR/Types/IntegerStamp.hpp"
#include <Nautilus/Backends/WASM/WASMCompiler.hpp>
#include <Nautilus/IR/BasicBlocks/BasicBlock.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Nautilus::Backends::WASM {

WASMCompiler::WASMCompiler() = default;

BinaryenModuleRef WASMCompiler::compile(std::shared_ptr<IR::IRGraph> ir) {
    wasm = BinaryenModuleCreate();
    consumed.clear();
    generateWASM(ir->getRootOperation());
/*
    if (!BinaryenModuleValidate(expressions)) {
        //TODO: WASM incorrect, throw error
    }
*/
    //BinaryenModuleOptimize(wasm);
    return wasm;
}

void WASMCompiler::generateWASM(std::shared_ptr<IR::Operations::FunctionOperation> functionOp) {
    std::cout << "Handling FunctionOp!" << std::endl;
    BinaryenExpressions bodyList;
    std::vector<BinaryenType> args;
    int argIndex = 0;
    for (const auto& inputArg : functionOp->getFunctionBasicBlock()->getArguments()) {
        BinaryenExpressionRef argExpression;
        if (auto integerStamp = cast_if<IR::Types::IntegerStamp>(inputArg->getStamp().get())) {
            if (integerStamp->getBitWidth() == IR::Types::IntegerStamp::I64) {
                args.push_back(BinaryenTypeInt64());
                argExpression = BinaryenLocalGet(wasm, argIndex, BinaryenTypeInt64());
            } else {
                args.push_back(BinaryenTypeInt32());
                argExpression = BinaryenLocalGet(wasm, argIndex, BinaryenTypeInt32());
            }
        } else {
            auto floatStamp = cast_if<IR::Types::FloatStamp>(inputArg->getStamp().get());
            if (floatStamp->getBitWidth() == IR::Types::FloatStamp::F64) {
                args.push_back(BinaryenTypeFloat64());
                argExpression = BinaryenLocalGet(wasm, argIndex, BinaryenTypeInt64());
            } else {
                args.push_back(BinaryenTypeFloat32());
                argExpression = BinaryenLocalGet(wasm, argIndex, BinaryenTypeInt32());
            }
        }
        bodyList.setValue(inputArg->getIdentifier(), argExpression);
        consumed.emplace(inputArg->getIdentifier(), argExpression);
        argIndex++;
    }

    generateWASM(functionOp->getFunctionBasicBlock(), bodyList);

    //TODO: fix this
    BinaryenExpressionRef bodyArray[100];
    int i = 0;
    for (const auto& exp : bodyList.getContent()) {
        if (!consumed.contains(exp.first)) {
            bodyArray[i] = exp.second;
            i++;
        }
    }

    //TODO: fix this
    BinaryenType argsArray[10];
    int j = 0;
    for (const auto& x : args) {
        argsArray[j] = x;
        j++;
    }
    BinaryenType arguments = BinaryenTypeCreate(argsArray, j);
    BinaryenExpressionRef body = BinaryenBlock(wasm, "body", bodyArray, i, BinaryenTypeAuto());
    BinaryenAddFunction(wasm, functionOp->getName().c_str(), arguments, getType(functionOp->getOutputArg()), nullptr, 0, body);
    BinaryenAddFunctionExport(wasm, functionOp->getName().c_str(), functionOp->getName().c_str());
}

void WASMCompiler::generateWASM(IR::BasicBlockPtr basicBlock, BinaryenExpressions& expressions) {
    std::cout << "Handling BasicBlock!" << std::endl;
    for (const auto& operation : basicBlock->getOperations()) {
        generateWASM(operation, expressions);
    }
}

void WASMCompiler::generateWASM(const IR::Operations::OperationPtr& operation, BinaryenExpressions& expressions) {
    switch (operation->getOperationType()) {
        case IR::Operations::Operation::OperationType::FunctionOp:
            generateWASM(std::static_pointer_cast<IR::Operations::FunctionOperation>(operation), expressions);
            break;
        case IR::Operations::Operation::OperationType::LoopOp:
            generateWASM(std::static_pointer_cast<IR::Operations::LoopOperation>(operation), expressions);
            break;
        case IR::Operations::Operation::OperationType::ConstIntOp:
            generateWASM(std::static_pointer_cast<IR::Operations::ConstIntOperation>(operation), expressions);
            break;
        case IR::Operations::Operation::OperationType::ConstFloatOp:
            generateWASM(std::static_pointer_cast<IR::Operations::ConstFloatOperation>(operation), expressions);
            break;
        case IR::Operations::Operation::OperationType::AddOp:
            generateWASM(std::static_pointer_cast<IR::Operations::AddOperation>(operation), expressions);
            break;
        case IR::Operations::Operation::OperationType::SubOp:
            generateWASM(std::static_pointer_cast<IR::Operations::SubOperation>(operation), expressions);
            break;
        case IR::Operations::Operation::OperationType::MulOp:
            generateWASM(std::static_pointer_cast<IR::Operations::MulOperation>(operation), expressions);
            break;
        case IR::Operations::Operation::OperationType::DivOp:
            generateWASM(std::static_pointer_cast<IR::Operations::DivOperation>(operation), expressions);
            break;
        case IR::Operations::Operation::OperationType::LoadOp:
            generateWASM(std::static_pointer_cast<IR::Operations::LoadOperation>(operation), expressions);
            break;
        case IR::Operations::Operation::OperationType::AddressOp:
            generateWASM(std::static_pointer_cast<IR::Operations::AddressOperation>(operation), expressions);
            break;
        case IR::Operations::Operation::OperationType::StoreOp:
            generateWASM(std::static_pointer_cast<IR::Operations::StoreOperation>(operation), expressions);
            break;
        case IR::Operations::Operation::OperationType::BranchOp:
            generateWASM(std::static_pointer_cast<IR::Operations::BranchOperation>(operation), expressions);
            break;
        case IR::Operations::Operation::OperationType::IfOp:
            generateWASM(std::static_pointer_cast<IR::Operations::IfOperation>(operation), expressions);
            break;
        case IR::Operations::Operation::OperationType::CompareOp:
            generateWASM(std::static_pointer_cast<IR::Operations::CompareOperation>(operation), expressions);
            break;
        case IR::Operations::Operation::OperationType::ProxyCallOp:
            generateWASM(std::static_pointer_cast<IR::Operations::ProxyCallOperation>(operation), expressions);
            break;
        case IR::Operations::Operation::OperationType::ReturnOp:
            generateWASM(std::static_pointer_cast<IR::Operations::ReturnOperation>(operation), expressions);
            break;
        case IR::Operations::Operation::OperationType::OrOp:
            generateWASM(std::static_pointer_cast<IR::Operations::OrOperation>(operation), expressions);
            break;
        case IR::Operations::Operation::OperationType::AndOp:
            generateWASM(std::static_pointer_cast<IR::Operations::AndOperation>(operation), expressions);
            break;
        case IR::Operations::Operation::OperationType::NegateOp:
            generateWASM(std::static_pointer_cast<IR::Operations::NegateOperation>(operation), expressions);
            break;
        case IR::Operations::Operation::OperationType::CastOp:
            generateWASM(std::static_pointer_cast<IR::Operations::CastOperation>(operation), expressions);
            break;
        case IR::Operations::Operation::OperationType::ConstBooleanOp:
            generateWASM(std::static_pointer_cast<IR::Operations::ConstBooleanOperation>(operation), expressions);
            break;
        default: NES_NOT_IMPLEMENTED();
    }
}

void WASMCompiler::generateWASM(std::shared_ptr<IR::Operations::NegateOperation> negateOperation, BinaryenExpressions& expressions) {
    std::cout << "Handling NegateOp!" << std::endl;
    auto input = expressions.getValue(negateOperation->getInput()->getIdentifier());
    auto x = negateOperation->toString();
    auto y = expressions.contains("");
}

void WASMCompiler::generateWASM(std::shared_ptr<IR::Operations::OrOperation> orOperation, BinaryenExpressions& expressions) {
    auto left = expressions.getValue(orOperation->getLeftInput()->getIdentifier());
    auto right = expressions.getValue(orOperation->getRightInput()->getIdentifier());
    BinaryenExpressionRef orOp = BinaryenBinary(wasm, BinaryenOrInt32(), left, right);

    consumed.emplace(orOperation->getLeftInput()->getIdentifier(), left);
    consumed.emplace(orOperation->getRightInput()->getIdentifier(), right);
    expressions.setValue(orOperation->getIdentifier(), orOp);
}

void WASMCompiler::generateWASM(std::shared_ptr<IR::Operations::AndOperation> andOperation, BinaryenExpressions& expressions) {
    auto left = expressions.getValue(andOperation->getLeftInput()->getIdentifier());
    auto right = expressions.getValue(andOperation->getRightInput()->getIdentifier());
    BinaryenExpressionRef andOp = BinaryenBinary(wasm, BinaryenAndInt32(), left, right);

    consumed.emplace(andOperation->getLeftInput()->getIdentifier(), left);
    consumed.emplace(andOperation->getRightInput()->getIdentifier(), right);
    expressions.setValue(andOperation->getIdentifier(), andOp);
}

void WASMCompiler::generateWASM(std::shared_ptr<IR::Operations::LoopOperation> loopOp, BinaryenExpressions& expressions) {
    auto x = loopOp->toString();
    auto y = expressions.contains("");
}

//==--------------------------==//
//==-- MEMORY (LOAD, STORE) --==//
//==--------------------------==//
void WASMCompiler::generateWASM(std::shared_ptr<IR::Operations::AddressOperation> addressOp, BinaryenExpressions& expressions) {
    auto x = addressOp->toString();
    auto y = expressions.contains("");
}

void WASMCompiler::generateWASM(std::shared_ptr<IR::Operations::LoadOperation> loadOp, BinaryenExpressions& expressions) {
    auto x = loadOp->toString();
    auto y = expressions.contains("");
}

void WASMCompiler::generateWASM(std::shared_ptr<IR::Operations::ConstIntOperation> constIntOp, BinaryenExpressions& expressions) {
    std::cout << "Handling ConstIntOp!" << std::endl;
    if (auto integerStamp = cast_if<IR::Types::IntegerStamp>(constIntOp->getStamp().get())) {
        if (integerStamp->getBitWidth() == IR::Types::IntegerStamp::I64) {
            expressions.setValue(constIntOp->getIdentifier(),
                                 BinaryenConst(wasm, BinaryenLiteralInt64(constIntOp->getConstantIntValue())));
        } else {
            expressions.setValue(constIntOp->getIdentifier(),
                                 BinaryenConst(wasm, BinaryenLiteralInt32(constIntOp->getConstantIntValue())));
        }
    }
}

void WASMCompiler::generateWASM(std::shared_ptr<IR::Operations::ConstFloatOperation> constFloatOp, BinaryenExpressions& expressions) {
    std::cout << "Handling ConstFloatOp!" << std::endl;
    if (auto floatStamp = cast_if<IR::Types::FloatStamp>(constFloatOp->getStamp().get())) {
        if (floatStamp->getBitWidth() == IR::Types::FloatStamp::F64) {
            expressions.setValue(constFloatOp->getIdentifier(),
                                 BinaryenConst(wasm, BinaryenLiteralFloat64(constFloatOp->getConstantFloatValue())));
        } else {
            expressions.setValue(constFloatOp->getIdentifier(),
                                 BinaryenConst(wasm, BinaryenLiteralFloat32(constFloatOp->getConstantFloatValue())));
        }
    }
}

//==---------------------------==//
//==-- ARITHMETIC OPERATIONS --==//
//==---------------------------==//
void WASMCompiler::generateWASM(std::shared_ptr<IR::Operations::AddOperation> addOp, BinaryenExpressions& expressions) {
    std::cout << "Handling AddOp!" << std::endl;
    auto left = expressions.getValue(addOp->getLeftInput()->getIdentifier());
    auto right = expressions.getValue(addOp->getRightInput()->getIdentifier());
    auto type = BinaryenExpressionGetType(left);
    BinaryenExpressionRef add;
    if (type == BinaryenTypeInt32()) {
        add = BinaryenBinary(wasm, BinaryenAddInt32(), left, right);
    } else if (type == BinaryenTypeInt64()) {
        add = BinaryenBinary(wasm, BinaryenAddInt64(), left, right);
    } else if (type == BinaryenTypeFloat32()) {
        add = BinaryenBinary(wasm, BinaryenAddFloat32(), left, right);
    } else {
        add = BinaryenBinary(wasm, BinaryenAddFloat64(), left, right);
    }
    consumed.emplace(addOp->getLeftInput()->getIdentifier(), left);
    consumed.emplace(addOp->getRightInput()->getIdentifier(), right);
    expressions.setValue(addOp->getIdentifier(), add);
}

void WASMCompiler::generateWASM(std::shared_ptr<IR::Operations::SubOperation> subOp, BinaryenExpressions& expressions) {
    std::cout << "Handling SubOp!" << std::endl;
    auto left = expressions.getValue(subOp->getLeftInput()->getIdentifier());
    auto right = expressions.getValue(subOp->getRightInput()->getIdentifier());
    auto type = BinaryenExpressionGetType(left);
    BinaryenExpressionRef sub;
    if (type == BinaryenTypeInt32()) {
        sub = BinaryenBinary(wasm, BinaryenSubInt32(), left, right);
    } else if (type == BinaryenTypeInt64()) {
        sub = BinaryenBinary(wasm, BinaryenSubInt64(), left, right);
    } else if (type == BinaryenTypeFloat32()) {
        sub = BinaryenBinary(wasm, BinaryenSubFloat32(), left, right);
    } else {
        sub = BinaryenBinary(wasm, BinaryenSubFloat64(), left, right);
    }
    consumed.emplace(subOp->getLeftInput()->getIdentifier(), left);
    consumed.emplace(subOp->getRightInput()->getIdentifier(), right);
    expressions.setValue(subOp->getIdentifier(), sub);
}

void WASMCompiler::generateWASM(std::shared_ptr<IR::Operations::MulOperation> mulOp, BinaryenExpressions& expressions) {
    std::cout << "Handling MulOp!" << std::endl;
    auto left = expressions.getValue(mulOp->getLeftInput()->getIdentifier());
    auto right = expressions.getValue(mulOp->getRightInput()->getIdentifier());
    auto type = BinaryenExpressionGetType(left);
    BinaryenExpressionRef mul;
    if (type == BinaryenTypeInt32()) {
        mul = BinaryenBinary(wasm, BinaryenMulInt32(), left, right);
    } else if (type == BinaryenTypeInt64()) {
        mul = BinaryenBinary(wasm, BinaryenMulInt64(), left, right);
    } else if (type == BinaryenTypeFloat32()) {
        mul = BinaryenBinary(wasm, BinaryenMulFloat32(), left, right);
    } else {
        mul = BinaryenBinary(wasm, BinaryenMulFloat64(), left, right);
    }

    consumed.emplace(mulOp->getLeftInput()->getIdentifier(), left);
    consumed.emplace(mulOp->getRightInput()->getIdentifier(), right);
    expressions.setValue(mulOp->getIdentifier(), mul);
}

//TODO
void WASMCompiler::generateWASM(std::shared_ptr<IR::Operations::DivOperation> divOp, BinaryenExpressions& expressions) {
    auto left = expressions.getValue(divOp->getLeftInput()->getIdentifier());
    auto right = expressions.getValue(divOp->getRightInput()->getIdentifier());
    if (divOp->getStamp()->isFloat()) {

    }

    consumed.emplace(divOp->getLeftInput()->getIdentifier(), left);
    consumed.emplace(divOp->getRightInput()->getIdentifier(), right);
}

void WASMCompiler::generateWASM(std::shared_ptr<IR::Operations::StoreOperation> storeOp, BinaryenExpressions& expressions) {
    auto y = expressions.contains("");
    auto x = storeOp->toString();
}

void WASMCompiler::generateWASM(std::shared_ptr<IR::Operations::ReturnOperation> returnOp, BinaryenExpressions& expressions) {
    std::cout << "Handling ReturnOp!" << std::endl;
    auto y = expressions.contains("");
    auto x = returnOp->toString();
}

void WASMCompiler::generateWASM(std::shared_ptr<IR::Operations::ProxyCallOperation> proxyCallOp, BinaryenExpressions& expressions) {
    std::cout << "Handling ProxyCallOp!" << std::endl;
    auto y = expressions.contains("");
    auto x = proxyCallOp->toString();
}

void WASMCompiler::generateWASM(std::shared_ptr<IR::Operations::CompareOperation> compareOp, BinaryenExpressions& expressions) {
    std::cout << "Handling CompareOp!" << std::endl;
    auto left = expressions.getValue(compareOp->getLeftInput()->getIdentifier());
    auto right = expressions.getValue(compareOp->getRightInput()->getIdentifier());
    BinaryenExpressionRef compExpression;
    if (compareOp->getComparator() < 10) {
        compExpression = BinaryenBinary(wasm, convertToInt32Comparison(compareOp->getComparator()), left, right);
    } else {
        compExpression = BinaryenBinary(wasm, convertToFloat32Comparison(compareOp->getComparator()), left, right);
    }
    consumed.emplace(compareOp->getLeftInput()->getIdentifier(), left);
    consumed.emplace(compareOp->getRightInput()->getIdentifier(), right);
    expressions.setValue(compareOp->getIdentifier(), compExpression);
}

void WASMCompiler::generateWASM(std::shared_ptr<IR::Operations::IfOperation> ifOp, BinaryenExpressions& expressions) {
    std::cout << "Handling ifOp!" << std::endl;
    auto y = expressions.contains("");
    auto x = ifOp->toString();
}

void WASMCompiler::generateWASM(std::shared_ptr<IR::Operations::BranchOperation> branchOp, BinaryenExpressions& expressions) {
    auto y = expressions.contains("");
    auto x = branchOp->toString();
}

void WASMCompiler::generateWASM(std::shared_ptr<IR::Operations::CastOperation> castOperation,
                                        BinaryenExpressions& expressions) {
    std::cout << "Handling CastOp!" << std::endl;
    auto y = expressions.contains("");
    auto x = castOperation->toString();
}

void WASMCompiler::generateWASM(std::shared_ptr<IR::Operations::ConstBooleanOperation> constBooleanOp,
                                        BinaryenExpressions& expressions) {
    std::cout << "Handling ConstBooleanOp!" << std::endl;
    if (constBooleanOp->getValue()) {
        expressions.setValue(constBooleanOp->getIdentifier(),
                             BinaryenConst(wasm, BinaryenLiteralInt32(1)));
    } else {
        expressions.setValue(constBooleanOp->getIdentifier(),
                             BinaryenConst(wasm, BinaryenLiteralInt32(0)));
    }
}

BinaryenType WASMCompiler::getType(IR::Types::StampPtr stampPtr) {
    if (auto integerStamp = cast_if<IR::Types::IntegerStamp>(stampPtr.get())) {
        if (integerStamp->getBitWidth() == IR::Types::IntegerStamp::I64) {
            return BinaryenTypeInt64();
        } else {
            return BinaryenTypeInt32();
        }
    } else if (auto floatStamp = cast_if<IR::Types::FloatStamp>(stampPtr.get())) {
        if (floatStamp->getBitWidth() == IR::Types::FloatStamp::F64) {
            return BinaryenTypeFloat64();
        } else {
            return BinaryenTypeFloat32();
        }
    } else {
        //Return Boolean as Int32
        return BinaryenTypeInt32();
    }
}

BinaryenOp WASMCompiler::convertToInt32Comparison(IR::Operations::CompareOperation::Comparator comparisonType) {
    switch (comparisonType) {
        case (IR::Operations::CompareOperation::Comparator::IEQ): return BinaryenEqInt32();
        case (IR::Operations::CompareOperation::Comparator::INE): return BinaryenNeInt32();
        case (IR::Operations::CompareOperation::Comparator::ISLT): return BinaryenLtSInt32();
        case (IR::Operations::CompareOperation::Comparator::ISLE): return BinaryenLeSInt32();
        case (IR::Operations::CompareOperation::Comparator::ISGT): return BinaryenGtSInt32();
        case (IR::Operations::CompareOperation::Comparator::ISGE): return BinaryenGeSInt32();
        // Unsigned Comparisons.
        case (IR::Operations::CompareOperation::Comparator::IULT): return BinaryenLtUInt32();
        case (IR::Operations::CompareOperation::Comparator::IULE): return BinaryenLeUInt32();
        case (IR::Operations::CompareOperation::Comparator::IUGT): return BinaryenGtUInt32();
        default: return BinaryenGeUInt32();
    }
}

BinaryenOp WASMCompiler::convertToInt64Comparison(IR::Operations::CompareOperation::Comparator comparisonType) {
    switch (comparisonType) {
        case (IR::Operations::CompareOperation::Comparator::IEQ): return BinaryenEqInt64();
        case (IR::Operations::CompareOperation::Comparator::INE): return BinaryenNeInt64();
        case (IR::Operations::CompareOperation::Comparator::ISLT): return BinaryenLtSInt64();
        case (IR::Operations::CompareOperation::Comparator::ISLE): return BinaryenLeSInt64();
        case (IR::Operations::CompareOperation::Comparator::ISGT): return BinaryenGtSInt64();
        case (IR::Operations::CompareOperation::Comparator::ISGE): return BinaryenGeSInt64();
        // Unsigned Comparisons.
        case (IR::Operations::CompareOperation::Comparator::IULT): return BinaryenLtUInt64();
        case (IR::Operations::CompareOperation::Comparator::IULE): return BinaryenLeUInt64();
        case (IR::Operations::CompareOperation::Comparator::IUGT): return BinaryenGtUInt64();
        default: return BinaryenGeUInt64();
    }
}

BinaryenOp WASMCompiler::convertToFloat32Comparison(IR::Operations::CompareOperation::Comparator comparisonType) {
    switch (comparisonType) {
        case (IR::Operations::CompareOperation::Comparator::FOLT): return BinaryenLtFloat32();
        case (IR::Operations::CompareOperation::Comparator::FOLE): return BinaryenLeFloat32();
        case (IR::Operations::CompareOperation::Comparator::FOEQ): return BinaryenEqFloat32();
        case (IR::Operations::CompareOperation::Comparator::FOGT): return BinaryenGtFloat32();
        case (IR::Operations::CompareOperation::Comparator::FOGE): return BinaryenGeFloat32();
        default: return BinaryenNeFloat32();
    }
}

BinaryenOp WASMCompiler::convertToFloat64Comparison(IR::Operations::CompareOperation::Comparator comparisonType) {
    switch (comparisonType) {
        case (IR::Operations::CompareOperation::Comparator::FOLT): return BinaryenLtFloat64();
        case (IR::Operations::CompareOperation::Comparator::FOLE): return BinaryenLeFloat64();
        case (IR::Operations::CompareOperation::Comparator::FOEQ): return BinaryenEqFloat64();
        case (IR::Operations::CompareOperation::Comparator::FOGT): return BinaryenGtFloat64();
        case (IR::Operations::CompareOperation::Comparator::FOGE): return BinaryenGeFloat64();
        default: return BinaryenNeFloat64();
    }
}

}// namespace NES::Nautilus::Backends::WASM