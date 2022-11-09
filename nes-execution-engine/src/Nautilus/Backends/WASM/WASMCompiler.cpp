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

#include <Nautilus/IR/Types/FloatStamp.hpp>
#include <Nautilus/IR/Types/IntegerStamp.hpp>
#include <Nautilus/Backends/WASM/WASMCompiler.hpp>
#include <Nautilus/IR/BasicBlocks/BasicBlock.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Nautilus::Backends::WASM {

WASMCompiler::WASMCompiler() = default;

/**
 * Entry point for lowering Nautilus IR to WebAssembly (WASM) binary code
 * @param ir
 * @return
 */
BinaryenModuleRef WASMCompiler::lower(const std::shared_ptr<IR::IRGraph>& ir) {
    BinaryenSetColorsEnabled(true);
    wasm = BinaryenModuleCreate();
    relooper = RelooperCreate(wasm);
    consumed.clear();
    index = 0;
    argIndex = 0;
    ir->toString();
    generateWASM(ir->getRootOperation());
    //tmp();
    BinaryenModuleAutoDrop(wasm);
    if (!BinaryenModuleValidate(wasm)) {
        std::cout << "Generated pre-optimized wasm is incorrect!" << std::endl;
    }


    //BinaryenModuleOptimize(wasm);
    /*
    if (!BinaryenModuleValidate(wasm)) {
        std::cout << "Generated post-optimized wasm is incorrect!" << std::endl;
    }
    */
    BinaryenModulePrintStackIR(wasm, false);
    return wasm;
}

BinaryenExpressionRef makeInt32(BinaryenModuleRef wasm, int x) {
    return BinaryenConst(wasm, BinaryenLiteralInt32(x));
}

BinaryenExpressionRef makeDroppedInt32(BinaryenModuleRef wasm, int x) {
    return BinaryenDrop(wasm, BinaryenConst(wasm, BinaryenLiteralInt32(x)));
}

BinaryenExpressionRef makeCallCheck(BinaryenModuleRef wasm, int x) {
    BinaryenExpressionRef callOperands[] = {makeInt32(wasm, x)};
    return BinaryenCall(wasm, "check", callOperands, 1, BinaryenTypeNone());
}

void WASMCompiler::tmp() {
    BinaryenAddFunctionImport(wasm,
                              "check",
                              "module",
                              "check",
                              BinaryenTypeInt32(),
                              BinaryenTypeNone());
    BinaryenType localTypes[] = {BinaryenTypeInt32()};
    RelooperBlockRef block0 =
        RelooperAddBlock(relooper, makeCallCheck(wasm, 0));
    RelooperBlockRef block1 =
        RelooperAddBlock(relooper, makeCallCheck(wasm, 1));
    RelooperBlockRef block2 =
        RelooperAddBlock(relooper, makeCallCheck(wasm, 2));
    BinaryenExpressionRef temp = makeDroppedInt32(wasm, 10);
    RelooperAddBranch(block0, block1, makeInt32(wasm, 55), temp);
    RelooperAddBranch(block0, block2, NULL, makeDroppedInt32(wasm, 20));
    BinaryenExpressionRef body = RelooperRenderAndDispose(relooper, block0, 0);
    BinaryenFunctionRef sinker = BinaryenAddFunction(wasm,
                                                     "split-plus-code",
                                                     BinaryenTypeNone(),
                                                     BinaryenTypeNone(),
                                                     localTypes,
                                                     1,
                                                     body);
}

void WASMCompiler::generateWASM(std::shared_ptr<IR::Operations::FunctionOperation> functionOp) {
    std::cout << "Handling FunctionOp!" << std::endl;
    BinaryenExpressions bodyList;
    std::vector<BinaryenType> args;
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

    //Add function args
    auto argsArray = std::make_unique<BinaryenType[]>(args.size());
    int j = 0;

    for (const auto& x : args) {
        argsArray[j] = x;
        j++;
    }

    //Extract variables and set them as locals
    auto varArray = std::make_unique<BinaryenType[]>(localVariables.size());
    int k = 0;
    for (const auto& x : localVariables.getMapping()) {
        varArray[k] = x.second;
        k++;
    }

    BinaryenType arguments = BinaryenTypeCreate(argsArray.get(), j);
    //BinaryenExpressionRef body = BinaryenBlock(wasm, "body", bodyArray.get(), i, BinaryenTypeAuto());
    BinaryenExpressionRef body = generateBody();
    //BinaryenExpressionRef body = RelooperRenderAndDispose(relooper, relooperBlocks[0].second, 0);
    //BinaryenAddFunction(wasm, functionOp->getName().c_str(), arguments, getType(functionOp->getOutputArg()),
    //                    varArray.get(), localVariables.size(), body);
    BinaryenAddFunction(wasm, functionOp->getName().c_str(), arguments, getType(functionOp->getOutputArg()),
                        varArray.get(), index, body);
    BinaryenAddFunctionExport(wasm, functionOp->getName().c_str(), functionOp->getName().c_str());
    //TODO Add memory
}

void WASMCompiler::generateWASM(IR::BasicBlockPtr basicBlock, BinaryenExpressions& expressions) {
    std::cout << "Handling BasicBlock: " << basicBlock->getIdentifier() << std::endl;
    currentBlock = basicBlock;
    currentExpressions.clear();

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
    auto y = expressions.size();
}

void WASMCompiler::generateWASM(std::shared_ptr<IR::Operations::OrOperation> orOperation, BinaryenExpressions& expressions) {
    auto left = expressions.getValue(orOperation->getLeftInput()->getIdentifier());
    auto right = expressions.getValue(orOperation->getRightInput()->getIdentifier());
    BinaryenExpressionRef orOp = BinaryenBinary(wasm, BinaryenOrInt32(), left, right);

    consumed.emplace(orOperation->getLeftInput()->getIdentifier(), left);
    consumed.emplace(orOperation->getRightInput()->getIdentifier(), right);
    expressions.setValue(orOperation->getIdentifier(), orOp);
    currentExpressions.setValue(orOperation->getIdentifier(), orOp);
}

void WASMCompiler::generateWASM(std::shared_ptr<IR::Operations::AndOperation> andOperation, BinaryenExpressions& expressions) {
    auto left = expressions.getValue(andOperation->getLeftInput()->getIdentifier());
    auto right = expressions.getValue(andOperation->getRightInput()->getIdentifier());
    BinaryenExpressionRef andOp = BinaryenBinary(wasm, BinaryenAndInt32(), left, right);

    consumed.emplace(andOperation->getLeftInput()->getIdentifier(), left);
    consumed.emplace(andOperation->getRightInput()->getIdentifier(), right);
    expressions.setValue(andOperation->getIdentifier(), andOp);
    currentExpressions.setValue(andOperation->getIdentifier(), andOp);
}

void WASMCompiler::generateWASM(std::shared_ptr<IR::Operations::LoopOperation> loopOp, BinaryenExpressions& expressions) {
    std::cout << "Handling LoopOp!" << std::endl;
    //auto loopExpression = BinaryenLoop(wasm, );
    auto x = loopOp->toString();
    auto y = expressions.size();
}

//==--------------------------==//
//==-- MEMORY (LOAD, STORE) --==//
//==--------------------------==//
void WASMCompiler::generateWASM(std::shared_ptr<IR::Operations::AddressOperation> addressOp, BinaryenExpressions& expressions) {
    std::cout << "Handling AddressOp!" << std::endl;
    auto x = addressOp->toString();
    auto y = expressions.size();
}

void WASMCompiler::generateWASM(std::shared_ptr<IR::Operations::LoadOperation> loadOp, BinaryenExpressions& expressions) {
    std::cout << "Handling LoadOp!" << std::endl;
    auto x = loadOp->toString();
    auto y = expressions.size();
}

void WASMCompiler::generateWASM(std::shared_ptr<IR::Operations::ConstIntOperation> constIntOp, BinaryenExpressions& expressions) {
    std::cout << "Handling ConstIntOp!" << std::endl;
    //TODO: Check if var already exists!
    if (auto integerStamp = cast_if<IR::Types::IntegerStamp>(constIntOp->getStamp().get())) {
        if (integerStamp->getBitWidth() == IR::Types::IntegerStamp::I64) {
            localVariables.setUniqueValue(constIntOp->getIdentifier(), BinaryenTypeInt64());
            auto x = BinaryenLocalSet(wasm, argIndex + index, BinaryenConst(wasm, BinaryenLiteralInt64(constIntOp->getConstantIntValue())));
            expressions.setValue("set_" + constIntOp->getIdentifier(), x);
            auto y = BinaryenLocalGet(wasm, argIndex + index, BinaryenTypeInt64());
            expressions.setValue(constIntOp->getIdentifier(), y);
            consumed.emplace(constIntOp->getIdentifier(), y);
            index++;
            //expressions.setValue(constIntOp->getIdentifier(),
            //                     BinaryenConst(wasm, BinaryenLiteralInt64(constIntOp->getConstantIntValue())));
            //currentExpressions.setValue(constIntOp->getIdentifier(),
            //                     BinaryenConst(wasm, BinaryenLiteralInt64(constIntOp->getConstantIntValue())));
        } else {
            localVariables.setUniqueValue(constIntOp->getIdentifier(), BinaryenTypeInt32());
            auto x = BinaryenLocalSet(wasm, argIndex + index, BinaryenConst(wasm, BinaryenLiteralInt32(constIntOp->getConstantIntValue())));
            expressions.setValue("set_" + constIntOp->getIdentifier(), x);
            auto y = BinaryenLocalGet(wasm, argIndex + index, BinaryenTypeInt32());
            expressions.setValue(constIntOp->getIdentifier(), y);
            consumed.emplace(constIntOp->getIdentifier(), y);
            index++;
            //expressions.setValue(constIntOp->getIdentifier(),
            //                     BinaryenConst(wasm, BinaryenLiteralInt32(constIntOp->getConstantIntValue())));
            //currentExpressions.setValue(constIntOp->getIdentifier(),
            //                     BinaryenConst(wasm, BinaryenLiteralInt32(constIntOp->getConstantIntValue())));
        }
    }
}

void WASMCompiler::generateWASM(std::shared_ptr<IR::Operations::ConstFloatOperation> constFloatOp, BinaryenExpressions& expressions) {
    std::cout << "Handling ConstFloatOp!" << std::endl;
    if (auto floatStamp = cast_if<IR::Types::FloatStamp>(constFloatOp->getStamp().get())) {
        if (floatStamp->getBitWidth() == IR::Types::FloatStamp::F64) {
            //localVariables.emplace_back(BinaryenTypeFloat64());
            expressions.setValue(constFloatOp->getIdentifier(),
                                 BinaryenConst(wasm, BinaryenLiteralFloat64(constFloatOp->getConstantFloatValue())));
            currentExpressions.setValue(constFloatOp->getIdentifier(),
                                 BinaryenConst(wasm, BinaryenLiteralFloat64(constFloatOp->getConstantFloatValue())));
        } else {
            //localVariables.emplace_back(BinaryenTypeFloat32());
            expressions.setValue(constFloatOp->getIdentifier(),
                                 BinaryenConst(wasm, BinaryenLiteralFloat32(constFloatOp->getConstantFloatValue())));
            currentExpressions.setValue(constFloatOp->getIdentifier(),
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

    //TODO
    std::cout << "USAGES: " << addOp->getLeftInput()->getUsages().size() << std::endl;

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
    currentExpressions.setValue(addOp->getIdentifier(), add);
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
    currentExpressions.setValue(subOp->getIdentifier(), sub);
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
    currentExpressions.setValue(mulOp->getIdentifier(), mul);
}

void WASMCompiler::generateWASM(std::shared_ptr<IR::Operations::DivOperation> divOp, BinaryenExpressions& expressions) {
    auto left = expressions.getValue(divOp->getLeftInput()->getIdentifier());
    auto right = expressions.getValue(divOp->getRightInput()->getIdentifier());
    if (divOp->getStamp()->isFloat()) {

    }
    consumed.emplace(divOp->getLeftInput()->getIdentifier(), left);
    consumed.emplace(divOp->getRightInput()->getIdentifier(), right);
}

void WASMCompiler::generateWASM(std::shared_ptr<IR::Operations::StoreOperation> storeOp, BinaryenExpressions& expressions) {
    std::cout << "Handling StoreOp!" << std::endl;
    auto y = expressions.size();
    auto x = storeOp->toString();
}

void WASMCompiler::generateWASM(std::shared_ptr<IR::Operations::ProxyCallOperation> proxyCallOp, BinaryenExpressions& expressions) {
    std::cout << "Handling ProxyCallOp!" << std::endl;
    auto y = expressions.size();
    auto x = proxyCallOp->toString();
}

void WASMCompiler::generateWASM(std::shared_ptr<IR::Operations::CompareOperation> compareOp, BinaryenExpressions& expressions) {
    std::cout << "Handling CompareOp!" << std::endl;
    auto left = expressions.getValue(compareOp->getLeftInput()->getIdentifier());
    //std::cout << "LEFT USAGES: " << compareOp->getLeftInput()->getUsages().size() << std::endl;
    auto right = expressions.getValue(compareOp->getRightInput()->getIdentifier());
    //std::cout << "RIGHT USAGES: " << compareOp->getRightInput()->getUsages().size() << std::endl;
    auto leftType = getType(compareOp->getLeftInput()->getStamp());
    BinaryenExpressionRef compExpression;
    if (compareOp->getComparator() < 10) {
        if (leftType == BinaryenTypeInt32()) {
            compExpression = BinaryenBinary(wasm, convertToInt32Comparison(compareOp->getComparator()), left, right);
        } else {
            compExpression = BinaryenBinary(wasm, convertToInt64Comparison(compareOp->getComparator()), left, right);
        }
    } else {
        if (leftType == BinaryenTypeFloat32()) {
            compExpression = BinaryenBinary(wasm, convertToFloat32Comparison(compareOp->getComparator()), left, right);
        } else {
            compExpression = BinaryenBinary(wasm, convertToFloat64Comparison(compareOp->getComparator()), left, right);
        }
    }
    //consumed.emplace(compareOp->getLeftInput()->getIdentifier(), left);
    //consumed.emplace(compareOp->getRightInput()->getIdentifier(), right);
    expressions.setValue(compareOp->getIdentifier(), compExpression);
    currentExpressions.setValue(compareOp->getIdentifier(), compExpression);
}

void WASMCompiler::generateWASM(std::shared_ptr<IR::Operations::CastOperation> castOperation,
                                        BinaryenExpressions& expressions) {
    std::cout << "Handling CastOp!" << std::endl;
    auto y = expressions.size();
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

void WASMCompiler::generateWASM(std::shared_ptr<IR::Operations::ReturnOperation> returnOp, BinaryenExpressions& expressions) {
    std::cout << "Handling ReturnOp!" << std::endl;
    BinaryenExpressionRef ret;
    if (!returnOp->hasReturnValue()) {
        ret = BinaryenReturn(wasm, nullptr);
    } else {
        auto returnExp = expressions.getValue(returnOp->getReturnValue()->getIdentifier());
        ret = BinaryenReturn(wasm, returnExp);
        consumed.emplace(returnOp->getReturnValue()->getIdentifier(), returnExp);
    }
    expressions.setValue(returnOp->getIdentifier(), ret);
    currentExpressions.setValue(returnOp->getIdentifier(), ret);
    genBody(expressions);
}

void WASMCompiler::generateWASM(std::shared_ptr<IR::Operations::BranchOperation> branchOp, BinaryenExpressions& expressions) {
    std::cout << "Handling branchOp!" << std::endl;
    BinaryenExpressions blockArgs;
    for (const auto& targetBlockArgument : branchOp->getNextBlockInvocation().getArguments()) {
        blockArgs.setValue(targetBlockArgument->getIdentifier(), expressions.getValue(targetBlockArgument->getIdentifier()));
    }
    auto currentTrue = branchOp->getNextBlockInvocation().getArguments();
    auto nextTrue = branchOp->getNextBlockInvocation().getBlock()->getArguments();
    for(int i = 0; i < (int)branchOp->getNextBlockInvocation().getArguments().size(); i++) {
        std::cout << "BRANCH ARGS: " << currentTrue[i]->getIdentifier() << " - " << nextTrue[i]->getIdentifier() << std::endl;
        blockArgs.setValue(nextTrue[i]->getIdentifier(), expressions.getValue(currentTrue[i]->getIdentifier()));
        //expressions.setValue(blockArg->getIdentifier(), expressions.getValue(blockArg->getIdentifier()));
    }

    //blockLinking.setValue(currentBlock->getIdentifier(), branchOp->getNextBlockInvocation().getBlock()->getIdentifier());
    blockLinking.emplace_back(std::make_tuple(currentBlock->getIdentifier(),
                                              branchOp->getNextBlockInvocation().getBlock()->getIdentifier(),
                                              nullptr));
    genBody(expressions);

    generateBasicBlock(branchOp->getNextBlockInvocation(), blockArgs);
    //expressions.setValue(branchOp->getIdentifier(), tmp);
    //currentExpressions.setValue(branchOp->getIdentifier(), tmp);

}

void WASMCompiler::generateWASM(std::shared_ptr<IR::Operations::IfOperation> ifOp, BinaryenExpressions& expressions) {
    std::cout << "Handling ifOp!" << std::endl;
    BinaryenExpressionRef trueBlock, falseBlock;
    BinaryenExpressions trueBlockArgs, falseBlockArgs;
    //auto v = BinaryenBreak(wasm, "0", nullptr, nullptr);
    //expressions.setValue("test", v);
    //blockLinking.setValue(currentBlock->getIdentifier(), ifOp->getFalseBlockInvocation().getBlock()->getIdentifier());
    blockLinking.emplace_back(std::make_tuple(currentBlock->getIdentifier(),
                                              ifOp->getFalseBlockInvocation().getBlock()->getIdentifier(),
                                              nullptr));
    //blockLinking.setValue(currentBlock->getIdentifier(), ifOp->getTrueBlockInvocation().getBlock()->getIdentifier());
    /**
     * We link to the true block which needs to satisfy the condition. Therefore, save the condition
     * for creating the branch later on
     */
    blockLinking.emplace_back(std::make_tuple(currentBlock->getIdentifier(),
                                              ifOp->getTrueBlockInvocation().getBlock()->getIdentifier(),
                                              expressions.getValue(ifOp->getValue()->getIdentifier())));
    consumed.emplace(ifOp->getValue()->getIdentifier(), expressions.getValue(ifOp->getValue()->getIdentifier()));

    genBody(expressions);

    auto currentTrue = ifOp->getTrueBlockInvocation().getArguments();
    auto nextTrue = ifOp->getTrueBlockInvocation().getBlock()->getArguments();
    for(int i = 0; i < (int)ifOp->getTrueBlockInvocation().getArguments().size(); i++) {
        std::cout << "IF TRUE ARGS: " << currentTrue[i]->getIdentifier() << " - " << nextTrue[i]->getIdentifier() << std::endl;
        trueBlockArgs.setValue(nextTrue[i]->getIdentifier(), expressions.getValue(currentTrue[i]->getIdentifier()));
        //expressions.setValue(blockArg->getIdentifier(), expressions.getValue(blockArg->getIdentifier()));
    }
    generateBasicBlock(ifOp->getTrueBlockInvocation(), trueBlockArgs);

    auto currentFalse = ifOp->getFalseBlockInvocation().getArguments();
    auto nextFalse = ifOp->getFalseBlockInvocation().getBlock()->getArguments();
    for(int i = 0; i < (int)ifOp->getFalseBlockInvocation().getArguments().size(); i++) {
        std::cout << "IF FALSE ARGS: " << currentFalse[i]->getIdentifier() << " - " << nextFalse[i]->getIdentifier() << std::endl;
        falseBlockArgs.setValue(nextFalse[i]->getIdentifier(), expressions.getValue(currentFalse[i]->getIdentifier()));
        consumed.erase(nextFalse[i]->getIdentifier());
        //expressions.setValue(blockArg->getIdentifier(), expressions.getValue(blockArg->getIdentifier()));
    }
    generateBasicBlock(ifOp->getFalseBlockInvocation(), falseBlockArgs);
    //auto ifOperation = BinaryenIf(wasm, expressions.getValue(ifOp->getValue()->getIdentifier()), trueBlock, falseBlock);
    //expressions.setValue(ifOp->getIdentifier(), ifOperation);
    //currentExpressions.setValue(ifOp->getIdentifier(), ifOperation);
    //consumed.emplace(ifOp->getValue()->getIdentifier(), expressions.getValue(ifOp->getValue()->getIdentifier()));
}

void WASMCompiler::genBody(BinaryenExpressions expressions) {
    auto bodyArray = std::make_unique<BinaryenExpressionRef[]>(expressions.size());
    int i = 0;
    for (const auto& exp : expressions.getMapping()) {
        if (!consumed.contains(exp.first)) {
            bodyArray[i] = exp.second;
            i++;
            //consumed.emplace(exp.first, exp.second);
        }
    }
    std::cout << "Current Block: " << currentBlock->getIdentifier() << std::endl;
    for (int j = 0; j < i; ++j) {
        BinaryenExpressionPrint(bodyArray[j]);
    }
    auto x = BinaryenBlock(wasm, currentBlock->getIdentifier().c_str(), bodyArray.get(), i, BinaryenTypeAuto());
    blocks.setUniqueValue(currentBlock->getIdentifier(), x);
}

void WASMCompiler::generateBasicBlock(IR::Operations::BasicBlockInvocation& blockInvocation,
                                                       BinaryenExpressions expressions) {
    std::cout << "Handling generateBasicBlock!" << std::endl;
    auto targetBlock = blockInvocation.getBlock();
    // TODO Check if the block already exists. If yes -> phi node?
    if (!blocks.contains(targetBlock->getIdentifier())) {
        //return blocks.getValue(targetBlock->getIdentifier());

        auto targetBlockArguments = targetBlock->getArguments();
        // Add attributes as arguments to block.
        BinaryenExpressions nextExpressions;
        int i = 0;
        for (const auto& headBlockHeadTypes : targetBlockArguments) {
            //expressions.setValue(headBlockHeadTypes->getIdentifier(), expressions[i].second);
            //consumed.emplace(headBlockHeadTypes->getIdentifier(), expressions[i].second);
            nextExpressions.setValue(headBlockHeadTypes->getIdentifier(), expressions[i].second);
            consumed.emplace(headBlockHeadTypes->getIdentifier(), expressions[i].second);
            i++;
        }
        generateWASM(targetBlock, expressions);
    }
    /*
    auto bodyArray = std::make_unique<BinaryenExpressionRef[]>(expressions.size());
    int j = 0;
    for (const auto& exp : expressions.getMapping()) {
        if (!consumed.contains(exp.first)) {
            bodyArray[j] = exp.second;
            j++;
        }
    }

    BinaryenExpressionRef basicBlock = BinaryenBlock(wasm, targetBlock->getIdentifier().c_str(),
                                                     bodyArray.get(), j, BinaryenTypeAuto());
    //blocks.setUniqueValue(targetBlock->getIdentifier(), basicBlock);
    return basicBlock;
    */
}

BinaryenExpressionRef WASMCompiler::generateBody() {
    for (const auto& block : blocks.getMapping()) {
        relooperBlocks.setValue(block.first, RelooperAddBlock(relooper, block.second));
        std::cout << block.first << "=";
        BinaryenExpressionPrint(block.second);
    }
    //std::cout << "HERE: Size " << blockLinking.size() << ": "<< blockLinking[1].first << " - " << blockLinking[1].second << std::endl;
    for (const auto& link : blockLinking) {
        auto fromBlock = std::get<0>(link);
        auto toBlock = std::get<1>(link);
        auto condition = std::get<2>(link);
        std::cout << fromBlock << " -> " << toBlock << std::endl;
        //if (fromBlock != "2" && toBlock != "2") {
            if (condition != nullptr) {
                RelooperAddBranch(relooperBlocks.getValue(fromBlock), relooperBlocks.getValue(toBlock), condition, nullptr);
            } else {
                RelooperAddBranch(relooperBlocks.getValue(fromBlock), relooperBlocks.getValue(toBlock), nullptr, nullptr);
            }
        //}
    }
    return RelooperRenderAndDispose(relooper, relooperBlocks[0].second, 0);
}

void WASMCompiler::convertConstToLocal(std::string& key, BinaryenExpressions expressions) {
    auto constValue = expressions.getValue(key);
    localVariables.setValue(key, BinaryenExpressionGetType(constValue));
    BinaryenExpressionRef x = BinaryenLocalSet(wasm, argIndex + index, constValue);
    index++;
    //Remove Const since we are adding it as a local
    expressions.remove(key);
    expressions.setValue(key, x);
}
/*
BinaryenExpressionRef WASMCompiler::generateLocalGet(std::string& key) {
    return BinaryenLocalGet(wasm, localVariables.getIndex(key), localVariables.getValue(key));
}
*/
}// namespace NES::Nautilus::Backends::WASM