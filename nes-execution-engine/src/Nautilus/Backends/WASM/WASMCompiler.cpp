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

    generateWASM(ir->getRootOperation());

    BinaryenModuleAutoDrop(wasm);
    if (!BinaryenModuleValidate(wasm)) {
        std::cout << "Generated pre-optimized wasm is incorrect!" << std::endl;
    }
    BinaryenModuleOptimize(wasm);
    BinaryenModulePrintStackIR(wasm, false);
    return wasm;
}

void WASMCompiler::generateWASM(const std::shared_ptr<IR::Operations::FunctionOperation>& functionOp) {
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
        localVarMapping.setValue(inputArg->getIdentifier(), argExpression);
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
    BinaryenExpressionRef body = generateBody();
    BinaryenAddFunction(wasm, functionOp->getName().c_str(), arguments,
                        getBinaryenType(functionOp->getOutputArg()),
                        varArray.get(), index, body);
    BinaryenAddFunctionExport(wasm, functionOp->getName().c_str(), functionOp->getName().c_str());
    //TODO Add memory
}

void WASMCompiler::generateWASM(const IR::BasicBlockPtr& basicBlock, BinaryenExpressions& expressions) {
    currentBlock = basicBlock;
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

void WASMCompiler::generateWASM(const std::shared_ptr<IR::Operations::NegateOperation>& negateOperation, BinaryenExpressions& expressions) {
    auto input = expressions.getValue(negateOperation->getInput()->getIdentifier());
    auto x = negateOperation->toString();
    auto y = expressions.size();
}

void WASMCompiler::generateWASM(const std::shared_ptr<IR::Operations::OrOperation>& orOperation, BinaryenExpressions& expressions) {
    auto left = expressions.getValue(orOperation->getLeftInput()->getIdentifier());
    auto right = expressions.getValue(orOperation->getRightInput()->getIdentifier());
    BinaryenExpressionRef orOp = BinaryenBinary(wasm, BinaryenOrInt32(), left, right);

    consumed.emplace(orOperation->getLeftInput()->getIdentifier(), left);
    consumed.emplace(orOperation->getRightInput()->getIdentifier(), right);
    expressions.setValue(orOperation->getIdentifier(), orOp);
}

void WASMCompiler::generateWASM(const std::shared_ptr<IR::Operations::AndOperation>& andOperation, BinaryenExpressions& expressions) {
    auto left = expressions.getValue(andOperation->getLeftInput()->getIdentifier());
    auto right = expressions.getValue(andOperation->getRightInput()->getIdentifier());
    BinaryenExpressionRef andOp = BinaryenBinary(wasm, BinaryenAndInt32(), left, right);

    consumed.emplace(andOperation->getLeftInput()->getIdentifier(), left);
    consumed.emplace(andOperation->getRightInput()->getIdentifier(), right);
    expressions.setValue(andOperation->getIdentifier(), andOp);
}

void WASMCompiler::generateWASM(const std::shared_ptr<IR::Operations::LoopOperation>& loopOp, BinaryenExpressions& expressions) {
    auto x = loopOp->toString();
    auto y = expressions.size();
}

void WASMCompiler::generateWASM(const std::shared_ptr<IR::Operations::AddressOperation>& addressOp, BinaryenExpressions& expressions) {
    auto x = addressOp->toString();
    auto y = expressions.size();
}

void WASMCompiler::generateWASM(const std::shared_ptr<IR::Operations::LoadOperation>& loadOp, BinaryenExpressions& expressions) {
    auto x = loadOp->toString();
    auto y = expressions.size();
}

void WASMCompiler::generateWASM(const std::shared_ptr<IR::Operations::ConstIntOperation>& constIntOp, BinaryenExpressions& expressions) {
    if (auto integerStamp = cast_if<IR::Types::IntegerStamp>(constIntOp->getStamp().get())) {
        if (integerStamp->getBitWidth() == IR::Types::IntegerStamp::I64) {
            expressions.setValue(constIntOp->getIdentifier(),
                                 BinaryenConst(wasm, BinaryenLiteralInt64(constIntOp->getConstantIntValue())));
        } else {
            expressions.setValue(constIntOp->getIdentifier(),
                                 BinaryenConst(wasm, BinaryenLiteralInt32((int32_t)constIntOp->getConstantIntValue())));
        }
        consumed.emplace(constIntOp->getIdentifier(), expressions.getValue(constIntOp->getIdentifier()));
    }
}

void WASMCompiler::generateWASM(const std::shared_ptr<IR::Operations::ConstFloatOperation>& constFloatOp, BinaryenExpressions& expressions) {
    if (auto floatStamp = cast_if<IR::Types::FloatStamp>(constFloatOp->getStamp().get())) {
        if (floatStamp->getBitWidth() == IR::Types::FloatStamp::F64) {
            expressions.setValue(constFloatOp->getIdentifier(),
                                 BinaryenConst(wasm, BinaryenLiteralFloat64(constFloatOp->getConstantFloatValue())));
        } else {
            expressions.setValue(constFloatOp->getIdentifier(),
                                 BinaryenConst(wasm, BinaryenLiteralFloat32((float)constFloatOp->getConstantFloatValue())));
        }
        consumed.emplace(constFloatOp->getIdentifier(), expressions.getValue(constFloatOp->getIdentifier()));
    }
}

void WASMCompiler::generateWASM(const std::shared_ptr<IR::Operations::AddOperation>& addOp, BinaryenExpressions& expressions) {
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

void WASMCompiler::generateWASM(const std::shared_ptr<IR::Operations::SubOperation>& subOp, BinaryenExpressions& expressions) {
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

void WASMCompiler::generateWASM(const std::shared_ptr<IR::Operations::MulOperation>& mulOp, BinaryenExpressions& expressions) {
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

void WASMCompiler::generateWASM(const std::shared_ptr<IR::Operations::DivOperation>& divOp, BinaryenExpressions& expressions) {
    auto left = expressions.getValue(divOp->getLeftInput()->getIdentifier());
    auto right = expressions.getValue(divOp->getRightInput()->getIdentifier());
    if (divOp->getStamp()->isFloat()) {

    }
    consumed.emplace(divOp->getLeftInput()->getIdentifier(), left);
    consumed.emplace(divOp->getRightInput()->getIdentifier(), right);
}

void WASMCompiler::generateWASM(const std::shared_ptr<IR::Operations::StoreOperation>& storeOp, BinaryenExpressions& expressions) {
    //std::cout << "Handling StoreOp!" << std::endl;
    auto y = expressions.size();
    auto x = storeOp->toString();
}

void WASMCompiler::generateWASM(const std::shared_ptr<IR::Operations::ProxyCallOperation>& proxyCallOp, BinaryenExpressions& expressions) {
    //std::cout << "Handling ProxyCallOp!" << std::endl;
    auto y = expressions.size();
    auto x = proxyCallOp->toString();
}

void WASMCompiler::generateWASM(const std::shared_ptr<IR::Operations::CompareOperation>& compareOp, BinaryenExpressions& expressions) {
    auto left = expressions.getValue(compareOp->getLeftInput()->getIdentifier());
    auto right = expressions.getValue(compareOp->getRightInput()->getIdentifier());
    auto leftType = getBinaryenType(compareOp->getLeftInput()->getStamp());
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
    expressions.setValue(compareOp->getIdentifier(), compExpression);
}

void WASMCompiler::generateWASM(const std::shared_ptr<IR::Operations::CastOperation>& castOperation,
                                        BinaryenExpressions& expressions) {
    auto y = expressions.size();
    auto x = castOperation->toString();
}

void WASMCompiler::generateWASM(const std::shared_ptr<IR::Operations::ConstBooleanOperation>& constBooleanOp,
                                        BinaryenExpressions& expressions) {
    if (constBooleanOp->getValue()) {
        expressions.setValue(constBooleanOp->getIdentifier(),
                             BinaryenConst(wasm, BinaryenLiteralInt32(1)));
    } else {
        expressions.setValue(constBooleanOp->getIdentifier(),
                             BinaryenConst(wasm, BinaryenLiteralInt32(0)));
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

void WASMCompiler::generateWASM(const std::shared_ptr<IR::Operations::ReturnOperation>& returnOp, BinaryenExpressions& expressions) {
    BinaryenExpressionRef ret;
    if (!returnOp->hasReturnValue()) {
        ret = BinaryenReturn(wasm, nullptr);
    } else {
        auto returnExp = expressions.getValue(returnOp->getReturnValue()->getIdentifier());
        ret = BinaryenReturn(wasm, returnExp);
        consumed.emplace(returnOp->getReturnValue()->getIdentifier(), returnExp);
    }
    expressions.setValue(returnOp->getIdentifier(), ret);
    genBody(expressions);
}

void WASMCompiler::generateWASM(const std::shared_ptr<IR::Operations::BranchOperation>& branchOp, BinaryenExpressions& expressions) {
    BinaryenExpressions blockArgs;
    auto currentTrueArguments = branchOp->getNextBlockInvocation().getArguments();
    auto nextTrueArguments = branchOp->getNextBlockInvocation().getBlock()->getArguments();
    for(int i = 0; i < (int)branchOp->getNextBlockInvocation().getArguments().size(); i++) {
        BinaryenExpressionRef localGet;
        auto currVar = expressions.getValue(currentTrueArguments[i]->getIdentifier());

        /**
         * local.get corresponds to BinaryenExpressionGetId(expression) == 8
         */
        if (BinaryenExpressionGetId(currVar) == 8) {
            localGet = currVar;
        } else {
        /**
         * The local variable for the next block already exists
         */
            if (localVarMapping.contains("set_" + nextTrueArguments[i]->getIdentifier())) {
                auto u =
                    BinaryenLocalSet(wasm,
                                     BinaryenLocalSetGetIndex(localVarMapping.getValue("set_" + nextTrueArguments[i]->getIdentifier())),
                                     expressions.getValue(currentTrueArguments[i]->getIdentifier()));
                expressions.setValue("set_" + nextTrueArguments[i]->getIdentifier(), u);
                localGet = localVarMapping.getValue(nextTrueArguments[i]->getIdentifier());
            }
            /**
            * Create a new local variable which will be passed into the next block as a block argument
            */
            else {
                localVariables.setUniqueValue(nextTrueArguments[i]->getIdentifier(), BinaryenTypeInt32());
                auto x = BinaryenLocalSet(wasm, argIndex + index, expressions.getValue(currentTrueArguments[i]->getIdentifier()));
                expressions.setValue("set_" + nextTrueArguments[i]->getIdentifier(), x);
                localVarMapping.setValue("set_" + nextTrueArguments[i]->getIdentifier(), x);
                localGet = BinaryenLocalGet(wasm, argIndex + index, BinaryenTypeInt32());
                expressions.setValue(nextTrueArguments[i]->getIdentifier(), localGet);
                localVarMapping.setValue(nextTrueArguments[i]->getIdentifier(), localGet);
                consumed.emplace(nextTrueArguments[i]->getIdentifier(), localGet);
                index++;
            }
        }
        blockArgs.setValue(nextTrueArguments[i]->getIdentifier(), localGet);
    }
    blockLinking.emplace_back(std::make_tuple(currentBlock->getIdentifier(),
                                              branchOp->getNextBlockInvocation().getBlock()->getIdentifier(),
                                              nullptr));
    genBody(expressions);
    generateBasicBlock(branchOp->getNextBlockInvocation(), blockArgs);
}

void WASMCompiler::generateWASM(std::shared_ptr<IR::Operations::IfOperation> ifOp, BinaryenExpressions& expressions) {
    BinaryenExpressions trueBlockArgs, falseBlockArgs;

    blockLinking.emplace_back(std::make_tuple(currentBlock->getIdentifier(),
                                              ifOp->getFalseBlockInvocation().getBlock()->getIdentifier(),
                                              nullptr));
    /**
     * We link to the true block which needs to satisfy the condition. Therefore, save the condition
     * for creating the branch later on in generateBody()
     */
    blockLinking.emplace_back(std::make_tuple(currentBlock->getIdentifier(),
                                              ifOp->getTrueBlockInvocation().getBlock()->getIdentifier(),
                                              expressions.getValue(ifOp->getValue()->getIdentifier())));
    consumed.emplace(ifOp->getValue()->getIdentifier(), expressions.getValue(ifOp->getValue()->getIdentifier()));

    auto currentTrueArguments = ifOp->getTrueBlockInvocation().getArguments();
    auto nextTrue = ifOp->getTrueBlockInvocation().getBlock()->getArguments();
    for(int i = 0; i < (int)ifOp->getTrueBlockInvocation().getArguments().size(); i++) {
        BinaryenExpressionRef localGet;
        auto currVar = expressions.getValue(currentTrueArguments[i]->getIdentifier());
        if (BinaryenExpressionGetId(currVar) == 8) {
            localGet = currVar;
        } else {
            if (localVarMapping.contains("set_" + nextTrue[i]->getIdentifier())) {
                auto u =
                    BinaryenLocalSet(wasm,
                                     BinaryenLocalSetGetIndex(localVarMapping.getValue("set_" + nextTrue[i]->getIdentifier())),
                                     expressions.getValue(currentTrueArguments[i]->getIdentifier()));
                expressions.setValue("set_" + nextTrue[i]->getIdentifier(), u);
                localGet = localVarMapping.getValue(nextTrue[i]->getIdentifier());
            } else {
                //TODO: Retrieve type!
                localVariables.setUniqueValue(nextTrue[i]->getIdentifier(), BinaryenTypeInt32());
                auto x = BinaryenLocalSet(wasm, argIndex + index, expressions.getValue(currentTrueArguments[i]->getIdentifier()));
                expressions.setValue("set_" + nextTrue[i]->getIdentifier(), x);
                localVarMapping.setValue("set_" + nextTrue[i]->getIdentifier(), x);
                localGet = BinaryenLocalGet(wasm, argIndex + index, BinaryenTypeInt32());
                expressions.setValue(nextTrue[i]->getIdentifier(), localGet);
                localVarMapping.setValue(nextTrue[i]->getIdentifier(), localGet);
                consumed.emplace(nextTrue[i]->getIdentifier(), localGet);
                index++;
            }
        }
        trueBlockArgs.setValue(nextTrue[i]->getIdentifier(), localGet);
    }

    auto currentFalse = ifOp->getFalseBlockInvocation().getArguments();
    auto nextFalse = ifOp->getFalseBlockInvocation().getBlock()->getArguments();
    for(int i = 0; i < (int)ifOp->getFalseBlockInvocation().getArguments().size(); i++) {
        BinaryenExpressionRef localGet;
        auto currVar = expressions.getValue(currentFalse[i]->getIdentifier());
        if (BinaryenExpressionGetId(currVar) == 8) {
            localGet = currVar;
        } else {
            if (localVarMapping.contains("set_" + nextFalse[i]->getIdentifier())) {
                auto u =
                    BinaryenLocalSet(wasm,
                                     BinaryenLocalSetGetIndex(localVarMapping.getValue("set_" + nextFalse[i]->getIdentifier())),
                                     expressions.getValue(currentFalse[i]->getIdentifier()));
                expressions.setValue("set_" + nextFalse[i]->getIdentifier(), u);
                localGet = localVarMapping.getValue(nextFalse[i]->getIdentifier());
            } else {
                localVariables.setUniqueValue(nextFalse[i]->getIdentifier(), BinaryenTypeInt32());
                auto x = BinaryenLocalSet(wasm, argIndex + index, expressions.getValue(currentFalse[i]->getIdentifier()));
                expressions.setValue("set_" + nextFalse[i]->getIdentifier(), x);
                localVarMapping.setValue("set_" + nextFalse[i]->getIdentifier(), x);
                localGet = BinaryenLocalGet(wasm, argIndex + index, BinaryenTypeInt32());
                expressions.setValue(nextFalse[i]->getIdentifier(), localGet);
                localVarMapping.setValue(nextFalse[i]->getIdentifier(), localGet);
                consumed.emplace(nextFalse[i]->getIdentifier(), localGet);
                index++;
            }
        }
        falseBlockArgs.setValue(nextFalse[i]->getIdentifier(), localGet);
    }
    genBody(expressions);
    generateBasicBlock(ifOp->getTrueBlockInvocation(), trueBlockArgs);
    generateBasicBlock(ifOp->getFalseBlockInvocation(), falseBlockArgs);
}

void WASMCompiler::genBody(BinaryenExpressions expressions) {
    auto bodyArray = std::make_unique<BinaryenExpressionRef[]>(expressions.size());
    int i = 0;
    for (const auto& exp : expressions.getMapping()) {
        if (!consumed.contains(exp.first)) {
            bodyArray[i] = exp.second;
            i++;
        }
    }
    auto x = BinaryenBlock(wasm, currentBlock->getIdentifier().c_str(), bodyArray.get(), i, BinaryenTypeAuto());
    blocks.setUniqueValue(currentBlock->getIdentifier(), x);
}

void WASMCompiler::generateBasicBlock(IR::Operations::BasicBlockInvocation& blockInvocation,
                                                       BinaryenExpressions expressions) {
    auto targetBlock = blockInvocation.getBlock();
    if (!blocks.contains(targetBlock->getIdentifier())) {
        generateWASM(targetBlock, expressions);
    }
}

BinaryenType WASMCompiler::getBinaryenType(const IR::Types::StampPtr& stampPtr) {
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

BinaryenExpressionRef WASMCompiler::generateBody() {
    for (const auto& block : blocks.getMapping()) {
        relooperBlocks.setValue(block.first, RelooperAddBlock(relooper, block.second));
    }
    for (const auto& link : blockLinking) {
        auto fromBlock = std::get<0>(link);
        auto toBlock = std::get<1>(link);
        auto condition = std::get<2>(link);
        if (condition != nullptr) {
            RelooperAddBranch(relooperBlocks.getValue(fromBlock), relooperBlocks.getValue(toBlock), condition, nullptr);
        } else {
            RelooperAddBranch(relooperBlocks.getValue(fromBlock), relooperBlocks.getValue(toBlock), nullptr, nullptr);
        }
    }
    return RelooperRenderAndDispose(relooper, relooperBlocks[0].second, 0);
}

}// namespace NES::Nautilus::Backends::WASM