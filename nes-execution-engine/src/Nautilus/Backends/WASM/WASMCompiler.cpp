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
#include <Nautilus/Util/Frame.hpp>
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
    std::vector<BinaryenType> params;
    for (auto inputArg : functionOp->getFunctionBasicBlock()->getArguments()) {
        //TODO: Type checking for parameter
        params.push_back(BinaryenTypeInt32());
    }
    /*
    if (!params.empty()) {
        BinaryenType params2 = BinaryenTypeCreate(params.data(), params.size());
        BinaryenAddFunction(expressions, functionOp->getName().c_str(), params2, BinaryenTypeInt32()
                                                                                , nullptr, 0, nullptr);
    } else {
        BinaryenAddFunction(expressions, functionOp->getName().c_str(), 0, BinaryenTypeInt32()
                                                                          , nullptr, 0, nullptr);
    }
    */
    generateWASM(functionOp->getFunctionBasicBlock(), bodyList);

    BinaryenExpressionRef bodyArray[100];
    int i = 0;
    for (const auto& exp : bodyList.getContent()) {
        if (!consumed.contains(exp.first)) {
            bodyArray[i] = exp.second;
            i++;
        }
    }

    BinaryenExpressionRef body =
        BinaryenBlock(wasm,
                      "body",
                      bodyArray,
                      i,
                      BinaryenTypeAuto());

    BinaryenAddFunction(wasm, functionOp->getName().c_str(), 0, BinaryenTypeInt32(), nullptr, 0, body);
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
    auto x = negateOperation->toString();
    auto y = expressions.contains("");
}

void WASMCompiler::generateWASM(std::shared_ptr<IR::Operations::OrOperation> orOperation, BinaryenExpressions& expressions) {
    auto x = orOperation->toString();
    auto y = expressions.contains("");
}

void WASMCompiler::generateWASM(std::shared_ptr<IR::Operations::AndOperation> andOperation, BinaryenExpressions& expressions) {
    auto x = andOperation->toString();
    auto y = expressions.contains("");
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

void WASMCompiler::generateWASM(std::shared_ptr<IR::Operations::SubOperation> subIntOp, BinaryenExpressions& expressions) {
    auto y = expressions.contains("");
    auto x = subIntOp->toString();
}

void WASMCompiler::generateWASM(std::shared_ptr<IR::Operations::MulOperation> mulOp, BinaryenExpressions& expressions) {
    std::cout << "Handling MulOp!" << std::endl;
    auto left = expressions.getValue(mulOp->getLeftInput()->getIdentifier());
    auto right = expressions.getValue(mulOp->getRightInput()->getIdentifier());
    auto type = BinaryenExpressionGetType(left);
    BinaryenExpressionRef add;
    if (type == BinaryenTypeInt32()) {
        add = BinaryenBinary(wasm, BinaryenMulInt32(), left, right);
    } else if (type == BinaryenTypeInt64()) {
        add = BinaryenBinary(wasm, BinaryenMulInt64(), left, right);
    } else if (type == BinaryenTypeFloat32()) {
        add = BinaryenBinary(wasm, BinaryenMulFloat32(), left, right);
    } else {
        add = BinaryenBinary(wasm, BinaryenMulFloat64(), left, right);
    }

    consumed.emplace(mulOp->getLeftInput()->getIdentifier(), left);
    consumed.emplace(mulOp->getRightInput()->getIdentifier(), right);
    expressions.setValue(mulOp->getIdentifier(), add);
}

void WASMCompiler::generateWASM(std::shared_ptr<IR::Operations::DivOperation> divIntOp, BinaryenExpressions& expressions) {
    auto y = expressions.contains("");
    auto x = divIntOp->toString();
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
    auto y = expressions.contains("");
    auto x = compareOp->toString();
}

void WASMCompiler::generateWASM(std::shared_ptr<IR::Operations::IfOperation> ifOp, BinaryenExpressions& expressions) {
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
    auto y = expressions.contains("");
    auto x = constBooleanOp->toString();
}

}// namespace NES::Nautilus::Backends::WASM