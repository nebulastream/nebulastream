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

#include <Nautilus/Backends/WASM/WASMCompiler.hpp>
#include <Nautilus/IR/BasicBlocks/BasicBlock.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Nautilus::Backends::WASM {

WASMCompiler::WASMCompiler() {}

BinaryenModuleRef WASMCompiler::compile(std::shared_ptr<IR::IRGraph> ir) {
    auto module = BinaryenModuleCreate();
    generateWASM(ir->getRootOperation(), module);
/*
    if (!BinaryenModuleValidate(module)) {
        //TODO: WASM incorrect, throw error
    }
*/
    return module;
}

void WASMCompiler::generateWASM(std::shared_ptr<IR::Operations::FunctionOperation> functionOp, BinaryenModuleRef& module) {
    std::cout << "Handling FunctionOp!" << std::endl;
    BinaryenExpressionRef functionBody;
    std::vector<BinaryenType> params;
    for (auto inputArg : functionOp->getFunctionBasicBlock()->getArguments()) {
        params.push_back(BinaryenTypeInt32());
    }
    if (!params.empty()) {
        BinaryenType params2 = BinaryenTypeCreate(params.data(), params.size());
        BinaryenAddFunction(module, functionOp->getName().c_str(), params2, BinaryenTypeInt32()
                                                                                , nullptr, 0, nullptr);
    } else {
        BinaryenAddFunction(module, functionOp->getName().c_str(), 0, BinaryenTypeInt32()
                                                                          , nullptr, 0, nullptr);
    }
    generateWASM(functionOp->getFunctionBasicBlock(), module);
}

void WASMCompiler::generateWASM(IR::BasicBlockPtr basicBlock, BinaryenExpressionRef& module) {
    std::cout << "Handling BasicBlock!" << std::endl;
    for (const auto& operation : basicBlock->getOperations()) {
        generateWASM(operation, module);
    }
}

void WASMCompiler::generateWASM(const IR::Operations::OperationPtr& operation, BinaryenExpressionRef& module) {
    std::cout << "Handling Operations" << std::endl;
    switch (operation->getOperationType()) {
        case IR::Operations::Operation::OperationType::FunctionOp:
            generateWASM(std::static_pointer_cast<IR::Operations::FunctionOperation>(operation), module);
            break;
        case IR::Operations::Operation::OperationType::LoopOp:
            generateWASM(std::static_pointer_cast<IR::Operations::LoopOperation>(operation), module);
            break;
        case IR::Operations::Operation::OperationType::ConstIntOp:
            generateWASM(std::static_pointer_cast<IR::Operations::ConstIntOperation>(operation), module);
            break;
        case IR::Operations::Operation::OperationType::ConstFloatOp:
            generateWASM(std::static_pointer_cast<IR::Operations::ConstFloatOperation>(operation), module);
            break;
        case IR::Operations::Operation::OperationType::AddOp:
            generateWASM(std::static_pointer_cast<IR::Operations::AddOperation>(operation), module);
            break;
        case IR::Operations::Operation::OperationType::SubOp:
            generateWASM(std::static_pointer_cast<IR::Operations::SubOperation>(operation), module);
            break;
        case IR::Operations::Operation::OperationType::MulOp:
            generateWASM(std::static_pointer_cast<IR::Operations::MulOperation>(operation), module);
            break;
        case IR::Operations::Operation::OperationType::DivOp:
            generateWASM(std::static_pointer_cast<IR::Operations::DivOperation>(operation), module);
            break;
        case IR::Operations::Operation::OperationType::LoadOp:
            generateWASM(std::static_pointer_cast<IR::Operations::LoadOperation>(operation), module);
            break;
        case IR::Operations::Operation::OperationType::AddressOp:
            generateWASM(std::static_pointer_cast<IR::Operations::AddressOperation>(operation), module);
            break;
        case IR::Operations::Operation::OperationType::StoreOp:
            generateWASM(std::static_pointer_cast<IR::Operations::StoreOperation>(operation), module);
            break;
        case IR::Operations::Operation::OperationType::BranchOp:
            generateWASM(std::static_pointer_cast<IR::Operations::BranchOperation>(operation), module);
            break;
        case IR::Operations::Operation::OperationType::IfOp:
            generateWASM(std::static_pointer_cast<IR::Operations::IfOperation>(operation), module);
            break;
        case IR::Operations::Operation::OperationType::CompareOp:
            generateWASM(std::static_pointer_cast<IR::Operations::CompareOperation>(operation), module);
            break;
        case IR::Operations::Operation::OperationType::ProxyCallOp:
            generateWASM(std::static_pointer_cast<IR::Operations::ProxyCallOperation>(operation), module);
            break;
        case IR::Operations::Operation::OperationType::ReturnOp:
            generateWASM(std::static_pointer_cast<IR::Operations::ReturnOperation>(operation), module);
            break;
        case IR::Operations::Operation::OperationType::OrOp:
            generateWASM(std::static_pointer_cast<IR::Operations::OrOperation>(operation), module);
            break;
        case IR::Operations::Operation::OperationType::AndOp:
            generateWASM(std::static_pointer_cast<IR::Operations::AndOperation>(operation), module);
            break;
        case IR::Operations::Operation::OperationType::NegateOp:
            generateWASM(std::static_pointer_cast<IR::Operations::NegateOperation>(operation), module);
            break;
        case IR::Operations::Operation::OperationType::CastOp:
            generateWASM(std::static_pointer_cast<IR::Operations::CastOperation>(operation), module);
            break;
        case IR::Operations::Operation::OperationType::ConstBooleanOp:
            generateWASM(std::static_pointer_cast<IR::Operations::ConstBooleanOperation>(operation), module);
            break;
        default: NES_NOT_IMPLEMENTED();
    }
}

void WASMCompiler::generateWASM(std::shared_ptr<IR::Operations::NegateOperation> negateOperation, BinaryenExpressionRef& module) {
    std::cout << "Handling NegateOp!" << std::endl;
    auto x = negateOperation->toString();
    BinaryenModulePrint(module);
}

void WASMCompiler::generateWASM(std::shared_ptr<IR::Operations::OrOperation> orOperation, BinaryenExpressionRef& module) {
    BinaryenModulePrint(module);
    auto x = orOperation->toString();
}

void WASMCompiler::generateWASM(std::shared_ptr<IR::Operations::AndOperation> andOperation, BinaryenExpressionRef& module) {
    BinaryenModulePrint(module);
    auto x = andOperation->toString();
}

void WASMCompiler::generateWASM(std::shared_ptr<IR::Operations::LoopOperation> loopOp, BinaryenExpressionRef& module) {
    BinaryenModulePrint(module);
    auto x = loopOp->toString();
}

//==--------------------------==//
//==-- MEMORY (LOAD, STORE) --==//
//==--------------------------==//
void WASMCompiler::generateWASM(std::shared_ptr<IR::Operations::AddressOperation> addressOp, BinaryenExpressionRef& module) {
    BinaryenModulePrint(module);
    auto x = addressOp->toString();
}

void WASMCompiler::generateWASM(std::shared_ptr<IR::Operations::LoadOperation> loadOp, BinaryenExpressionRef& module) {
    BinaryenModulePrint(module);
    auto x = loadOp->toString();
}

void WASMCompiler::generateWASM(std::shared_ptr<IR::Operations::ConstIntOperation> constIntOp, BinaryenExpressionRef& module) {
    std::cout << "Handling ConstIntOp!" << std::endl;
    auto constInt = BinaryenConst(module, BinaryenLiteralInt64(constIntOp->getConstantIntValue()));
}

void WASMCompiler::generateWASM(std::shared_ptr<IR::Operations::ConstFloatOperation> constFloatOp, BinaryenExpressionRef& module) {
    std::cout << "Handling ConstFloatOp!" << std::endl;
    BinaryenModulePrint(module);
    auto x = constFloatOp->toString();
}

//==---------------------------==//
//==-- ARITHMETIC OPERATIONS --==//
//==---------------------------==//
void WASMCompiler::generateWASM(std::shared_ptr<IR::Operations::AddOperation> addOp, BinaryenExpressionRef& module) {
    std::cout << "Handling AddOp!" << std::endl;
    auto left = addOp->getLeftInput()->getIdentifier();
    auto right = addOp->getRightInput()->getIdentifier();
    BinaryenModulePrint(module);
    auto x = addOp->toString();
}

void WASMCompiler::generateWASM(std::shared_ptr<IR::Operations::SubOperation> subIntOp, BinaryenExpressionRef& module) {
    BinaryenModulePrint(module);
    auto x = subIntOp->toString();
}

void WASMCompiler::generateWASM(std::shared_ptr<IR::Operations::MulOperation> mulOp, BinaryenExpressionRef& module) {
    BinaryenModulePrint(module);
    auto x = mulOp->toString();
}

void WASMCompiler::generateWASM(std::shared_ptr<IR::Operations::DivOperation> divIntOp, BinaryenExpressionRef& module) {
    BinaryenModulePrint(module);
    auto x = divIntOp->toString();
}

void WASMCompiler::generateWASM(std::shared_ptr<IR::Operations::StoreOperation> storeOp, BinaryenExpressionRef& module) {
    BinaryenModulePrint(module);
    auto x = storeOp->toString();
}

void WASMCompiler::generateWASM(std::shared_ptr<IR::Operations::ReturnOperation> returnOp, BinaryenExpressionRef& module) {
    std::cout << "Handling ReturnOp!" << std::endl;
    BinaryenModulePrint(module);
    auto x = returnOp->toString();
}

void WASMCompiler::generateWASM(std::shared_ptr<IR::Operations::ProxyCallOperation> proxyCallOp, BinaryenExpressionRef& module) {
    std::cout << "Handling ProxyCallOp!" << std::endl;
    BinaryenModulePrint(module);
    auto x = proxyCallOp->toString();
}

void WASMCompiler::generateWASM(std::shared_ptr<IR::Operations::CompareOperation> compareOp, BinaryenExpressionRef& module) {
    BinaryenModulePrint(module);
    auto x = compareOp->toString();
}

void WASMCompiler::generateWASM(std::shared_ptr<IR::Operations::IfOperation> ifOp, BinaryenExpressionRef& module) {
    BinaryenModulePrint(module);
    auto x = ifOp->toString();
}

void WASMCompiler::generateWASM(std::shared_ptr<IR::Operations::BranchOperation> branchOp, BinaryenExpressionRef& module) {
    BinaryenModulePrint(module);
    auto x = branchOp->toString();
}

void WASMCompiler::generateWASM(std::shared_ptr<IR::Operations::CastOperation> castOperation,
                                        BinaryenExpressionRef& module) {
    std::cout << "Handling CastOp!" << std::endl;
    BinaryenModulePrint(module);
    auto x = castOperation->toString();
}

void WASMCompiler::generateWASM(std::shared_ptr<IR::Operations::ConstBooleanOperation> constBooleanOp,
                                        BinaryenExpressionRef& module) {
    BinaryenModulePrint(module);
    auto x = constBooleanOp->toString();
}

}// namespace NES::Nautilus::Backends::WASM