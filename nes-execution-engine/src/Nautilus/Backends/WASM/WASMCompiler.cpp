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

#include "/home/victor/binaryen/src/binaryen-c.h"
#include <Nautilus/IR/BasicBlocks/BasicBlock.hpp>
#include <Nautilus/Backends/WASM/WASMCompiler.hpp>
#include <utility>

namespace NES::Nautilus::Backends::WASM {

WASMCompiler::WASMCompiler() {}
BinaryenModuleRef WASMCompiler::generateWASM(std::shared_ptr<IR::IRGraph> ir) {
    BinaryenModuleRef module = BinaryenModuleCreate();
    generateWASM(ir->getRootOperation(), module);
    
    return module;
}

void WASMCompiler::generateWASM(IR::BasicBlockPtr basicBlock, BinaryenModuleRef& module) {
    for (const auto& operation : basicBlock->getOperations()) {
        generateWASM(operation, module);
    }
}

void WASMCompiler::generateWASM(const IR::Operations::OperationPtr& operation, BinaryenModuleRef& module) {
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

}// namespace NES::Nautilus::Backends::WASM