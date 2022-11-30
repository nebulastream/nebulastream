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
#include <Experimental/Babelfish/IRSerialization.hpp>
#include <Nautilus/IR/Operations/ArithmeticOperations/AddOperation.hpp>
#include <Nautilus/IR/Operations/ArithmeticOperations/MulOperation.hpp>
#include <Nautilus/IR/Operations/BranchOperation.hpp>
#include <Nautilus/IR/Operations/ConstBooleanOperation.hpp>
#include <Nautilus/IR/Operations/ConstIntOperation.hpp>
#include <Nautilus/IR/Operations/IfOperation.hpp>
#include <Nautilus/IR/Operations/LoadOperation.hpp>
#include <Nautilus/IR/Operations/LogicalOperations/AndOperation.hpp>
#include <Nautilus/IR/Operations/LogicalOperations/CompareOperation.hpp>
#include <Nautilus/IR/Operations/LogicalOperations/NegateOperation.hpp>
#include <Nautilus/IR/Operations/Loop/LoopOperation.hpp>
#include <Nautilus/IR/Operations/ProxyCallOperation.hpp>
#include <Nautilus/IR/Operations/ReturnOperation.hpp>
#include <Nautilus/IR/Operations/StoreOperation.hpp>
#include <Util/Logger/Logger.hpp>
#include <nlohmann/json.hpp>

namespace NES::ExecutionEngine::Experimental {

std::string IRSerialization::serialize(std::shared_ptr<IR::NESIR> ir) {
    nlohmann::json irJson{};
    irJson["id"] = 42;

    auto rootBlock = ir->getRootOperation()->getFunctionBasicBlock();
    serializeBlock(rootBlock);

    irJson["blocks"] = blocks;

    return irJson.serialize();
}

void IRSerialization::serializeBlock(std::shared_ptr<IR::BasicBlock> block) {
    blocks.emplace_back(nlohmann::json{});
    nlohmann::json& blockJson = blocks.back();

    blockJson["id"] = block->getIdentifier();
    std::vector<nlohmann::json> arguments;
    for (auto arg : block->getArguments()) {
        arguments.emplace_back(arg->getIdentifier());
    }
    blockJson["arguments"] = arguments;
    std::vector<nlohmann::json> jsonOperations;
    for (auto op : block->getOperations()) {
        serializeOperation(op, jsonOperations);
    }
    for (auto& jsonBlock : blocks) {
        if ((jsonBlock["id"].as_string()) == block->getIdentifier()) {
            jsonBlock["operations"] = jsonOperations;
        }
    }
}

void IRSerialization::serializeOperation(std::shared_ptr<IR::Operations::Operation> operation,
                                         std::vector<nlohmann::json>& jsonOperations) {
    jsonOperations.emplace_back(nlohmann::json{});
    ;
    nlohmann::json& opJson = jsonOperations.back();
    ;
    opJson["stamp"] = operation->getStamp()->toString();
    opJson["id"] = operation->getIdentifier();
    if (operation->getOperationType() == IR::Operations::Operation::ConstIntOp) {
        auto constOp = std::static_pointer_cast<IR::Operations::ConstIntOperation>(operation);
        opJson["type"] = "ConstInt";
        opJson["value"] = constOp->getConstantIntValue();
    } else if (operation->getOperationType() == IR::Operations::Operation::ConstBooleanOp) {
        auto constOp = std::static_pointer_cast<IR::Operations::ConstBooleanOperation>(operation);
        opJson["type"] = "ConstBool";
        opJson["value"] = constOp->getValue();
    } else if (operation->getOperationType() == IR::Operations::Operation::AddOp) {
        auto constOp = std::static_pointer_cast<IR::Operations::AddOperation>(operation);
        opJson["type"] = "Add";
        opJson["left"] = constOp->getLeftInput()->getIdentifier();
        opJson["right"] = constOp->getRightInput()->getIdentifier();
    } else if (operation->getOperationType() == IR::Operations::Operation::SubOp) {
        auto constOp = std::static_pointer_cast<IR::Operations::AddOperation>(operation);
        opJson["type"] = "Sub";
        opJson["left"] = constOp->getLeftInput()->getIdentifier();
        opJson["right"] = constOp->getRightInput()->getIdentifier();
    } else if (operation->getOperationType() == IR::Operations::Operation::MulOp) {
        auto mulOp = std::static_pointer_cast<IR::Operations::MulOperation>(operation);
        opJson["type"] = "Mul";
        opJson["left"] = mulOp->getLeftInput()->getIdentifier();
        opJson["right"] = mulOp->getRightInput()->getIdentifier();
    } else if (operation->getOperationType() == IR::Operations::Operation::ReturnOp) {
        auto constOp = std::static_pointer_cast<IR::Operations::ReturnOperation>(operation);
        opJson["type"] = "Return";
        if (constOp->hasReturnValue())
            opJson["value"] = constOp->getReturnValue()->getIdentifier();
    } else if (operation->getOperationType() == IR::Operations::Operation::LoopOp) {
        auto loopOperation = std::static_pointer_cast<IR::Operations::LoopOperation>(operation);
        opJson["type"] = "Loop";
        opJson["head"] = serializeOperation(loopOperation->getLoopHeadBlock());

    } else if (operation->getOperationType() == IR::Operations::Operation::IfOp) {
        auto ifOp = std::static_pointer_cast<IR::Operations::IfOperation>(operation);
        opJson["type"] = "If";
        opJson["input"] = ifOp->getValue()->getIdentifier();
        opJson["trueCase"] = serializeOperation(ifOp->getTrueBlockInvocation());
        opJson["falseCase"] = serializeOperation(ifOp->getFalseBlockInvocation());
    } else if (operation->getOperationType() == IR::Operations::Operation::CompareOp) {
        auto compOp = std::static_pointer_cast<IR::Operations::CompareOperation>(operation);
        opJson["type"] = "Compare";
        opJson["left"] = compOp->getLeftInput()->getIdentifier();
        opJson["right"] = compOp->getRightInput()->getIdentifier();
        switch (compOp->getComparator()) {
            case IR::Operations::CompareOperation::Comparator::IEQ: {
                opJson["Compare"] = "EQ";
                break;
            }
            case IR::Operations::CompareOperation::Comparator::ISLT: {
                opJson["Compare"] = "LT";
                break;
            }
            default: {
            }
        }
    } else if (operation->getOperationType() == IR::Operations::Operation::AndOp) {
        auto andOp = std::static_pointer_cast<IR::Operations::AndOperation>(operation);
        opJson["type"] = "And";
        opJson["left"] = andOp->getLeftInput()->getIdentifier();
        opJson["right"] = andOp->getRightInput()->getIdentifier();
    } else if (operation->getOperationType() == IR::Operations::Operation::NegateOp) {
        auto negate = std::static_pointer_cast<IR::Operations::NegateOperation>(operation);
        opJson["type"] = "Negate";
        opJson["input"] = negate->getInput()->getIdentifier();
    } else if (operation->getOperationType() == IR::Operations::Operation::BranchOp) {
        auto compOp = std::static_pointer_cast<IR::Operations::BranchOperation>(operation);
        opJson["type"] = "Branch";
        opJson["next"] = serializeOperation(compOp->getNextBlockInvocation());
    } else if (operation->getOperationType() == IR::Operations::Operation::LoadOp) {
        auto loadOp = std::static_pointer_cast<IR::Operations::LoadOperation>(operation);
        opJson["type"] = "Load";
        opJson["address"] = loadOp->getAddress()->getIdentifier();
    } else if (operation->getOperationType() == IR::Operations::Operation::StoreOp) {
        auto storeOp = std::static_pointer_cast<IR::Operations::StoreOperation>(operation);
        opJson["type"] = "Store";
        opJson["address"] = storeOp->getAddress()->getIdentifier();
        opJson["value"] = storeOp->getValue()->getIdentifier();
    } else if (operation->getOperationType() == IR::Operations::Operation::ProxyCallOp) {
        auto proxyCallOp = std::static_pointer_cast<IR::Operations::ProxyCallOperation>(operation);
        opJson["type"] = "call";
        opJson["symbol"] = proxyCallOp->getFunctionSymbol();
        std::vector<nlohmann::json> arguments;
        for (auto arg : proxyCallOp->getInputArguments()) {
            arguments.emplace_back(arg->getIdentifier());
        }
        opJson["arguments"] = arguments;
    } else {
        NES_THROW_RUNTIME_ERROR("operator is not known: " << operation->toString());
    }
}

nlohmann::json IRSerialization::serializeOperation(IR::Operations::BasicBlockInvocation& blockInvocation) {

    bool containsBlock = false;
    for (auto& block : blocks) {

        if ((block["id"].as_string()) == blockInvocation.getBlock()->getIdentifier()) {
            containsBlock = true;
        }
    }

    if (!containsBlock) {
        serializeBlock(blockInvocation.getBlock());
    }

    nlohmann::json opJson{};
    opJson["target"] = blockInvocation.getBlock()->getIdentifier();
    std::vector<nlohmann::json> arguments;
    for (auto arg : blockInvocation.getArguments()) {
        arguments.emplace_back(arg->getIdentifier());
    }
    opJson["arguments"] = arguments;
    return opJson;
}

}// namespace NES::ExecutionEngine::Experimental