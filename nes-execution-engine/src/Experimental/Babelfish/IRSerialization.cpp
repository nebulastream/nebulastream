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
#include "Experimental/NESIR/Operations/ArithmeticOperations/MulOperation.hpp"
#include "Experimental/NESIR/Operations/BranchOperation.hpp"
#include "Experimental/NESIR/Operations/ConstBooleanOperation.hpp"
#include "Experimental/NESIR/Operations/IfOperation.hpp"
#include "Experimental/NESIR/Operations/LogicalOperations/AndOperation.hpp"
#include "Experimental/NESIR/Operations/LogicalOperations/CompareOperation.hpp"
#include "Experimental/NESIR/Operations/LogicalOperations/NegateOperation.hpp"
#include "Experimental/NESIR/Operations/Loop/LoopOperation.hpp"
#include "Experimental/NESIR/Operations/ProxyCallOperation.hpp"
#include <Experimental/Babelfish/IRSerialization.hpp>
#include <Experimental/NESIR/Operations/ArithmeticOperations/AddOperation.hpp>
#include <Experimental/NESIR/Operations/ConstIntOperation.hpp>
#include <Experimental/NESIR/Operations/LoadOperation.hpp>
#include <Experimental/NESIR/Operations/ReturnOperation.hpp>
#include <Experimental/NESIR/Operations/StoreOperation.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::ExecutionEngine::Experimental {

std::string IRSerialization::serialize(std::shared_ptr<IR::NESIR> ir) {
    web::json::value irJson{};
    irJson["id"] = web::json::value(42);

    auto rootBlock = ir->getRootOperation()->getFunctionBasicBlock();
    serializeBlock(rootBlock);

    irJson["blocks"] = web::json::value::array(blocks);

    return irJson.serialize();
}

void IRSerialization::serializeBlock(std::shared_ptr<IR::BasicBlock> block) {
    blocks.emplace_back(web::json::value{});
    web::json::value& blockJson = blocks.back();

    blockJson["id"] = web::json::value(block->getIdentifier());
    std::vector<web::json::value> arguments;
    for (auto arg : block->getArguments()) {
        arguments.emplace_back(arg->getIdentifier());
    }
    blockJson["arguments"] = web::json::value::array(arguments);
    std::vector<web::json::value> jsonOperations;
    for (auto op : block->getOperations()) {
        serializeOperation(op, jsonOperations);
    }
    for (auto& jsonBlock : blocks) {
        if ((jsonBlock["id"].as_string()) == block->getIdentifier()) {
            jsonBlock["operations"] = web::json::value::array(jsonOperations);
        }
    }
}

void IRSerialization::serializeOperation(std::shared_ptr<IR::Operations::Operation> operation,
                                         std::vector<web::json::value>& jsonOperations) {
    jsonOperations.emplace_back(web::json::value{});
    ;
    web::json::value& opJson = jsonOperations.back();
    ;
    opJson["stamp"] = web::json::value(operation->getStamp()->toString());
    opJson["id"] = web::json::value(operation->getIdentifier());
    if (operation->getOperationType() == IR::Operations::Operation::ConstIntOp) {
        auto constOp = std::static_pointer_cast<IR::Operations::ConstIntOperation>(operation);
        opJson["type"] = web::json::value("ConstInt");
        opJson["value"] = web::json::value(constOp->getConstantIntValue());
    } else if (operation->getOperationType() == IR::Operations::Operation::ConstBooleanOp) {
        auto constOp = std::static_pointer_cast<IR::Operations::ConstBooleanOperation>(operation);
        opJson["type"] = web::json::value("ConstBool");
        opJson["value"] = web::json::value(constOp->getValue());
    } else if (operation->getOperationType() == IR::Operations::Operation::AddOp) {
        auto constOp = std::static_pointer_cast<IR::Operations::AddOperation>(operation);
        opJson["type"] = web::json::value("Add");
        opJson["left"] = web::json::value(constOp->getLeftInput()->getIdentifier());
        opJson["right"] = web::json::value(constOp->getRightInput()->getIdentifier());
    } else if (operation->getOperationType() == IR::Operations::Operation::SubOp) {
        auto constOp = std::static_pointer_cast<IR::Operations::AddOperation>(operation);
        opJson["type"] = web::json::value("Sub");
        opJson["left"] = web::json::value(constOp->getLeftInput()->getIdentifier());
        opJson["right"] = web::json::value(constOp->getRightInput()->getIdentifier());
    } else if (operation->getOperationType() == IR::Operations::Operation::MulOp) {
        auto mulOp = std::static_pointer_cast<IR::Operations::MulOperation>(operation);
        opJson["type"] = web::json::value("Mul");
        opJson["left"] = web::json::value(mulOp->getLeftInput()->getIdentifier());
        opJson["right"] = web::json::value(mulOp->getRightInput()->getIdentifier());
    } else if (operation->getOperationType() == IR::Operations::Operation::ReturnOp) {
        auto constOp = std::static_pointer_cast<IR::Operations::ReturnOperation>(operation);
        opJson["type"] = web::json::value("Return");
        if (constOp->hasReturnValue())
            opJson["value"] = web::json::value(constOp->getReturnValue()->getIdentifier());
    } else if (operation->getOperationType() == IR::Operations::Operation::LoopOp) {
        auto loopOperation = std::static_pointer_cast<IR::Operations::LoopOperation>(operation);
        opJson["type"] = web::json::value("Loop");
        opJson["head"] = serializeOperation(loopOperation->getLoopHeadBlock());

    } else if (operation->getOperationType() == IR::Operations::Operation::IfOp) {
        auto ifOp = std::static_pointer_cast<IR::Operations::IfOperation>(operation);
        opJson["type"] = web::json::value("If");
        opJson["input"] = web::json::value(ifOp->getValue()->getIdentifier());
        opJson["trueCase"] = serializeOperation(ifOp->getTrueBlockInvocation());
        opJson["falseCase"] = serializeOperation(ifOp->getFalseBlockInvocation());
    } else if (operation->getOperationType() == IR::Operations::Operation::CompareOp) {
        auto compOp = std::static_pointer_cast<IR::Operations::CompareOperation>(operation);
        opJson["type"] = web::json::value("Compare");
        opJson["left"] = web::json::value(compOp->getLeftInput()->getIdentifier());
        opJson["right"] = web::json::value(compOp->getRightInput()->getIdentifier());
        switch (compOp->getComparator()) {
            case IR::Operations::CompareOperation::Comparator::IEQ: {
                opJson["Compare"] = web::json::value("EQ");
                break;
            }
            case IR::Operations::CompareOperation::Comparator::ISLT: {
                opJson["Compare"] = web::json::value("LT");
                break;
            }
            default: {
            }
        }
    } else if (operation->getOperationType() == IR::Operations::Operation::AndOp) {
        auto andOp = std::static_pointer_cast<IR::Operations::AndOperation>(operation);
        opJson["type"] = web::json::value("And");
        opJson["left"] = web::json::value(andOp->getLeftInput()->getIdentifier());
        opJson["right"] = web::json::value(andOp->getRightInput()->getIdentifier());
    } else if (operation->getOperationType() == IR::Operations::Operation::NegateOp) {
        auto negate = std::static_pointer_cast<IR::Operations::NegateOperation>(operation);
        opJson["type"] = web::json::value("Negate");
        opJson["input"] = web::json::value(negate->getInput()->getIdentifier());
    } else if (operation->getOperationType() == IR::Operations::Operation::BranchOp) {
        auto compOp = std::static_pointer_cast<IR::Operations::BranchOperation>(operation);
        opJson["type"] = web::json::value("Branch");
        opJson["next"] = serializeOperation(compOp->getNextBlockInvocation());
    } else if (operation->getOperationType() == IR::Operations::Operation::LoadOp) {
        auto loadOp = std::static_pointer_cast<IR::Operations::LoadOperation>(operation);
        opJson["type"] = web::json::value("Load");
        opJson["address"] = web::json::value(loadOp->getAddress()->getIdentifier());
    } else if (operation->getOperationType() == IR::Operations::Operation::StoreOp) {
        auto storeOp = std::static_pointer_cast<IR::Operations::StoreOperation>(operation);
        opJson["type"] = web::json::value("Store");
        opJson["address"] = web::json::value(storeOp->getAddress()->getIdentifier());
        opJson["value"] = web::json::value(storeOp->getValue()->getIdentifier());
    } else if (operation->getOperationType() == IR::Operations::Operation::ProxyCallOp) {
        auto proxyCallOp = std::static_pointer_cast<IR::Operations::ProxyCallOperation>(operation);
        opJson["type"] = web::json::value("call");
        opJson["symbol"] = web::json::value(proxyCallOp->getFunctionSymbol());
        std::vector<web::json::value> arguments;
        for (auto arg : proxyCallOp->getInputArguments()) {
            arguments.emplace_back(arg->getIdentifier());
        }
        opJson["arguments"] = web::json::value::array(arguments);
    } else {
        NES_THROW_RUNTIME_ERROR("operator is not known: " << operation->toString());
    }
}

web::json::value IRSerialization::serializeOperation(IR::Operations::BasicBlockInvocation& blockInvocation) {

    bool containsBlock = false;
    for (auto& block : blocks) {

        if ((block["id"].as_string()) == blockInvocation.getBlock()->getIdentifier()) {
            containsBlock = true;
        }
    }

    if (!containsBlock) {
        serializeBlock(blockInvocation.getBlock());
    }

    web::json::value opJson{};
    opJson["target"] = web::json::value(blockInvocation.getBlock()->getIdentifier());
    std::vector<web::json::value> arguments;
    for (auto arg : blockInvocation.getArguments()) {
        arguments.emplace_back(arg->getIdentifier());
    }
    opJson["arguments"] = web::json::value::array(arguments);
    return opJson;
}

}// namespace NES::ExecutionEngine::Experimental