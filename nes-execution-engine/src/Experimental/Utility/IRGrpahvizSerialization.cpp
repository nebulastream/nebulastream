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
#include "Nautilus/IR/Operations/ArithmeticOperations/MulOperation.hpp"
#include "Nautilus/IR/Operations/BranchOperation.hpp"
#include "Nautilus/IR/Operations/ConstBooleanOperation.hpp"
#include "Nautilus/IR/Operations/FunctionOperation.hpp"
#include "Nautilus/IR/Operations/IfOperation.hpp"
#include "Nautilus/IR/Operations/LogicalOperations/AndOperation.hpp"
#include "Nautilus/IR/Operations/LogicalOperations/CompareOperation.hpp"
#include "Nautilus/IR/Operations/LogicalOperations/NegateOperation.hpp"
#include "Nautilus/IR/Operations/Loop/LoopOperation.hpp"
#include "Nautilus/IR/Operations/ProxyCallOperation.hpp"
#include <Experimental/Utility/IRGraphvizSerialization.hpp>
#include <Nautilus/IR/Operations/ArithmeticOperations/AddOperation.hpp>
#include <Nautilus/IR/Operations/ConstIntOperation.hpp>
#include <Nautilus/IR/Operations/LoadOperation.hpp>
#include <Nautilus/IR/Operations/ReturnOperation.hpp>
#include <Nautilus/IR/Operations/StoreOperation.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Nautilus {

std::string IRGraphvizSerialization::serialize(std::shared_ptr<IR::IRGraph> ir) {

    auto rootBlock = ir->getRootOperation()->getFunctionBasicBlock();
    serializeBlock(rootBlock);

    auto ss = std::stringstream();
    ss << "digraph G {\n";
    ss << "graph [fontsize=10 fontname=\"Verdana\" compound=true];\n";
    ss << "node [fontsize=10 fontname=\"Verdana\"];\n";
    std::reverse(blocks.begin(), blocks.end());
    for (auto blo : blocks) {
        ss << blo << std::endl;
    }

    ss << "}";
    return ss.str();
}

void IRGraphvizSerialization::serializeBlock(std::shared_ptr<IR::BasicBlock> block) {
    auto blockString = std::stringstream();
    visitedBlocks.emplace(block->getIdentifier());
    blockString << "subgraph cluster_" << block->getIdentifier() << "{\n";
    blockString << "node [style=filled];\n";
    blockString << "color=blue;\n";
    blockString << "label = \"Block " << block->getIdentifier() << "\";\n";
    /*
    blocks.emplace_back(web::json::value{});
    web::json::value& blockJson = blocks.back();

    blockJson["id"] = web::json::value(block->getIdentifier());
    std::vector<web::json::value> arguments;

    blockJson["arguments"] = web::json::value::array(arguments);
     */

    auto startId = "\"" + block->getIdentifier() + "_" + std::to_string(0) + "\"";

    blockString << startId << " [label=\"Start\"]\n";
    std::string lastControlFlowOpt = startId;
    auto i = 1;
    for (auto arg : block->getArguments()) {
        auto id = "\"" + block->getIdentifier() + "_" + std::to_string(i) + "\"";
        blockString << id << " [label=\"P(" << arg->getIdentifier() << ")\" shape=\"box\"]\n";
        idMap.insert(std::make_pair(arg->getIdentifier(), id));

        i++;
    }

    for (auto op : block->getOperations()) {
        auto resultOp = serializeOperation(op, block->getIdentifier(), blockString, i);
        if (op->getOperationType() == IR::Operations::Operation::LoadOp
            || op->getOperationType() == IR::Operations::Operation::StoreOp
            || op->getOperationType() == IR::Operations::Operation::IfOp
            || op->getOperationType() == IR::Operations::Operation::BranchOp
            || op->getOperationType() == IR::Operations::Operation::ReturnOp
            || op->getOperationType() == IR::Operations::Operation::ProxyCallOp) {
            blockString << lastControlFlowOpt << "->" << resultOp << " [color=\"red\"];\n";
            lastControlFlowOpt = resultOp;
        }
        i++;
    }

    blockString << "}\n";
    blocks.push_back(blockString.str());
    // ss << "cluster_" << block->getIdentifier() << ";\n";
}

std::string IRGraphvizSerialization::serializeOperation(std::shared_ptr<IR::Operations::Operation> operation,
                                                        std::string blockid,
                                                        std::stringstream& ss,
                                                        int operationIndex) {

    auto stamp = operation->getStamp()->toString();
    auto id = "\"" + blockid + "_" + std::to_string(operationIndex) + "\"";
    if (!operation->getIdentifier().empty()) {
        idMap.insert(std::make_pair(operation->getIdentifier(), id));
    }
    if (operation->getOperationType() == IR::Operations::Operation::ConstIntOp) {
        auto constOp = std::static_pointer_cast<IR::Operations::ConstIntOperation>(operation);
        ss << id << " [label=\"" << constOp->getConstantIntValue() << "\" shape=\"box\"]\n";
        // opJson["type"] = web::json::value("ConstInt");
        // opJson["value"] = web::json::value(constOp->getConstantIntValue());
    } else if (operation->getOperationType() == IR::Operations::Operation::ConstBooleanOp) {
        auto constOp = std::static_pointer_cast<IR::Operations::ConstBooleanOperation>(operation);
        ss << id << " [label=\"ConstBool\"]\n";
        //opJson["type"] = web::json::value("ConstBool");
        //opJson["value"] = web::json::value(constOp->getValue());
    } else if (operation->getOperationType() == IR::Operations::Operation::AddOp) {
        auto constOp = std::static_pointer_cast<IR::Operations::AddOperation>(operation);
        ss << id << " [label=\"Add\"shape=\"diamond\"  color=\"#3cb4a4\"];\n";
        if (idMap.contains(constOp->getLeftInput()->getIdentifier()))
            ss << idMap[constOp->getLeftInput()->getIdentifier()] << "->" << id << " [color=\"blue\"];\n";
        if (idMap.contains(constOp->getRightInput()->getIdentifier()))
            ss << idMap[constOp->getRightInput()->getIdentifier()] << "->" << id << " [color=\"blue\"];\n";
        //opJson["type"] = web::json::value("Add");
        //opJson["left"] = web::json::value(constOp->getLeftInput()->getIdentifier());
        //opJson["right"] = web::json::value(constOp->getRightInput()->getIdentifier());
    } else if (operation->getOperationType() == IR::Operations::Operation::SubOp) {
        auto constOp = std::static_pointer_cast<IR::Operations::AddOperation>(operation);
        ss << id << " [label=\"Sub\" shape=\"diamond\" color=\"#3cb4a4\"]";
        if (idMap.contains(constOp->getLeftInput()->getIdentifier()))
            ss << idMap[constOp->getLeftInput()->getIdentifier()] << "->" << id << " [color=\"blue\"];\n";
        if (idMap.contains(constOp->getRightInput()->getIdentifier()))
            ss << idMap[constOp->getRightInput()->getIdentifier()] << "->" << id << " [color=\"blue\"];\n";

        // opJson["type"] = web::json::value("Sub");
        // opJson["left"] = web::json::value(constOp->getLeftInput()->getIdentifier());
        //opJson["right"] = web::json::value(constOp->getRightInput()->getIdentifier());
    } else if (operation->getOperationType() == IR::Operations::Operation::MulOp) {
        auto mulOp = std::static_pointer_cast<IR::Operations::MulOperation>(operation);
        ss << id << " [label=\"Mul\" shape=\"diamond\" color=\"#3cb4a4\"]";
        if (idMap.contains(mulOp->getLeftInput()->getIdentifier()))
            ss << idMap[mulOp->getLeftInput()->getIdentifier()] << "->" << id << " [color=\"blue\"];\n";
        if (idMap.contains(mulOp->getRightInput()->getIdentifier()))
            ss << idMap[mulOp->getRightInput()->getIdentifier()] << "->" << id << " [color=\"blue\"];\n";

        // opJson["type"] = web::json::value("Mul");
        // opJson["left"] = web::json::value(mulOp->getLeftInput()->getIdentifier());
        // opJson["right"] = web::json::value(mulOp->getRightInput()->getIdentifier());
    } else if (operation->getOperationType() == IR::Operations::Operation::ReturnOp) {
        auto constOp = std::static_pointer_cast<IR::Operations::ReturnOperation>(operation);
        // id = "return" + blockid;
        ss << id << " [label=\"Return\" shape=\"Msquare\"]";

        // opJson["type"] = web::json::value("Return");
        //if (constOp->hasReturnValue())
        //   opJson["value"] = web::json::value(constOp->getReturnValue()->getIdentifier());
    } else if (operation->getOperationType() == IR::Operations::Operation::LoopOp) {
        //  auto loopOperation = std::static_pointer_cast<IR::Operations::LoopOperation>(operation);
        //  opJson["type"] = web::json::value("Loop");
        // opJson["head"] = serializeOperation(loopOperation->getLoopHeadBlock());

    } else if (operation->getOperationType() == IR::Operations::Operation::IfOp) {
        auto ifOp = std::static_pointer_cast<IR::Operations::IfOperation>(operation);
        ss << id << " [label=\"If\" shape=\"Msquare\" color=\"#da2d4f\"]";
        // id = "if" + blockid;
        //  opJson["type"] = web::json::value("If");
        //  opJson["input"] = web::json::value(ifOp->getValue()->getIdentifier());

        if (idMap.contains(ifOp->getValue()->getIdentifier()))
            ss << idMap[ifOp->getValue()->getIdentifier()] << "->" << id << " [color=\"blue\"];\n";

        serializeOperation(ifOp->getTrueBlockInvocation());
        serializeOperation(ifOp->getFalseBlockInvocation());
        auto blockEdges = std::stringstream();
        auto targetIdTrue = "\"" + ifOp->getTrueBlockInvocation().getBlock()->getIdentifier() + "_" + std::to_string(0) + "\"";
        auto targetIdFalse = "\"" + ifOp->getFalseBlockInvocation().getBlock()->getIdentifier() + "_" + std::to_string(0) + "\"";
        blockEdges << id << "->" << targetIdTrue << "\n";
        blockEdges << id << "->" << targetIdFalse << "\n";

        blocks.emplace_back(blockEdges.str());

    } else if (operation->getOperationType() == IR::Operations::Operation::CompareOp) {
        auto compOp = std::static_pointer_cast<IR::Operations::CompareOperation>(operation);

        //opJson["type"] = web::json::value("Compare");
        //opJson["left"] = web::json::value(compOp->getLeftInput()->getIdentifier());
        //opJson["right"] = web::json::value(compOp->getRightInput()->getIdentifier());
        switch (compOp->getComparator()) {
            case IR::Operations::CompareOperation::Comparator::IEQ: {
                //opJson["Compare"] = web::json::value("EQ");
                ss << id << " [label=\"EQ\" shape=\"diamond\" color=\"#3cb4a4\"]";
                break;
            }
            case IR::Operations::CompareOperation::Comparator::ISLT: {
                // opJson["Compare"] = web::json::value("LT");
                ss << id << " [label=\"LT\" shape=\"diamond\" color=\"#3cb4a4\"]";
                break;
            }
            default: {
            }
        }

        if (idMap.contains(compOp->getLeftInput()->getIdentifier()))
            ss << idMap[compOp->getLeftInput()->getIdentifier()] << "->" << id << " [color=\"blue\"];\n";
        if (idMap.contains(compOp->getRightInput()->getIdentifier()))
            ss << idMap[compOp->getRightInput()->getIdentifier()] << "->" << id << " [color=\"blue\"];\n";

    } else if (operation->getOperationType() == IR::Operations::Operation::AndOp) {
        auto andOp = std::static_pointer_cast<IR::Operations::AndOperation>(operation);
        //opJson["type"] = web::json::value("And");
        ss << id << " [label=\"And\" shape=\"diamond\" color=\"#3cb4a4\"]";
        //opJson["left"] = web::json::value(andOp->getLeftInput()->getIdentifier());
        //opJson["right"] = web::json::value(andOp->getRightInput()->getIdentifier());

        if (idMap.contains(andOp->getLeftInput()->getIdentifier()))
            ss << idMap[andOp->getLeftInput()->getIdentifier()] << "->" << id << " [color=\"blue\"];\n";
        if (idMap.contains(andOp->getRightInput()->getIdentifier()))
            ss << idMap[andOp->getRightInput()->getIdentifier()] << "->" << id << " [color=\"blue\"];\n";

    } else if (operation->getOperationType() == IR::Operations::Operation::NegateOp) {
        auto negate = std::static_pointer_cast<IR::Operations::NegateOperation>(operation);
        ss << id << " [label=\"Not\" shape=\"diamond\" color=\"#3cb4a4\"]";
        //opJson["type"] = web::json::value("Negate");
        //opJson["input"] = web::json::value(negate->getInput()->getIdentifier());
    } else if (operation->getOperationType() == IR::Operations::Operation::BranchOp) {
        auto compOp = std::static_pointer_cast<IR::Operations::BranchOperation>(operation);
        ss << id << " [label=\"Br\" shape=\"Msquare\" color=\"#da2d4f\"]";
        //id = "br" + blockid;
        auto blockEdges = std::stringstream();
        auto target = "\"" + compOp->getNextBlockInvocation().getBlock()->getIdentifier() + "_" + std::to_string(0) + "\"";

        for (auto arg : compOp->getNextBlockInvocation().getArguments()) {
            if (idMap.contains(arg->getIdentifier())) {
                auto startId =
                    "\"" + compOp->getNextBlockInvocation().getBlock()->getIdentifier() + "_" + std::to_string(0) + "\"";
                ss << idMap[arg->getIdentifier()] << "->" << id << " [color=\"blue\"];\n";
            }
        }

        blockEdges << id << "->" << target << "\n";
        blocks.emplace_back(blockEdges.str());
        // ss << operation.get << " [label=\"Br\"]";
        //opJson["type"] = web::json::value("Branch");
        serializeOperation(compOp->getNextBlockInvocation());
    } else if (operation->getOperationType() == IR::Operations::Operation::LoadOp) {
        auto loadOp = std::static_pointer_cast<IR::Operations::LoadOperation>(operation);
        ss << id << " [label=\"Load\" shape=\"box\" color=\"#da2d4f\"]";
        if (idMap.contains(loadOp->getAddress()->getIdentifier())) {
            auto inputStamp = loadOp->getAddress()->getIdentifier() + ": " + loadOp->getAddress()->getStamp()->toString();
            ss << idMap[loadOp->getAddress()->getIdentifier()] << "->" << id << "[color=\"blue\" taillabel=\"" << inputStamp
               << "\"];\n";
        }

        //opJson["type"] = web::json::value("Load");
        //opJson["address"] = web::json::value(loadOp->getAddress()->getIdentifier());
    } else if (operation->getOperationType() == IR::Operations::Operation::StoreOp) {
        auto storeOp = std::static_pointer_cast<IR::Operations::StoreOperation>(operation);
        //id = "store" + blockid;
        ss << id << " [label=\"Store\" shape=\"box\" color=\"#da2d4f\"]";
        //opJson["type"] = web::json::value("Store");
        //opJson["address"] = web::json::value(storeOp->getAddress()->getIdentifier());
        //opJson["value"] = web::json::value(storeOp->getValue()->getIdentifier());
        if (idMap.contains(storeOp->getAddress()->getIdentifier()))
            ss << idMap[storeOp->getAddress()->getIdentifier()] << "->" << id << " [color=\"blue\"];\n";
        if (idMap.contains(storeOp->getValue()->getIdentifier()))
            ss << idMap[storeOp->getValue()->getIdentifier()] << "->" << id << " [color=\"blue\"];\n";

    } else if (operation->getOperationType() == IR::Operations::Operation::ProxyCallOp) {
        auto proxyCallOp = std::static_pointer_cast<IR::Operations::ProxyCallOperation>(operation);
        ss << id << " [label=\"" << proxyCallOp->getFunctionSymbol() << +"\" shape=\"box\" color=\"#da2d4f\"]";
        //opJson["type"] = web::json::value("call");
        //opJson["symbol"] = web::json::value(proxyCallOp->getFunctionSymbol());
        for (auto arg : proxyCallOp->getInputArguments()) {
            if (idMap.contains(arg->getIdentifier()))
                ss << idMap[arg->getIdentifier()] << "->" << id << " [color=\"blue\"];\n";
        }
        //opJson["arguments"] = web::json::value::array(arguments);
    } else {
        NES_THROW_RUNTIME_ERROR("operator is not known: " << operation->toString());
    }
    return id;
}

std::string IRGraphvizSerialization::serializeOperation(IR::Operations::BasicBlockInvocation& blockInvocation) {

    if (!visitedBlocks.contains(blockInvocation.getBlock()->getIdentifier())) {
        serializeBlock(blockInvocation.getBlock());
    }

    /*
    web::json::value opJson{};
    opJson["target"] = web::json::value(blockInvocation.getBlock()->getIdentifier());
    std::vector<web::json::value> arguments;
    for (auto arg : blockInvocation.getArguments()) {
        arguments.emplace_back(arg->getIdentifier());
    }
    opJson["arguments"] = web::json::value::array(arguments);
    return opJson;
    */
    return "";
}

}// namespace NES::Nautilus