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

#include <Nautilus/Backends/Babelfish/BabelfishCode.hpp>
#include <Nautilus/IR/Operations/CastOperation.hpp>
#include <Nautilus/IR/Operations/ConstFloatOperation.hpp>
#include <Nautilus/Backends/Babelfish/BabelfishLoweringProvider.hpp>
#include <Nautilus/IR/Operations/ArithmeticOperations/DivOperation.hpp>
#include <Nautilus/IR/Operations/ArithmeticOperations/MulOperation.hpp>
#include <Nautilus/IR/Operations/LogicalOperations/AndOperation.hpp>
#include <Nautilus/IR/Operations/LogicalOperations/OrOperation.hpp>
#include <Nautilus/IR/Operations/Operation.hpp>
#include <Nautilus/IR/Types/AddressStamp.hpp>
#include <Nautilus/IR/Types/FloatStamp.hpp>
#include <Nautilus/IR/Types/IntegerStamp.hpp>
#include <Util/Logger/Logger.hpp>
#include <cstdint>
#include <nlohmann/json.hpp>
#include <nlohmann/json_fwd.hpp>
#include <sstream>
#include <utility>
#include <vector>

namespace NES::Nautilus::Backends::Babelfish {

BabelfishLoweringProvider::LoweringContext::LoweringContext(std::shared_ptr<IR::IRGraph> ir) : ir(std::move(ir)) {}

BabelfishCode BabelfishLoweringProvider::lower(std::shared_ptr<IR::IRGraph> ir) {
    auto ctx = LoweringContext(std::move(ir));
    return ctx.process();
}

BabelfishCode BabelfishLoweringProvider::LoweringContext::process() {

    auto functionOperation = ir->getRootOperation();
    RegisterFrame rootFrame;
    auto functionBasicBlock = functionOperation->getFunctionBasicBlock();
    this->process(functionBasicBlock, rootFrame);

    nlohmann::json irJson{};
    irJson["blocks"] = blocks;
    irJson["id"] = ir->getRootOperation()->getIdentifier().toString();
    std::vector<nlohmann::json> arguments;
    std::vector<Type> argumentsTypes;
    for (const auto& arg : functionOperation->getFunctionBasicBlock()->getArguments()) {
        nlohmann::json irJson{};
        irJson["id"] = arg->getIdentifier().toString();;
        irJson["type"] = arg->getStamp()->toString();
        arguments.emplace_back(irJson);
        argumentsTypes.emplace_back(i64);
    }
    irJson["arguments"] = arguments;

    std::stringstream pipelineCode;
    pipelineCode << irJson;

    BabelfishCode code = {pipelineCode.str(), returnType, argumentsTypes};

    return code;
}

std::string BabelfishLoweringProvider::LoweringContext::process(const std::shared_ptr<IR::BasicBlock>& block,
                                                                RegisterFrame& frame) {
    // assume that all argument registers are correctly set
    auto entry = activeBlocks.find(block->getIdentifier());
    if (entry == activeBlocks.end()) {
        // create bytecode block;
        auto blockName = block->getIdentifier();
        auto blockIndex = blocks.size();
        auto& blockJson = blocks.emplace_back();
        blockJson["id"] = blockName;
        std::vector<nlohmann::json> arguments;
        for (const auto& arg : block->getArguments()) {
            nlohmann::json jsonArg;
            jsonArg["stamp"] = arg->getStamp()->toString();
            jsonArg["id"] = arg->getIdentifier().toString();;
            arguments.emplace_back(jsonArg);
        }
        blockJson["arguments"] = arguments;
        activeBlocks.emplace(block->getIdentifier(), blockName);
        std::vector<nlohmann::json> jsonOperations;
        for (auto& opt : block->getOperations()) {
            this->process(opt, blockIndex, jsonOperations, frame);
        }

        for (auto& jsonBlock : blocks) {
            if (jsonBlock["id"] == block->getIdentifier()) {
                jsonBlock["operations"] = jsonOperations;
            }
        }
        return blockName;
    } else {
        return entry->second;
    }
}

void BabelfishLoweringProvider::LoweringContext::process(const std::shared_ptr<IR::Operations::CompareOperation>& operation,
                                                         nlohmann::json& opJson) {
    auto cmpOp = std::static_pointer_cast<IR::Operations::CompareOperation>(operation);
    opJson["type"] = "Compare";
    opJson["left"] = cmpOp->getLeftInput()->getIdentifier().toString();;
    opJson["right"] = cmpOp->getRightInput()->getIdentifier().toString();;

    std::string comperator;
    if (cmpOp->getComparator() == IR::Operations::CompareOperation::Comparator::EQ) {
        comperator = "EQ";
    } else if (cmpOp->getComparator() == IR::Operations::CompareOperation::Comparator::LT) {
        comperator = "LT";
    } else if (cmpOp->getComparator() == IR::Operations::CompareOperation::Comparator::GT) {
        comperator = "GT";
    } else if (cmpOp->getComparator() == IR::Operations::CompareOperation::Comparator::GE) {
        comperator = "GE";
    } else if (cmpOp->getComparator() == IR::Operations::CompareOperation::Comparator::LE) {
        comperator = "LE";
    } else {
        NES_NOT_IMPLEMENTED();
    }
    opJson["Compare"] = comperator;
}

nlohmann::json BabelfishLoweringProvider::LoweringContext::process(IR::Operations::BasicBlockInvocation& blockInvocation) {
    nlohmann::json opJson{};
    auto blockInputArguments = blockInvocation.getArguments();
    auto blockTargetArguments = blockInvocation.getBlock()->getArguments();
    opJson["target"] = blockInvocation.getBlock()->getIdentifier();
    std::vector<nlohmann::json> arguments;
    for (const auto& arg : blockInvocation.getArguments()) {
        nlohmann::json jsonArg;
        jsonArg["stamp"] = arg->getStamp()->toString();
        jsonArg["id"] = arg->getIdentifier().toString();;
        arguments.emplace_back(jsonArg);
    }
    opJson["arguments"] = arguments;
    return opJson;
}

void BabelfishLoweringProvider::LoweringContext::process(const std::shared_ptr<IR::Operations::Operation>& operation,
                                                         short,
                                                         std::vector<nlohmann::json>& block,
                                                         RegisterFrame& frame) {
    nlohmann::json& opJson = block.emplace_back(nlohmann::json{});
    opJson["stamp"] = operation->getStamp()->toString();
    opJson["id"] = operation->getIdentifier().toString();;
    switch (operation->getOperationType()) {
        case IR::Operations::Operation::OperationType::ConstBooleanOp: {
            auto constOp = std::static_pointer_cast<IR::Operations::ConstBooleanOperation>(operation);
            opJson["type"] = "ConstBool";
            opJson["value"] = constOp->getValue();
            return;
        }
        case IR::Operations::Operation::OperationType::ConstIntOp: {
            auto constOp = std::static_pointer_cast<IR::Operations::ConstIntOperation>(operation);
            opJson["type"] = "ConstInt";
            opJson["value"] = constOp->getValue();
            return;
        }
        case IR::Operations::Operation::OperationType::ConstFloatOp: {
            auto constOp = std::static_pointer_cast<IR::Operations::ConstFloatOperation>(operation);
            opJson["type"] = "ConstFloat";
            opJson["value"] = constOp->getValue();
            return;
        }
        case IR::Operations::Operation::OperationType::AddOp: {
            auto constOp = std::static_pointer_cast<IR::Operations::AddOperation>(operation);
            opJson["type"] = "Add";
            opJson["left"] = constOp->getLeftInput()->getIdentifier().toString();
            opJson["right"] = constOp->getRightInput()->getIdentifier().toString();
            return;
        }
        case IR::Operations::Operation::OperationType::MulOp: {
            auto mulOp = std::static_pointer_cast<IR::Operations::MulOperation>(operation);
            opJson["type"] = "Mul";
            opJson["left"] = mulOp->getLeftInput()->getIdentifier().toString();
            opJson["right"] = mulOp->getRightInput()->getIdentifier().toString();
            return;
        }
        case IR::Operations::Operation::OperationType::SubOp: {
            auto constOp = std::static_pointer_cast<IR::Operations::AddOperation>(operation);
            opJson["type"] = "Sub";
            opJson["left"] = constOp->getLeftInput()->getIdentifier().toString();
            opJson["right"] = constOp->getRightInput()->getIdentifier().toString();
            return;
        }
        case IR::Operations::Operation::OperationType::DivOp: {
            auto op = std::static_pointer_cast<IR::Operations::DivOperation>(operation);
            opJson["type"] = "Div";
            opJson["left"] = op->getLeftInput()->getIdentifier().toString();
            opJson["right"] = op->getRightInput()->getIdentifier().toString();
            return;
        }
        case IR::Operations::Operation::OperationType::ReturnOp: {
            auto constOp = std::static_pointer_cast<IR::Operations::ReturnOperation>(operation);
            opJson["type"] = "Return";
            returnType = constOp->getStamp();
            if (constOp->hasReturnValue()) {
                opJson["value"] = constOp->getReturnValue()->getIdentifier().toString();
            }
            return;
        }
        case IR::Operations::Operation::OperationType::CompareOp: {
            auto compOpt = std::static_pointer_cast<IR::Operations::CompareOperation>(operation);
            process(compOpt, opJson);
            return;
        }
        case IR::Operations::Operation::OperationType::IfOp: {
            auto ifOp = std::static_pointer_cast<IR::Operations::IfOperation>(operation);
            opJson["type"] = "If";
            opJson["input"] = ifOp->getValue()->getIdentifier().toString();
            opJson["trueCase"] = process(ifOp->getTrueBlockInvocation());
            opJson["falseCase"] = process(ifOp->getFalseBlockInvocation());
            process(ifOp->getTrueBlockInvocation().getBlock(), frame);
            process(ifOp->getFalseBlockInvocation().getBlock(), frame);
            return;
        }

        case IR::Operations::Operation::OperationType::BranchOp: {
            auto compOp = std::static_pointer_cast<IR::Operations::BranchOperation>(operation);
            opJson["type"] = "Branch";
            opJson["next"] = process(compOp->getNextBlockInvocation());
            process(compOp->getNextBlockInvocation().getBlock(), frame);
            return;
        }
        case IR::Operations::Operation::OperationType::LoadOp: {
            auto loadOp = std::static_pointer_cast<IR::Operations::LoadOperation>(operation);
            opJson["type"] = "Load";
            opJson["address"] = loadOp->getAddress()->getIdentifier().toString();
            return;
        }
        case IR::Operations::Operation::OperationType::StoreOp: {
            auto storeOp = std::static_pointer_cast<IR::Operations::StoreOperation>(operation);
            opJson["type"] = "Store";
            opJson["address"] = storeOp->getAddress()->getIdentifier().toString();
            opJson["value"] = storeOp->getValue()->getIdentifier().toString();
            return;
        }
        case IR::Operations::Operation::OperationType::ProxyCallOp: {
            auto proxyCallOp = std::static_pointer_cast<IR::Operations::ProxyCallOperation>(operation);
            opJson["type"] = "call";
            opJson["symbol"] = proxyCallOp->getFunctionSymbol();

            auto val = (int64_t) proxyCallOp->getFunctionPtr();

            opJson["fptr"] = val;
            std::vector<nlohmann::json> arguments;
            for (const auto& arg : proxyCallOp->getInputArguments()) {
                nlohmann::json jsonArg;
                jsonArg["id"] = arg->getIdentifier().toString();
                jsonArg["stamp"] = arg->getStamp()->toString();
                arguments.emplace_back(jsonArg);
            }
            opJson["arguments"] = arguments;
            return;
        }
        case IR::Operations::Operation::OperationType::OrOp: {
            auto andOp = std::static_pointer_cast<IR::Operations::OrOperation>(operation);
            opJson["type"] = "Or";
            opJson["left"] = andOp->getLeftInput()->getIdentifier().toString();
            opJson["right"] = andOp->getRightInput()->getIdentifier().toString();
            return;
        }
        case IR::Operations::Operation::OperationType::AndOp: {
            auto andOp = std::static_pointer_cast<IR::Operations::AndOperation>(operation);
            opJson["type"] = "And";
            opJson["left"] = andOp->getLeftInput()->getIdentifier().toString();
            opJson["right"] = andOp->getRightInput()->getIdentifier().toString();
            return;
        }
        case IR::Operations::Operation::OperationType::NegateOp: {
            auto negate = std::static_pointer_cast<IR::Operations::NegateOperation>(operation);
            opJson["type"] = "Negate";
            opJson["input"] = negate->getInput()->getIdentifier().toString();
            return;
        }
        case IR::Operations::Operation::OperationType::CastOp: {
            auto castOp = std::static_pointer_cast<IR::Operations::CastOperation>(operation);
            opJson["type"] = "Cast";
            opJson["input"] = castOp->getInput()->getIdentifier().toString();
            return;
        }
        default: {
            NES_THROW_RUNTIME_ERROR("Operation " << operation->toString() << " not handled");
            return;
        }
    }
}

}// namespace NES::Nautilus::Backends::Babelfish