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

#include "Nautilus/IR/Operations/LoadOperation.hpp"
#include <Nautilus/IR/Operations/ArithmeticOperations/AddOperation.hpp>
#include <Nautilus/IR/Operations/ArithmeticOperations/DivOperation.hpp>
#include <Nautilus/IR/Operations/ArithmeticOperations/MulOperation.hpp>
#include <Nautilus/IR/Operations/ArithmeticOperations/SubOperation.hpp>
#include <Nautilus/IR/Operations/BranchOperation.hpp>
#include <Nautilus/IR/Operations/CastOperation.hpp>
#include <Nautilus/IR/Operations/IfOperation.hpp>
#include <Nautilus/IR/Operations/LogicalOperations/AndOperation.hpp>
#include <Nautilus/IR/Operations/LogicalOperations/CompareOperation.hpp>
#include <Nautilus/IR/Operations/LogicalOperations/NegateOperation.hpp>
#include <Nautilus/IR/Operations/LogicalOperations/OrOperation.hpp>
#include <Nautilus/IR/Operations/Loop/LoopOperation.hpp>
#include <Nautilus/IR/Operations/ProxyCallOperation.hpp>
#include <Nautilus/IR/Operations/ReturnOperation.hpp>
#include <Nautilus/IR/Operations/StoreOperation.hpp>
#include <Util/Logger/Logger.hpp>
#include <Nautilus/IR/Operations/Operation.hpp>

namespace NES::Nautilus::IR::Operations {
Operation::Operation(OperationType opType, OperationIdentifier identifier, Types::StampPtr stamp)
    : opType(opType), identifier(identifier), stamp(stamp) {}
Operation::Operation(OperationType opType, Types::StampPtr stamp) : opType(opType), identifier(""), stamp(stamp) {}
Operation::OperationType Operation::getOperationType() const { return opType; }
OperationIdentifier Operation::getIdentifier() { return identifier; }
const Types::StampPtr& Operation::getStamp() const { return stamp; }

void Operation::addUsage(const Operation* operation) { usages.emplace_back(operation); }

const std::vector<const Operation*>& Operation::getUsages() { return usages; }

bool Operation::isConstOperation() const {
    return opType == OperationType::ConstBooleanOp || opType == OperationType::ConstFloatOp || opType == OperationType::ConstIntOp;
}

std::vector<std::shared_ptr<Operation>> Operation::getInputs() {
    switch(this->opType) {
        case OperationType::AddOp: {
            // auto addOp = std::static_pointer_cast<AddOperation>(operation);
            auto addOp = static_cast<AddOperation*>(this);
            auto leftInput = addOp->getLeftInput();
            auto rightInput = addOp->getRightInput();
            return {leftInput, rightInput};
            // return {addOp->getLeftInput(), addOp->getRightInput()};
        }
        case OperationType::AndOp: {
            auto andOp = static_cast<AndOperation*>(this);
            return {andOp->getLeftInput(), andOp->getRightInput()};
        }
        case OperationType::BranchOp: {
            auto branchOp = static_cast<BranchOperation*>(this);
            return branchOp->getNextBlockInvocation().getArguments();
        }
        case OperationType::CastOp: {
            auto castOp = static_cast<CastOperation*>(this);
            return {castOp->getInput()};
        }
        case OperationType::CompareOp: {
            auto compareOp = static_cast<CompareOperation*>(this);
            return {compareOp->getLeftInput(), compareOp->getRightInput()};
        }
        case OperationType::DivOp: {
            auto divOp = static_cast<DivOperation*>(this);
            return {divOp->getLeftInput(), divOp->getRightInput()};
        }
        case OperationType::FunctionOp: {
            //Todo handle function operations
            NES_DEBUG("Operation::getInputs(): Getting inputs for function operations is currently not defined.");
            // NES_NOT_IMPLEMENTED();
            return {};
        }
        case OperationType::IfOp: {
            auto ifOp = static_cast<IfOperation*>(this);
            auto firstBranchArgs = ifOp->getTrueBlockInvocation().getArguments();
            auto secondBranchArgs = ifOp->getFalseBlockInvocation().getArguments();
            firstBranchArgs.insert( firstBranchArgs.end(), secondBranchArgs.begin(), secondBranchArgs.end());
            return firstBranchArgs;
        }
        case OperationType::LoopOp: {
            auto loopOp = static_cast<LoopOperation*>(this);
            auto firstBranchArgs = loopOp->getLoopBodyBlock().getArguments();
            auto secondBranchArgs = loopOp->getLoopFalseBlock().getArguments();
            firstBranchArgs.insert( firstBranchArgs.end(), secondBranchArgs.begin(), secondBranchArgs.end());
            return firstBranchArgs;
        }
        case OperationType::LoadOp: {
            auto loadOp = static_cast<LoadOperation*>(this);
            return {loadOp->getAddress()};
        }
        case OperationType::MulOp: {
            auto mulOp = static_cast<MulOperation*>(this);
            return {mulOp->getLeftInput(), mulOp->getRightInput()};
        }
        case OperationType::NegateOp: {
            auto negateOp = static_cast<NegateOperation*>(this);
            return {negateOp->getInput()};
        }
        case OperationType::OrOp: {
            auto orOp = static_cast<OrOperation*>(this);
            return {orOp->getLeftInput(), orOp->getRightInput()};
        }
        case OperationType::ProxyCallOp: {
            auto proxyCallOp = static_cast<ProxyCallOperation*>(this);
            return proxyCallOp->getInputArguments();
        }
        case OperationType::ReturnOp: {
            auto returnOp = static_cast<ReturnOperation*>(this);
            if(returnOp->hasReturnValue()) {
                return {returnOp->getReturnValue()};
            }
            return {};
        }
        case OperationType::StoreOp: {
            auto storeOp = static_cast<StoreOperation*>(this);
            return {storeOp->getValue(), storeOp->getAddress()};
        }
        case OperationType::SubOp: {
            auto subOp = static_cast<SubOperation*>(this);
            return {subOp->getLeftInput(), subOp->getRightInput()};
        }
        default:
            /*
            AddressOp,              // no inputs
            BasicBlockArgument,     // no inputs
            BlockInvocation,        // no inputs
            ConstIntOp,             // no inputs
            ConstBooleanOp,         // no inputs
            ConstFloatOp,           // no inputs
            MLIR_YIELD,             // no inputs
            */
            NES_ERROR("We do not support getting inputs for the operation: {}", this->toString());
            return {};
    }
}

void Operation::replaceInput(const Operations::OperationPtr& toReplace, Operations::OperationPtr replaceWith) {
    switch(this->getOperationType()) {
        case OperationType::AddOp: {
            auto addOp = static_cast<AddOperation*>(this);
            if(addOp->getLeftInput() == toReplace) {
                addOp->setLeftInput(replaceWith);
            }
            if(addOp->getRightInput() == toReplace) {
                addOp->setRightInput(replaceWith);
            }
            break;
        }
        case OperationType::AndOp: {
            auto andOp = static_cast<AndOperation*>(this);
            if(andOp->getLeftInput() == toReplace) {
                andOp->setLeftInput(replaceWith);
            }
            if(andOp->getRightInput() == toReplace) {
                andOp->setRightInput(replaceWith);
            }
            break;
        }
        case OperationType::BranchOp: {
            auto branchOp = static_cast<BranchOperation*>(this);
            branchOp->getNextBlockInvocation().replaceArgument(toReplace, replaceWith);
            break;
        }
        case OperationType::CastOp: {
            auto castOp = static_cast<CastOperation*>(this);
            if(castOp->getInput() == toReplace) {
                castOp->setInput(replaceWith);
            }
            break;
        }
        case OperationType::CompareOp: {
            auto compareOp = static_cast<CompareOperation*>(this);
            if(compareOp->getLeftInput() == toReplace) {
                compareOp->setLeftInput(replaceWith);
            }
            if(compareOp->getRightInput() == toReplace) {
                compareOp->setRightInput(replaceWith);
            }
            break;
        }
        case OperationType::DivOp: {
            auto divOp = static_cast<DivOperation*>(this);
            if(divOp->getLeftInput() == toReplace) {
                divOp->setLeftInput(replaceWith);
            }
            if(divOp->getRightInput() == toReplace) {
                divOp->setRightInput(replaceWith);
            }
            break;
        }
        case OperationType::FunctionOp: {
            //Todo handle function operations
            NES_DEBUG("Operation::getInputs(): Getting inputs for function operations is currently not defined.");
            // NES_NOT_IMPLEMENTED();
            break;
        }
        case OperationType::IfOp: {
            auto ifOp = static_cast<IfOperation*>(this);
            ifOp->getTrueBlockInvocation().replaceArgument(toReplace, replaceWith);
            if(ifOp->hasFalseCase()) {
                ifOp->getFalseBlockInvocation().replaceArgument(toReplace, replaceWith);
            }
            if(ifOp->getBooleanValue()->getIdentifier() == toReplace->getIdentifier()) {
                ifOp->setBooleanValue(replaceWith);
            }
        }
        // case OperationType::LoopOp: {
        //     auto loopOp = std::static_pointer_cast<LoopOperation>(operation);
        //     auto firstBranchArgs = loopOp->getLoopBodyBlock().getArguments();
        //     auto secondBranchArgs = loopOp->getLoopFalseBlock().getArguments();
        //     firstBranchArgs.insert( firstBranchArgs.end(), secondBranchArgs.begin(), secondBranchArgs.end());
        //     return firstBranchArgs;
        // }
        case OperationType::LoadOp: {
            auto loadOp = static_cast<LoadOperation*>(this);
            if(loadOp->getAddress() == toReplace) {
                loadOp->setAddress(replaceWith);
            }
            break;
        }
        case OperationType::MulOp: {
            auto addOp = static_cast<MulOperation*>(this);
            if(addOp->getLeftInput() == toReplace) {
                addOp->setLeftInput(replaceWith);
            }
            if(addOp->getRightInput() == toReplace) {
                addOp->setRightInput(replaceWith);
            }
            break;
        }
        case OperationType::NegateOp: {
            auto negateOp = static_cast<NegateOperation*>(this);
            if(negateOp->getInput() == toReplace) {
                negateOp->setInput(replaceWith);
            }
            break;
        }
        case OperationType::OrOp: {
            auto orOp = static_cast<OrOperation*>(this);
            if(orOp->getLeftInput() == toReplace) {
                orOp->setLeftInput(replaceWith);
            }
            if(orOp->getRightInput() == toReplace) {
                orOp->setRightInput(replaceWith);
            }
            break;
        }
        case OperationType::ProxyCallOp: {
            auto proxyCallOp = static_cast<ProxyCallOperation*>(this);
            std::vector<OperationPtr> newInputArguments;
            for(const auto& inputArgument : proxyCallOp->getInputArguments()) {
                if(inputArgument == toReplace) {
                    newInputArguments.emplace_back(replaceWith);
                } else {
                    newInputArguments.emplace_back(inputArgument);
                }
            }
            proxyCallOp->setInputArguments(newInputArguments);
            break;
        }
        case OperationType::ReturnOp: {
            auto returnOp = static_cast<ReturnOperation*>(this);
            if(returnOp->getReturnValue() == toReplace) {
                returnOp->setReturnValue(replaceWith);
            }
            break;
        }
        case OperationType::StoreOp: {
            auto storeOp = static_cast<StoreOperation*>(this);
            if(storeOp->getValue() == toReplace) {
                storeOp->setValue(replaceWith);
            }
            if(storeOp->getAddress() == toReplace) {
                storeOp->setAddress(replaceWith);
            }
            break;
        }
        case OperationType::SubOp: {
            auto subOp = static_cast<SubOperation*>(this);
            if(subOp->getLeftInput() == toReplace) {
                subOp->setLeftInput(replaceWith);
            }
            if(subOp->getRightInput() == toReplace) {
                subOp->setRightInput(replaceWith);
            }
            break;
        }
        default:
            /*
            AddressOp,              // no inputs
            BasicBlockArgument,     // no inputs
            BlockInvocation,        // no inputs
            ConstIntOp,             // no inputs
            ConstBooleanOp,         // no inputs
            ConstFloatOp,           // no inputs
            MLIR_YIELD,             // no inputs
            */
            // return {};
            NES_TRACE("Called replace input on an operation without inputs: {}", this->toString());
    }
}

}// namespace NES::Nautilus::IR::Operations
