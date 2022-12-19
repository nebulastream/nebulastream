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

#include "Nautilus/IR/Operations/AddressOperation.hpp"
#include "Nautilus/IR/Operations/ArithmeticOperations/AddOperation.hpp"
#include "Nautilus/IR/Operations/ArithmeticOperations/DivOperation.hpp"
#include "Nautilus/IR/Operations/ArithmeticOperations/MulOperation.hpp"
#include "Nautilus/IR/Operations/ArithmeticOperations/SubOperation.hpp"
#include "Nautilus/IR/Operations/BranchOperation.hpp"
#include "Nautilus/IR/Operations/CastOperation.hpp"
#include "Nautilus/IR/Operations/FunctionOperation.hpp"
#include "Nautilus/IR/Operations/IfOperation.hpp"
#include "Nautilus/IR/Operations/LoadOperation.hpp"
#include "Nautilus/IR/Operations/LogicalOperations/AndOperation.hpp"
#include "Nautilus/IR/Operations/LogicalOperations/CompareOperation.hpp"
#include "Nautilus/IR/Operations/LogicalOperations/NegateOperation.hpp"
#include "Nautilus/IR/Operations/LogicalOperations/OrOperation.hpp"
#include "Nautilus/IR/Operations/Loop/LoopOperation.hpp"
#include "Nautilus/IR/Operations/ProxyCallOperation.hpp"
#include "Nautilus/IR/Operations/ReturnOperation.hpp"
#include "Nautilus/IR/Operations/StoreOperation.hpp"
#include "Util/Logger/Logger.hpp"
#include <Nautilus/IR/Operations/Operation.hpp>
#include <memory>

namespace NES::Nautilus::IR::Operations {
Operation::Operation(OperationType opType, OperationIdentifier identifier, Types::StampPtr stamp)
    : opType(opType), identifier(identifier), stamp(stamp) {}
Operation::Operation(OperationType opType, Types::StampPtr stamp) : opType(opType), identifier(""), stamp(stamp) {}
Operation::OperationType Operation::getOperationType() const { return opType; }
OperationIdentifier Operation::getIdentifier() { return identifier; }
const Types::StampPtr& Operation::getStamp() const { return stamp; }

void Operation::addUsage(const Operation* operation) { usages.emplace_back(operation); }

const std::vector<const Operation*>& Operation::getUsages() { return usages; }


bool Operation::isLoopOperation() { return opType == Operation::LoopOp; }
bool Operation::isIfOperation() { return opType == Operation::IfOp; }

std::vector<std::shared_ptr<Operation>> Operation::getInputs(std::shared_ptr<Operation> operation) {
    switch(operation->getOperationType()) {
        case AddOp: {
            auto addOp = std::static_pointer_cast<AddOperation>(operation);
            return {addOp->getLeftInput(), addOp->getRightInput()};
        }
        case AndOp: {
            auto andOp = std::static_pointer_cast<AndOperation>(operation);
            return {andOp->getLeftInput(), andOp->getRightInput()};
        }
        case BranchOp: {
            auto branchOp = std::static_pointer_cast<BranchOperation>(operation);
            return branchOp->getNextBlockInvocation().getBranchOps();
        }
        case CastOp: {
            auto castOp = std::static_pointer_cast<CastOperation>(operation);
            return {castOp->getInput()};
        }
        case CompareOp: {
            auto compareOp = std::static_pointer_cast<CompareOperation>(operation);
            return {compareOp->getLeftInput(), compareOp->getRightInput()};
        }
        case DivOp: {
            auto divOp = std::static_pointer_cast<DivOperation>(operation);
            return {divOp->getLeftInput(), divOp->getRightInput()};
        }
        case FunctionOp: {
            //Todo handle function operations
            NES_DEBUG("Operation::getInputs(): Getting inputs for function operations is currently not defined.")
            // NES_NOT_IMPLEMENTED();
            return {};
        }
        case IfOp: {
            auto ifOp = std::static_pointer_cast<IfOperation>(operation);
            auto firstBranchArgs = ifOp->getTrueBlockInvocation().getBranchOps();
            auto secondBranchArgs = ifOp->getFalseBlockInvocation().getBranchOps();
            firstBranchArgs.insert( firstBranchArgs.end(), secondBranchArgs.begin(), secondBranchArgs.end());
            return firstBranchArgs;
        }
        case LoopOp: {
            auto loopOp = std::static_pointer_cast<LoopOperation>(operation);
            auto firstBranchArgs = loopOp->getLoopBodyBlock().getBranchOps();
            auto secondBranchArgs = loopOp->getLoopFalseBlock().getBranchOps();
            firstBranchArgs.insert( firstBranchArgs.end(), secondBranchArgs.begin(), secondBranchArgs.end());
            return firstBranchArgs;
        }
        case LoadOp: {
            auto loadOp = std::static_pointer_cast<LoadOperation>(operation);
            return {loadOp->getAddress()};
        }
        case MulOp: {
            auto mulOp = std::static_pointer_cast<MulOperation>(operation);
            return {mulOp->getLeftInput(), mulOp->getRightInput()};
        }
        case NegateOp: {
            auto negateOp = std::static_pointer_cast<NegateOperation>(operation);
            return {negateOp->getNegateInput()};
        }
        case OrOp: {
            auto orOp = std::static_pointer_cast<OrOperation>(operation);
            return {orOp->getLeftInput(), orOp->getRightInput()};
        }
        case ProxyCallOp: {
            auto proxyCallOp = std::static_pointer_cast<ProxyCallOperation>(operation);
            return proxyCallOp->getInputArguments();
        }
        case ReturnOp: {
            auto returnOp = std::static_pointer_cast<ReturnOperation>(operation);
            return {returnOp->getReturnValue()};
        }
        case StoreOp: {
            auto storeOp = std::static_pointer_cast<StoreOperation>(operation);
            return {storeOp->getValue(), storeOp->getAddress()};
        }
        case SubOp: {
            auto subOp = std::static_pointer_cast<SubOperation>(operation);
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
            return {};
    }
}

void Operation::setInput(std::shared_ptr<Operation> operation, std::shared_ptr<Operation> newOperation,  size_t index) {
    switch(operation->getOperationType()) {
        case AddOp: {
            auto addOp = std::static_pointer_cast<AddOperation>(operation);
            if(index == 0) {
                addOp->setLeftInput(newOperation);
            } else {
                addOp->setRightInput(newOperation);
            }
            break;
        }
        case AndOp: {
            // auto andOp = std::static_pointer_cast<AndOperation>(operation);
            // return {andOp->getLeftInput(), andOp->getRightInput()};
            break;
        }
        case BranchOp: {
            // auto branchOp = std::static_pointer_cast<BranchOperation>(operation);
            // return branchOp->getNextBlockInvocation().getBranchOps();
            break;
        }
        case CastOp: {
            // auto castOp = std::static_pointer_cast<CastOperation>(operation);
            // return {castOp->getInput()};
            break;
        }
        case CompareOp: {
            auto compareOp = std::static_pointer_cast<CompareOperation>(operation);
            if(index == 0) {
                compareOp->setLeftInput(newOperation);
            } else {
                compareOp->setRightInput(newOperation);
            }
            break;
        }
        case DivOp: {
            auto divOp = std::static_pointer_cast<DivOperation>(operation);
            if(index == 0) {
                divOp->setLeftInput(newOperation);
            } else {
                divOp->setRightInput(newOperation);
            }
            break;
        }
        case FunctionOp: {
            //Todo handle function operations
            NES_DEBUG("Operation::getInputs(): Getting inputs for function operations is currently not defined.")
            // NES_NOT_IMPLEMENTED();
            break;
        }
        case IfOp: {
            // auto ifOp = std::static_pointer_cast<IfOperation>(operation);
            // auto firstBranchArgs = ifOp->getTrueBlockInvocation().getBranchOps();
            // auto secondBranchArgs = ifOp->getFalseBlockInvocation().getBranchOps();
            // firstBranchArgs.insert( firstBranchArgs.end(), secondBranchArgs.begin(), secondBranchArgs.end());
            // return firstBranchArgs;
            break;
        }
        case LoopOp: {
            // auto loopOp = std::static_pointer_cast<LoopOperation>(operation);
            // auto firstBranchArgs = loopOp->getLoopBodyBlock().getBranchOps();
            // auto secondBranchArgs = loopOp->getLoopFalseBlock().getBranchOps();
            // firstBranchArgs.insert( firstBranchArgs.end(), secondBranchArgs.begin(), secondBranchArgs.end());
            // return firstBranchArgs;
            break;
        }
        case LoadOp: {
            auto loadOp = std::static_pointer_cast<LoadOperation>(operation);
            loadOp->setAddress(newOperation);
            break;
        }
        case MulOp: {
            auto mulOp = std::static_pointer_cast<MulOperation>(operation);
            if(index == 0) {
                mulOp->setLeftInput(newOperation);
            } else {
                mulOp->setRightInput(newOperation);
            }
            break;
        }
        case NegateOp: {
            auto negateOp = std::static_pointer_cast<NegateOperation>(operation);
            negateOp->setNegateInput(newOperation);
            break;
        }
        case OrOp: {
            auto orOp = std::static_pointer_cast<OrOperation>(operation);
            if(index == 0) {
                orOp->setLeftInput(newOperation);
            } else {
                orOp->setRightInput(newOperation);
            }
            break;
        }
        case ProxyCallOp: {
            // auto proxyCallOp = std::static_pointer_cast<ProxyCallOperation>(operation);
            // return proxyCallOp->getInputArguments();
            break;
        }
        case ReturnOp: {
            auto returnOp = std::static_pointer_cast<ReturnOperation>(operation);
            returnOp->setReturnValue(newOperation);
            break;
        }
        case StoreOp: {
            auto storeOp = std::static_pointer_cast<StoreOperation>(operation);
            if(index == 0) {
                storeOp->setValueInput(newOperation);
            } else {
                storeOp->setAddressInput(newOperation);
            }
            break;
        }
        case SubOp: {
            auto subOp = std::static_pointer_cast<SubOperation>(operation);
            if(index == 0) {
                subOp->setLeftInput(newOperation);
            } else {
                subOp->setRightInput(newOperation);
            }
            break;
        }
        default:
            break;
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
    }
}

}// namespace NES::Nautilus::IR::Operations
