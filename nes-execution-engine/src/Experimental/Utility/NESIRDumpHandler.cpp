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

#include "Experimental/NESIR/BasicBlocks/BasicBlock.hpp"
#include "Experimental/NESIR/Operations/BranchOperation.hpp"
#include "Experimental/NESIR/Operations/IfOperation.hpp"
#include "Experimental/NESIR/Operations/LoopOperation.hpp"
#include <Experimental/Utility/NESIRDumpHandler.hpp>
#include <Experimental/NESIR/Operations/Operation.hpp>
#include <Util/Logger/Logger.hpp>
#include <iostream>


namespace NES {
namespace ExecutionEngine::Experimental::IR {

NESIRDumpHandler::NESIRDumpHandler(std::ostream& out) : out(out) {}

std::shared_ptr<NESIRDumpHandler> NESIRDumpHandler::create(std::ostream& out) {
    return std::make_shared<NESIRDumpHandler>(out);
}

IR::BasicBlockPtr NESIRDumpHandler::getNextLowerOrEqualLevelBasicBlock(BasicBlockPtr thenBlock, int ifParentBlockLevel) {
    auto terminatorOp = thenBlock->getOperations().back();
    if (terminatorOp->getOperationType() == Operations::Operation::BranchOp) {
        auto branchOp = std::static_pointer_cast<Operations::BranchOperation>(terminatorOp);
        if (branchOp->getNextBlock()->getScopeLevel() <= ifParentBlockLevel) {
            return branchOp->getNextBlock();
        } else {
            return getNextLowerOrEqualLevelBasicBlock(branchOp->getNextBlock(), ifParentBlockLevel);
        }
    } else if (terminatorOp->getOperationType() == Operations::Operation::LoopOp) {
        auto loopOp = std::static_pointer_cast<Operations::LoopOperation>(terminatorOp);
        auto loopIfOp = std::static_pointer_cast<Operations::IfOperation>(loopOp->getLoopHeadBlock()->getOperations().back());
        if (loopIfOp->getElseBranchBlock()->getScopeLevel() <= ifParentBlockLevel) {
            return loopIfOp->getElseBranchBlock();
        } else {
            return getNextLowerOrEqualLevelBasicBlock(loopIfOp->getElseBranchBlock(), ifParentBlockLevel);
        }
    } else {
        auto ifOp = std::static_pointer_cast<Operations::IfOperation>(terminatorOp);
        if (ifOp->getElseBranchBlock() != nullptr) {
            return getNextLowerOrEqualLevelBasicBlock(ifOp->getElseBranchBlock(), ifParentBlockLevel);
        } else {
            return getNextLowerOrEqualLevelBasicBlock(ifOp->getThenBranchBlock(), ifParentBlockLevel);
        }
    }
}

void NESIRDumpHandler::dumpHelper(OperationPtr const& terminatorOp, int32_t scopeLevel) {
    switch(terminatorOp->getOperationType()) {
        case Operations::Operation::OperationType::BranchOp: {
            auto branchOp = std::static_pointer_cast<Operations::BranchOperation>(terminatorOp);
            if(branchOp->getNextBlock()->getScopeLevel() > scopeLevel) {
                dumpHelper(branchOp->getNextBlock());
            }
            break;
        }
        case Operations::Operation::OperationType::LoopOp: {
            auto loopOperation = std::static_pointer_cast<Operations::LoopOperation>(terminatorOp);
            dumpHelper(loopOperation->getLoopHeadBlock());
            break;
        }
        case Operations::Operation::OperationType::IfOp: {
            auto ifOp = std::static_pointer_cast<Operations::IfOperation>(terminatorOp);
            BasicBlockPtr lastTerminatorOp = getNextLowerOrEqualLevelBasicBlock(ifOp->getThenBranchBlock(), 
                                                                  ifOp->getThenBranchBlock()->getScopeLevel() -1);
            dumpHelper(ifOp->getThenBranchBlock());
            if(ifOp->getElseBranchBlock() && ifOp->getElseBranchBlock()->getScopeLevel() >= scopeLevel) {
                dumpHelper(ifOp->getElseBranchBlock());
            }
            dumpHelper(lastTerminatorOp);
            break;
        }
        case Operations::Operation::OperationType::ReturnOp:
            break;
        default:
            break;
    }
}

void NESIRDumpHandler::dumpHelper(BasicBlockPtr const& basicBlock) {
    if(!visitedBlocks.contains(basicBlock->getIdentifier())) {
        int32_t indent = basicBlock->getScopeLevel()+1;
        visitedBlocks.emplace(basicBlock->getIdentifier());
        out << '\n' << std::string(basicBlock->getScopeLevel() * 4, ' ') << basicBlock->getIdentifier() << '(';
        if(basicBlock->getInputArgs().size() > 0) {
            out << basicBlock->getInputArgs().at(0);
            for(int i = 1; i < (int) basicBlock->getInputArgs().size(); ++i) {
                out << ", " << basicBlock->getInputArgs().at(i);
            }
        }
        out << "):" << '\n';
        for(auto operation: basicBlock->getOperations()) {
            out << std::string(indent * 4, ' ') << operation->toString() << std::endl;
        }
        OperationPtr terminatorOp = basicBlock->getOperations().back();
        dumpHelper(terminatorOp, basicBlock->getScopeLevel());
    }
}

void NESIRDumpHandler::dump(const std::shared_ptr<Operations::FunctionOperation> funcOp) { 
    out << funcOp->toString() << " {";
    dumpHelper(funcOp->getFunctionBasicBlock()); 
    out << "}\n";
}

}// namespace ExecutionEngine::Experimental::IR {
}// namespace NES
