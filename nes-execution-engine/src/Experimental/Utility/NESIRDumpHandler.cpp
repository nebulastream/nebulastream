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

//Todo why passing 'out' as a parameter? Its already a class parameter.
NESIRDumpHandler::NESIRDumpHandler(std::ostream& out) : out(out) {}

std::shared_ptr<NESIRDumpHandler> NESIRDumpHandler::create(std::ostream& out) {
    return std::make_shared<NESIRDumpHandler>(out);
}

OperationPtr NESIRDumpHandler::findLastTerminatorOp(BasicBlockPtr thenBlock, int ifParentBlockLevel) const {
    auto terminatorOp = thenBlock->getOperations().back();
    // Generate Args for next block
    if (terminatorOp->getOperationType() == Operations::Operation::BranchOp) {
        auto branchOp = std::static_pointer_cast<Operations::BranchOperation>(terminatorOp);
        if (branchOp->getNextBlock()->getScopeLevel() <= ifParentBlockLevel) {
            return branchOp;
        } else {
            return findLastTerminatorOp(branchOp->getNextBlock(), ifParentBlockLevel);
        }
    } else if (terminatorOp->getOperationType() == Operations::Operation::LoopOp) {
        auto loopOp = std::static_pointer_cast<Operations::LoopOperation>(terminatorOp);
        auto loopIfOp = std::static_pointer_cast<Operations::IfOperation>(loopOp->getLoopHeadBlock()->getOperations().back());
        if (loopIfOp->getElseBranchBlock()->getScopeLevel() <= ifParentBlockLevel) {
            return loopIfOp;
        } else {
            return findLastTerminatorOp(loopIfOp->getElseBranchBlock(), ifParentBlockLevel);
        }
    } else {
        auto ifOp = std::static_pointer_cast<Operations::IfOperation>(terminatorOp);
        if (ifOp->getElseBranchBlock() != nullptr) {
            return findLastTerminatorOp(ifOp->getElseBranchBlock(), ifParentBlockLevel);
        } else {
            return findLastTerminatorOp(ifOp->getThenBranchBlock(), ifParentBlockLevel);
        }
    }
}

void NESIRDumpHandler::dumpHelper(OperationPtr const& op, uint64_t indent, std::ostream& out) const {
    out << std::string(indent * 4, ' ') << op->toString() << std::endl;
}

void NESIRDumpHandler::dumpHelper(OperationPtr const& terminatorOp, uint64_t indent, std::ostream& out, 
                                  int32_t scopeLevel, bool isLoopHead) const {
    switch(terminatorOp->getOperationType()) {
        case Operations::Operation::OperationType::BranchOp: {
            auto branchOp = std::static_pointer_cast<Operations::BranchOperation>(terminatorOp);
            if(branchOp->getNextBlock()->getScopeLevel() >= scopeLevel) {
                dumpHelper(branchOp->getNextBlock(), indent, out);
            }
            break;
        }
        case Operations::Operation::OperationType::LoopOp: {
            auto loopOperation = std::static_pointer_cast<Operations::LoopOperation>(terminatorOp);
            dumpHelper(loopOperation->getLoopHeadBlock(), indent, out, true);
            break;
        }
        case Operations::Operation::OperationType::IfOp: {
            auto ifOp = std::static_pointer_cast<Operations::IfOperation>(terminatorOp);
            auto lastTerminatorOp = findLastTerminatorOp(ifOp->getThenBranchBlock(), ifOp->getThenBranchBlock()->getScopeLevel() -1);
            dumpHelper(ifOp->getThenBranchBlock(), indent, out);
            if(ifOp->getElseBranchBlock() && ifOp->getElseBranchBlock()->getScopeLevel() >= scopeLevel) {
                dumpHelper(ifOp->getElseBranchBlock(), indent, out);
            }
            if(!isLoopHead) {
            // if(ifOp->getBoolArgName() != "loopHeadCompareOp") {
                dumpHelper(lastTerminatorOp, indent, out, scopeLevel);
            }
            break;
        }
        case Operations::Operation::OperationType::ReturnOp:
            break;
        default:
            break;
    }
}

void NESIRDumpHandler::dumpHelper(BasicBlockPtr const& basicBlock, uint64_t indent, std::ostream& out, 
                                  bool isLoopHead) const {
    out << '\n' << std::string(indent * 4, ' ') << basicBlock->getIdentifier() << '(';
    if(basicBlock->getInputArgs().size() > 0) {
        out << basicBlock->getInputArgs().at(0);
        for(int i = 1; i < (int) basicBlock->getInputArgs().size(); ++i) {
            out << ", " << basicBlock->getInputArgs().at(i);
        }
    }
    out << "):" << '\n';
    ++indent;
    //Todo might make sense to iterate over complete operations then start with next block(s)
    for(auto operation: basicBlock->getOperations()) {
        dumpHelper(operation, indent, out);
    }
    --indent;
    OperationPtr terminatorOp = basicBlock->getOperations().back();
    dumpHelper(terminatorOp, indent, out, basicBlock->getScopeLevel(), isLoopHead);
    
}



void NESIRDumpHandler::dump(const std::shared_ptr<Operations::FunctionOperation> funcOp) { 
    out << std::string(4, ' ') << funcOp->toString() << std::endl;
    dumpHelper(funcOp->getFunctionBasicBlock(), 1, out); 
}
// void NESIRDumpHandler::dump(BasicBlockPtr basicBlock) { 

// }

}// namespace ExecutionEngine::Experimental::IR {
}// namespace NES
