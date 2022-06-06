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

void NESIRDumpHandler::dumpHelper(OperationPtr const& op, uint64_t indent, std::ostream& out) const {
    out << std::string(indent * 4, ' ') << op->toString() << std::endl;
}

void NESIRDumpHandler::dumpHelper(BasicBlockPtr const& basicBlock, uint64_t indent, std::ostream& out) const {
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
    switch(terminatorOp->getOperationType()) {
        case IR::Operations::Operation::OperationType::BranchOp: {
            auto loopOperation = std::static_pointer_cast<IR::Operations::BranchOperation>(terminatorOp);
            dumpHelper(loopOperation->getNextBlock(), indent, out);
            break;
        }
        case IR::Operations::Operation::OperationType::LoopOp: {
            auto loopOperation = std::static_pointer_cast<IR::Operations::LoopOperation>(terminatorOp);
            dumpHelper(loopOperation->getLoopHeadBlock(), indent, out);
            break;
        }
        case IR::Operations::Operation::OperationType::IfOp: {
            auto loopOperation = std::static_pointer_cast<IR::Operations::IfOperation>(terminatorOp);
            dumpHelper(loopOperation->getThenBranchBlock(), indent, out);
            dumpHelper(loopOperation->getElseBranchBlock(), indent, out);
            break;
        }
        case IR::Operations::Operation::OperationType::ReturnOp:
            break;
        default:
            break;
    }
}



void NESIRDumpHandler::dump(const std::shared_ptr<Operations::FunctionOperation> funcOp) { 
    out << std::string(4, ' ') << funcOp->toString() << std::endl;
    dumpHelper(funcOp->getFunctionBasicBlock(), 1, out); 
}
// void NESIRDumpHandler::dump(BasicBlockPtr basicBlock) { 

// }

}// namespace ExecutionEngine::Experimental::IR {
}// namespace NES
