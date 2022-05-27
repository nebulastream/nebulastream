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
#include <Experimental/NESIR/Operations/IfOperation.hpp>

namespace NES::ExecutionEngine::Experimental::IR::Operations {
IfOperation::IfOperation(std::string boolArgName, std::vector<std::string> thenBlockArgs, std::vector<std::string> elseBlockArgs) 
    : Operation(Operation::IfOp), boolArgName(boolArgName), thenBlockArgs(std::move(thenBlockArgs)), elseBlockArgs(std::move(elseBlockArgs)) {}

    std::string IfOperation::getBoolArgName() { return boolArgName; }

    BasicBlockPtr IfOperation::setThenBranchBlock(BasicBlockPtr thenBlock) {
        this->thenBranchBlock = std::move(thenBlock);
        return this->thenBranchBlock;
    }
    BasicBlockPtr IfOperation::setElseBranchBlock(BasicBlockPtr elseBlock) {
        this->elseBranchBlock = std::move(elseBlock);
        return this->elseBranchBlock;
    }

    BasicBlockPtr IfOperation::getThenBranchBlock() { return thenBranchBlock; }
    BasicBlockPtr IfOperation::getElseBranchBlock() { return elseBranchBlock; }
    std::vector<std::string> IfOperation::getThenBlockArgs() { return thenBlockArgs; }
    std::vector<std::string> IfOperation::getElseBlockArgs() { return elseBlockArgs; }
    
}// namespace NES
