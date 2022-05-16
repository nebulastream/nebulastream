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

namespace NES {
IfOperation::IfOperation(std::string boolArgName, BasicBlockPtr thenBranchBlock, BasicBlockPtr elseBranchBlock, 
                         BasicBlockPtr afterIfBlock) 
    : Operation(Operation::IfOp), boolArgName(boolArgName), thenBranchBlock(std::move(thenBranchBlock)), 
    elseBranchBlock(std::move(elseBranchBlock)), afterIfBlock(afterIfBlock) {}

    std::string IfOperation::getBoolArgName() { return boolArgName; }
    BasicBlockPtr IfOperation::getThenBranchBlock() { return thenBranchBlock; }
    BasicBlockPtr IfOperation::getElseBranchBlock() { return elseBranchBlock; }
    BasicBlockPtr IfOperation::getAfterIfBlock() { return afterIfBlock; }
    
}// namespace NES
