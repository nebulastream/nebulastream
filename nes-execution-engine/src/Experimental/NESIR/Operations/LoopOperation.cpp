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

#include <Experimental/NESIR/Operations/LoopOperation.hpp>
#include <utility>
namespace NES {
LoopOperation::LoopOperation(LoopType loopType, const std::vector<std::string>& loopBlockArgs)
    : Operation(Operation::LoopOp), loopType(loopType), loopBlockArgs(std::move(loopBlockArgs)) {}

LoopOperation::LoopType LoopOperation::getLoopType() { return loopType; }
std::vector<std::string> LoopOperation::getLoopBlockArgs() { return loopBlockArgs; }

BasicBlockPtr LoopOperation::setLoopHeadBlock(BasicBlockPtr loopHeadBlock) {
    this->loopHeadBlock = std::move(loopHeadBlock);
    return this->loopHeadBlock;
}

BasicBlockPtr LoopOperation::getLoopHeadBlock() { return loopHeadBlock; }

bool LoopOperation::classof(const Operation *Op) {
    return Op->getOperationType() == Operation::LoopOp;
}

}// namespace NES
