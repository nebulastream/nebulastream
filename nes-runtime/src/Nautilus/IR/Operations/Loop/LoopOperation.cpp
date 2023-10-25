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

#include <Nautilus/IR/BasicBlocks/BasicBlockInvocation.hpp>
#include <Nautilus/IR/Operations/Loop/LoopOperation.hpp>
#include <Nautilus/IR/Types/StampFactory.hpp>
#include <utility>
namespace NES::Nautilus::IR::Operations {
LoopOperation::LoopOperation(LoopType loopType)
    : Operation(Operation::OperationType::LoopOp, Types::StampFactory::createVoidStamp()), loopType(loopType) {}

LoopOperation::LoopType LoopOperation::getLoopType() { return loopType; }
// Todo leads to segfault
void LoopOperation::setLoopType(LoopOperation::LoopType loopType) { this->loopType = loopType; }

BasicBlockInvocation& LoopOperation::getLoopBodyBlock() { return loopBodyBlock; }
BasicBlockInvocation& LoopOperation::getLoopFalseBlock() { return loopFalseBlock; }
// void LoopOperation::setLoopBodyBlock(BasicBlockInvocation loopBodyBlock) { this->loopBodyBlock = loopBodyBlock.getBlock(); }
// void LoopOperation::setLoopFalseBlock(BasicBlockInvocation loopFalseBlock) { this->loopFalseBlock = loopFalseBlock.getBlock(); }
BasicBlockInvocation& LoopOperation::getLoopHeadBlock() { return loopHeadBlock; }
BasicBlockInvocation& LoopOperation::getLoopEndBlock() { return loopEndBlock; }

void LoopOperation::setLoopInfo(std::shared_ptr<LoopInfo> loopInfo) { this->loopInfo = loopInfo; }

std::shared_ptr<LoopInfo> LoopOperation::getLoopInfo() { return loopInfo; }

std::string LoopOperation::toString() {
    std::string baseString = "loop " + loopHeadBlock.getBlock()->getIdentifier() + "(";
    auto loopBlockArgs = loopHeadBlock.getArguments();
    if (loopBlockArgs.size() > 0) {
        baseString += loopBlockArgs[0]->getIdentifier();
        for (int i = 1; i < (int) loopBlockArgs.size(); ++i) {
            baseString += ", " + loopBlockArgs.at(i)->getIdentifier();
        }
    }
    return baseString + ")";
}

}// namespace NES::Nautilus::IR::Operations
