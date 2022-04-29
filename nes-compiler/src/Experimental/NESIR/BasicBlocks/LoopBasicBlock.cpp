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

#include <Experimental/NESIR/BasicBlocks/LoopBasicBlock.hpp>
#include <utility>
namespace NES {
LoopBasicBlock::LoopBasicBlock(std::vector<Operation*> operations, BasicBlockPtr nextBlock, uint64_t upperLimit)
    : BasicBlock(BasicBlockType::LoopBasicBlock, std::move(operations)),
      nextBlock(std::move(nextBlock)), upperLimit(upperLimit) {}

uint64_t LoopBasicBlock::getUpperLimit() const { return upperLimit; }

bool LoopBasicBlock::classof(const BasicBlock *Block) {
    return Block->getBlockType() == BasicBlock::LoopBasicBlock;
}

}// namespace NES
