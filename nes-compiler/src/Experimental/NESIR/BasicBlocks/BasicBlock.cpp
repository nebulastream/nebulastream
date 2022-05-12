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

#include <Experimental/NESIR/BasicBlocks/BasicBlock.hpp>
#include <utility>

namespace NES {
BasicBlock::BasicBlock(std::string identifier, std::vector<OperationPtr> operations, std::vector<std::string> inputArgs,
                        std::vector<std::shared_ptr<BasicBlock>> nextBlocks)
    : identifier(std::move(identifier)), operations(std::move(operations)), inputArgs(std::move(inputArgs)), nextBlocks(std::move(nextBlocks)) {}

std::string BasicBlock::getIdentifier() { return identifier; }

std::vector<OperationPtr> BasicBlock::getOperations() { return operations; }

std::vector<std::string> BasicBlock::getInputArgs() { return inputArgs; }

std::vector<std::shared_ptr<BasicBlock>> BasicBlock::getNextBlocks() { return nextBlocks; }

void BasicBlock::setNextBlocks(std::vector<std::shared_ptr<BasicBlock>> nextBlocks) {
    this->nextBlocks = std::move(nextBlocks);
};

}// namespace NES