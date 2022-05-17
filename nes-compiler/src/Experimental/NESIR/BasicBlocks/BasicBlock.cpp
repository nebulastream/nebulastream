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
#include <cstdint>
#include <utility>

namespace NES {
BasicBlock::BasicBlock(std::string identifier, std::vector<OperationPtr> operations, std::vector<std::string> inputArgs, 
                       int32_t scopeLevel)
    : identifier(std::move(identifier)), operations(std::move(operations)), inputArgs(std::move(inputArgs)), 
      scopeLevel(scopeLevel) {}

std::string BasicBlock::getIdentifier() { return identifier; }

std::vector<OperationPtr> BasicBlock::getOperations() { return operations; }

std::vector<std::string> BasicBlock::getInputArgs() { return inputArgs; }
int32_t BasicBlock::getScopeLevel() { return scopeLevel; }

void BasicBlock::addOperation(OperationPtr operation) { operations.push_back(operation); }
void BasicBlock::popOperation() { operations.pop_back(); }

}// namespace NES