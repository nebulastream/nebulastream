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
#include "Nautilus/IR/BasicBlocks/BasicBlock.hpp"
#include "Nautilus/IR/Operations/Operation.hpp"
#include <Nautilus/IR/BasicBlocks/BasicBlockInvocation.hpp>
#include <Nautilus/IR/Types/StampFactory.hpp>
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <fmt/core.h>
#include <fmt/format.h>
#include <iterator>
#include <memory>
#include <vector>

namespace NES::Nautilus::IR::Operations {

BasicBlockInvocation::BasicBlockInvocation()
    : Operation(OperationType::BlockInvocation, Types::StampFactory::createVoidStamp()) {}

void BasicBlockInvocation::setBlock(BasicBlock& block) { this->basicBlock = &block; }

BasicBlock* BasicBlockInvocation::getBlock() { return basicBlock; }
const BasicBlock* BasicBlockInvocation::getBlock() const { return basicBlock; }

void BasicBlockInvocation::addArgument(Operation& argument) {
    this->operations.emplace_back(&argument);
    argument.addUsage(*this);
}

void BasicBlockInvocation::removeArgument(uint64_t argumentIndex) { operations.erase(operations.begin() + argumentIndex); }

size_t BasicBlockInvocation::getOperationArgIndex(const Operation& arg) const {
    auto it = std::find_if(operations.begin(), operations.end(), [&arg](const auto& op) {
        op == std::addressof(arg);
    });
    return it == operations.end() ? -1 : std::distance(operations.begin(), operations.end());
}

std::vector<OperationRef> BasicBlockInvocation::getArguments() const { return operations; }
std::string BasicBlockInvocation::toString() const {
    if (basicBlock) {
        return fmt::format("{}({})", basicBlock->getIdentifier(), fmt::join(operations.begin(), operations.end(), ","));
    } else {
        return fmt::format("NULL");
    }
}

}// namespace NES::Nautilus::IR::Operations