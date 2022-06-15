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
#include <Experimental/NESIR/BasicBlocks/BasicBlockInvocation.hpp>

namespace NES::ExecutionEngine::Experimental::IR::Operations {

BasicBlockInvocation::BasicBlockInvocation() {}

void BasicBlockInvocation::setBlock(BasicBlockPtr block) { this->basicBlock = block; }

BasicBlockPtr BasicBlockInvocation::getBlock() { return basicBlock; }

void BasicBlockInvocation::addArgument(OperationPtr argument) { this->operations.emplace_back(argument); }

void BasicBlockInvocation::removeArgument(uint64_t argumentIndex) {
    operations.erase(operations.begin() + argumentIndex);
}

std::vector<OperationPtr> BasicBlockInvocation::getArguments() {
    std::vector<OperationPtr> arguments;
    for (auto& arg : this->operations) {
        arguments.emplace_back(arg.lock());
    }
    return arguments;
}

}// namespace NES::ExecutionEngine::Experimental::IR::Operations