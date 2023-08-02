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
#include <Nautilus/IR/BasicBlocks/BasicBlockArgument.hpp>

namespace NES::Nautilus::IR::Operations {
BasicBlockArgument::BasicBlockArgument(const OperationIdentifier identifier, Types::StampPtr stamp)
    : Operation(OperationType::BasicBlockArgument, identifier, stamp) {}
std::ostream& operator<<(std::ostream& os, const BasicBlockArgument& argument) {
    os << argument.identifier;
    return os;
}

std::string BasicBlockArgument::toString() { return identifier; }
BasicBlockArgument::~BasicBlockArgument() { NES_DEBUG("~BasicBlockArgument"); }
}// namespace NES::Nautilus::IR::Operations