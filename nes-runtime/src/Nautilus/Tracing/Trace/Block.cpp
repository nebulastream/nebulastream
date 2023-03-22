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

#include <Nautilus/Tracing/Trace/Block.hpp>
#include <Nautilus/Tracing/ValueRef.hpp>
#include <algorithm>

namespace NES::Nautilus::Tracing {

Block::Block(uint32_t blockId) : blockId(blockId), type(Type::Default){};

bool Block::isLocalValueRef(ValueRef& ref) {

    for (const auto& operation : operations) {
        if (auto resultValueRef = std::get_if<ValueRef>(&operation.result)) {
            if (resultValueRef->blockId == ref.blockId && resultValueRef->operationId == ref.operationId) {
                return true;
            }
        }
    }
    //if (ref.blockId == blockId) {
    // this is a local ref
    //    return true;
    //}
    return std::find(arguments.begin(), arguments.end(), ref) != arguments.end();
}

void Block::addArgument(ValueRef ref) {
    // only add ref to arguments if it not already exists
    if (std::find(arguments.begin(), arguments.end(), ref) == arguments.end()) {
        arguments.emplace_back(ref);
    }
}

std::ostream& operator<<(std::ostream& os, const Block& block) {
    os << "(";
    for (size_t i = 0; i < block.arguments.size(); i++) {
        os << block.arguments[i] << ",";
    }
    os << ")";
    if (block.type == Block::Type::ControlFlowMerge) {
        os << " ControlFlowMerge";
    }
    os << "\n";
    for (size_t i = 0; i < block.operations.size(); i++) {
        os << "\t" << block.operations[i] << "\n";
    }
    return os;
}

}// namespace NES::Nautilus::Tracing