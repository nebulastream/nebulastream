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

#include <Compiler/CompilerFlags.hpp>

namespace NES::Compiler {

std::list<std::string> CompilerFlags::getFlags() const { return flags; }

void CompilerFlags::addFlag(std::string flag) {
    flags.emplace_back(std::move(flag));
    auto [_, unique] = uniqueness.insert(flags.back());
    if (!unique) {
        flags.pop_back();
    }
}

void CompilerFlags::mergeFlags(const CompilerFlags& flags) {
    for (const auto& flag : flags.getFlags()) {
        addFlag(flag);
    }
}

// Copying the CompilerFlags is non trivial because the copied string_views would not point in the current vector
// We only copy the owning vector and redo the set.
CompilerFlags::CompilerFlags(const CompilerFlags& other) : flags(other.flags) {
    for (const auto& order : flags) {
        uniqueness.insert(order);
    }
}

CompilerFlags& CompilerFlags::operator=(const CompilerFlags& other) {
    if (this == &other)
        return *this;
    flags = other.flags;

    for (const auto& insertionOrder : flags) {
        uniqueness.insert(insertionOrder);
    }

    return *this;
}

// Moving is trivial because the string_view still references to the moved strings data
CompilerFlags::CompilerFlags(CompilerFlags&& other) noexcept = default;
CompilerFlags& CompilerFlags::operator=(CompilerFlags&& other) noexcept = default;

}// namespace NES::Compiler
