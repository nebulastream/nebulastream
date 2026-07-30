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

#pragma once

#include <cstddef>
#include <memory>
#include <variant>
#include <vector>

#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <ExecutionContext.hpp>

namespace NES
{

/// Physical implementation of FMT. The format string is parsed once at
/// registration time into a sequence of fragments: each fragment is either a
/// literal byte run owned by this function, or a placeholder PhysicalFunction
/// (already wrapped in to_string upstream so its execute always returns a
/// VARSIZED). At execute time the fragments are concatenated into a single
/// arena-allocated VARSIZED.
class FmtPhysicalFunction final
{
public:
    struct LiteralFragment
    {
        std::shared_ptr<const std::byte[]> bytes;
        size_t size;
    };
    using Fragment = std::variant<LiteralFragment, PhysicalFunction>;

    explicit FmtPhysicalFunction(std::vector<Fragment> fragments);
    [[nodiscard]] VarVal execute(const Record& record, ArenaRef& arena) const;

private:
    std::vector<Fragment> fragments;
    /// Sum of literal-fragment sizes; placeholder sizes are added at execute time.
    size_t literalBytes = 0;
};

}
