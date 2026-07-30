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

#include "FmtPhysicalFunction.hpp"

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <utility>
#include <variant>
#include <vector>

#include <Functions/ConstantValueVariableSizePhysicalFunction.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <nautilus/std/cstring.h>
#include <Arena.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <PhysicalFunctionRegistry.hpp>
#include <function.hpp>
#include <val.hpp>

namespace NES
{

FmtPhysicalFunction::FmtPhysicalFunction(std::vector<Fragment> fragmentsIn) : fragments(std::move(fragmentsIn))
{
    for (const auto& fragment : fragments)
    {
        if (const auto* literal = std::get_if<LiteralFragment>(&fragment))
        {
            literalBytes += literal->size;
        }
    }
}

VarVal FmtPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    /// First pass: evaluate placeholder children and sum the runtime sizes.
    /// Their results are kept alive in `args` so subsequent arena allocations
    /// (the final concatenation buffer) can't invalidate them — the arena is
    /// linear within a buffer, so prior pointers stay valid.
    nautilus::val<uint64_t> totalSize{literalBytes};
    std::vector<VariableSizedData> args;
    for (const auto& fragment : nautilus::static_iterable(fragments))
    {
        if (const auto* placeholder = std::get_if<PhysicalFunction>(&fragment))
        {
            args.emplace_back(placeholder->execute(record, arena).getRawValueAs<VariableSizedData>());
            totalSize = totalSize + args.back().getSize();
        }
    }

    auto result = arena.allocateVariableSizedData(totalSize);

    /// Second pass: copy literals from the function's owned bytes and arg bytes
    /// from `args` into the result buffer in order.
    nautilus::val<uint64_t> offset{0};
    size_t argIndex = 0;
    for (const auto& fragment : nautilus::static_iterable(fragments))
    {
        if (const auto* literal = std::get_if<LiteralFragment>(&fragment))
        {
            nautilus::memcpy(result.getContent() + offset, literal->bytes.get(), literal->size);
            offset = offset + nautilus::val<uint64_t>{literal->size};
        }
        else
        {
            nautilus::memcpy(result.getContent() + offset, args[argIndex].getContent(), args[argIndex].getSize());
            offset = offset + args[argIndex].getSize();
            ++argIndex;
        }
    }

    return result;
}

PhysicalFunctionRegistryReturnType
PhysicalFunctionGeneratedRegistrar::RegisterFMTPhysicalFunction(PhysicalFunctionRegistryArguments args)
{
    PRECONDITION(!args.childFunctions.empty(), "FMT requires at least the format string as its first argument");

    /// The format string must have lowered to a constant VARSIZED literal.
    auto formatConst = args.childFunctions[0].tryGetAs<ConstantValueVariableSizePhysicalFunction>();
    if (!formatConst)
    {
        throw CannotInferSchema("FMT requires a constant VARSIZED literal as its first argument");
    }
    const auto& formatBytes = formatConst->get().getData();

    std::vector<FmtPhysicalFunction::Fragment> fragments;
    size_t placeholderIndex = 1;
    size_t literalStart = 0;
    bool inPlaceholder = false;
    for (size_t i = 0; i < formatBytes.size(); ++i)
    {
        const auto byte = formatBytes[i];
        if (!inPlaceholder && byte == '{')
        {
            if (i > literalStart)
            {
                const auto length = i - literalStart;
                auto buffer = std::make_unique<std::byte[]>(length);
                std::memcpy(buffer.get(), formatBytes.data() + literalStart, length);
                fragments.emplace_back(FmtPhysicalFunction::LiteralFragment{
                    .bytes = std::shared_ptr<const std::byte[]>(buffer.release()), .size = length});
            }
            inPlaceholder = true;
        }
        else if (inPlaceholder && byte == '}')
        {
            INVARIANT(placeholderIndex < args.childFunctions.size(), "FMT placeholder count exceeds argument count");
            fragments.emplace_back(args.childFunctions[placeholderIndex++]);
            literalStart = i + 1;
            inPlaceholder = false;
        }
    }
    INVARIANT(!inPlaceholder, "FMT format string has an unmatched '{{'");
    if (literalStart < formatBytes.size())
    {
        const auto length = formatBytes.size() - literalStart;
        auto buffer = std::make_unique<std::byte[]>(length);
        std::memcpy(buffer.get(), formatBytes.data() + literalStart, length);
        fragments.emplace_back(FmtPhysicalFunction::LiteralFragment{
            .bytes = std::shared_ptr<const std::byte[]>(buffer.release()), .size = length});
    }

    return FmtPhysicalFunction(std::move(fragments));
}

}
