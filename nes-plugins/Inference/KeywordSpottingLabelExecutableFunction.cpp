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

#include <memory>
#include <ranges>

#include <Functions/ConstantValueVariableSizePhysicalFunction.hpp>
#include <Functions/PhysicalFunction.hpp>

#include "PhysicalFunctionRegistry.hpp"

namespace NES
{
namespace
{

constexpr auto Labels = std::to_array<std::string_view>({
    "one", "two", "three", "_unknown_"
});

template <uint64_t index>
const ConstantValueVariableSizePhysicalFunction& switchStatementKwsLabel(
    nautilus::val<uint64_t>& argmax,
    const std::vector<ConstantValueVariableSizePhysicalFunction>& labels,
    const ConstantValueVariableSizePhysicalFunction& invalid)
{
    if (argmax == nautilus::val<uint64_t>(index - 1))
    {
        return labels[index - 1];
    }
    [[clang::musttail]] return switchStatementKwsLabel<index - 1>(argmax, labels, invalid);
}

template <>
const ConstantValueVariableSizePhysicalFunction& switchStatementKwsLabel<0>(
    nautilus::val<uint64_t>&,
    const std::vector<ConstantValueVariableSizePhysicalFunction>&,
    const ConstantValueVariableSizePhysicalFunction& invalid)
{
    return invalid;
}

}  // anonymous namespace

struct KeywordSpottingLabelPhysicalFunction final : PhysicalFunctionConcept
{
    [[nodiscard]] VarVal execute(const Record& record, ArenaRef& arena) const override
    {
        auto index = argmax.execute(record, arena).cast<nautilus::val<uint64_t>>();
        return switchStatementKwsLabel<Labels.size()>(index, labels, invalid).execute(record, arena);
    }

    KeywordSpottingLabelPhysicalFunction(PhysicalFunction argmax)
        : argmax(std::move(argmax))
        , labels(
              std::views::transform(Labels, [](auto&& label) { return ConstantValueVariableSizePhysicalFunction{label}; })
              | std::ranges::to<std::vector>())
    {
    }

    PhysicalFunction argmax;
    std::vector<ConstantValueVariableSizePhysicalFunction> labels;

    ConstantValueVariableSizePhysicalFunction invalid{"INVALID"};
};

}

namespace NES::PhysicalFunctionGeneratedRegistrar
{
PhysicalFunctionRegistryReturnType RegisterKeywordSpottingLabelPhysicalFunction(PhysicalFunctionRegistryArguments arguments)
{
    if (arguments.childFunctions.size() != 1)
    {
        throw TypeInferenceException("KeywordSpottingLabel expects 1 argument");
    }

    return PhysicalFunction(KeywordSpottingLabelPhysicalFunction(std::move(arguments.childFunctions[0])));
}
}
