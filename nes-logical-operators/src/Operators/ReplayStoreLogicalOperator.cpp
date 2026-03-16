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

#include <Operators/ReplayStoreLogicalOperator.hpp>

#include <ranges>
#include <sstream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include <Configurations/Descriptor.hpp>
#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Traits/Trait.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <LogicalOperatorRegistry.hpp>

namespace NES
{

std::string ReplayStoreLogicalOperator::explain(ExplainVerbosity verbosity, OperatorId id) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        std::stringstream cfg;
        Descriptor tmp{DescriptorConfig::Config(config)};
        cfg << &tmp;
        return fmt::format("REPLAY_STORE(opId: {}, config: {}, schema: {})", id, cfg.str(), getOutputSchema());
    }
    return {"REPLAY_STORE"};
}

std::string_view ReplayStoreLogicalOperator::getName() noexcept
{
    return NAME;
}

ReplayStoreLogicalOperator ReplayStoreLogicalOperator::withTraitSet(TraitSet ts) const
{
    auto copy = *this;
    copy.traitSet = std::move(ts);
    return copy;
}

TraitSet ReplayStoreLogicalOperator::getTraitSet() const
{
    return traitSet;
}

ReplayStoreLogicalOperator ReplayStoreLogicalOperator::withChildren(std::vector<LogicalOperator> newChildren) const
{
    auto copy = *this;
    copy.children = std::move(newChildren);
    return copy;
}

std::vector<LogicalOperator> ReplayStoreLogicalOperator::getChildren() const
{
    return children;
}

std::vector<Schema> ReplayStoreLogicalOperator::getInputSchemas() const
{
    INVARIANT(!children.empty(), "ReplayStore operator requires exactly one child");
    return children | std::ranges::views::transform([](const LogicalOperator& child) { return child.getOutputSchema(); })
        | std::ranges::to<std::vector>();
}

Schema ReplayStoreLogicalOperator::getOutputSchema() const
{
    INVARIANT(!children.empty(), "ReplayStore operator requires exactly one child");
    return children.front().getOutputSchema();
}

ReplayStoreLogicalOperator ReplayStoreLogicalOperator::withInferredSchema(std::vector<Schema> inputSchemas) const
{
    auto copy = *this;
    INVARIANT(!inputSchemas.empty(), "ReplayStore should have at least one input");
    const auto& first = inputSchemas.front();
    for (const auto& s : inputSchemas)
    {
        if (s != first)
        {
            throw CannotInferSchema("All input schemas must be equal for ReplayStore operator");
        }
    }
    return copy;
}

bool ReplayStoreLogicalOperator::operator==(const ReplayStoreLogicalOperator& rhs) const
{
    return getOutputSchema() == rhs.getOutputSchema() && getInputSchemas() == rhs.getInputSchemas() && getTraitSet() == rhs.getTraitSet()
        && config == rhs.config;
}

ReplayStoreLogicalOperator ReplayStoreLogicalOperator::withConfig(DescriptorConfig::Config validatedConfig) const
{
    auto copy = *this;
    copy.config = std::move(validatedConfig);
    return copy;
}

DescriptorConfig::Config ReplayStoreLogicalOperator::validateAndFormatConfig(std::unordered_map<std::string, std::string> configPairs)
{
    return DescriptorConfig::validateAndFormat<ConfigParameters>(std::move(configPairs), std::string(NAME));
}

}

namespace NES
{

Reflected
Reflector<TypedLogicalOperator<ReplayStoreLogicalOperator>>::operator()(const TypedLogicalOperator<ReplayStoreLogicalOperator>& op) const
{
    return reflect(detail::ReflectedStoreLogicalOperator{.config = op->getConfig()});
}

TypedLogicalOperator<ReplayStoreLogicalOperator>
Unreflector<TypedLogicalOperator<ReplayStoreLogicalOperator>>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    auto [config] = context.unreflect<detail::ReflectedStoreLogicalOperator>(reflected);
    return TypedLogicalOperator<ReplayStoreLogicalOperator>{ReplayStoreLogicalOperator(std::move(config))};
}

/// NOLINTNEXTLINE(performance-unnecessary-value-param)
LogicalOperatorRegistryReturnType
LogicalOperatorGeneratedRegistrar::RegisterReplayStoreLogicalOperator(LogicalOperatorRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return ReflectionContext{}.unreflect<TypedLogicalOperator<ReplayStoreLogicalOperator>>(arguments.reflected);
    }
    PRECONDITION(false, "Operator is only built directly via parser or via reflection, not using the registry");
    std::unreachable();
}

}
