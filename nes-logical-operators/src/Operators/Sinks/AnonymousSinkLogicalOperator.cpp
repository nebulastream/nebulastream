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

#include <Operators/Sinks/AnonymousSinkLogicalOperator.hpp>

#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>


#include <fmt/format.h>


#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <OutputFormatters/OutputFormatterDescriptor.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/Hash.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <folly/hash/Hash.h>
#include <ErrorHandling.hpp>

namespace NES
{

AnonymousSinkLogicalOperator::AnonymousSinkLogicalOperator(
    WeakLogicalOperator self,
    Identifier sinkType,
    AnonymousSinkSchema schema,
    GeneralSinkConfig generalSinkConfig,
    PluginSinkConfiguration pluginSinkConfiguration,
    OutputFormatterDescriptor outputFormatterDescriptor)
    : ManagedByOperator{std::move(self)}
    , sinkType(std::move(sinkType))
    , targetSchema(std::move(schema))
    , generalSinkConfig(std::move(generalSinkConfig))
    , pluginSinkConfig(std::move(pluginSinkConfiguration))
    , outputFormatterDescriptor(std::move(outputFormatterDescriptor))
{
}

TypedLogicalOperator<AnonymousSinkLogicalOperator> AnonymousSinkLogicalOperator::create(
    Identifier sinkType,
    AnonymousSinkSchema schema,
    GeneralSinkConfig generalSinkConfig,
    PluginSinkConfiguration pluginSinkConfiguration,
    OutputFormatterDescriptor outputFormatterDescriptor)
{
    return TypedLogicalOperator<AnonymousSinkLogicalOperator>{
        std::move(sinkType),
        std::move(schema),
        std::move(generalSinkConfig),
        std::move(pluginSinkConfiguration),
        std::move(outputFormatterDescriptor)};
}

AnonymousSinkLogicalOperator AnonymousSinkLogicalOperator::withInferredSchema()
{
    PRECONDITION(false, "Schema inference should happen on SinkLogicalOperator");
    std::unreachable();
}

Identifier AnonymousSinkLogicalOperator::getSinkType() const
{
    return sinkType;
}

bool AnonymousSinkLogicalOperator::operator==(const AnonymousSinkLogicalOperator& rhs) const
{
    return this == &rhs;
}

std::string AnonymousSinkLogicalOperator::explain(ExplainVerbosity verbosity, OperatorId id) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("ANONYMOUS_SINK(opId: {}, name: {}, traitSet: {})", id, NAME, traitSet.explain(verbosity));
    }
    return fmt::format("ANONYMOUS_SINK({})", NAME);
}

std::string_view AnonymousSinkLogicalOperator::getName() noexcept
{
    return NAME;
}

AnonymousSinkLogicalOperator AnonymousSinkLogicalOperator::withTraitSet(TraitSet traitSet) const
{
    auto copy = *this;
    copy.traitSet = std::move(traitSet);
    return copy;
}

TraitSet AnonymousSinkLogicalOperator::getTraitSet() const
{
    return traitSet;
}

AnonymousSinkLogicalOperator AnonymousSinkLogicalOperator::withChildrenUnsafe(std::vector<LogicalOperator> children) const
{
    auto copy = *this;
    copy.children = std::move(children);
    return copy;
}

/// NOLINTBEGIN(readability-convert-member-functions-to-static, performance-unnecessary-value-param)
AnonymousSinkLogicalOperator AnonymousSinkLogicalOperator::withChildren(std::vector<LogicalOperator>) const
{
    PRECONDITION(false, "Schema inference should happen on SinkLogicalOperator");
    std::unreachable();
}

/// NOLINTEND(readability-convert-member-functions-to-static, performance-unnecessary-value-param)

Schema<Field, Unordered> AnonymousSinkLogicalOperator::getOutputSchema()
{
    INVARIANT(false, "SinkLogicalOperator does not define an output schema");
    std::unreachable();
}

std::vector<LogicalOperator> AnonymousSinkLogicalOperator::getChildren() const
{
    return children;
}

AnonymousSinkSchema AnonymousSinkLogicalOperator::getSinkSchema() const
{
    return targetSchema;
}

GeneralSinkConfig AnonymousSinkLogicalOperator::getGeneralSinkConfig() const
{
    return generalSinkConfig;
}

PluginSinkConfiguration AnonymousSinkLogicalOperator::getPluginSinkConfiguration() const
{
    return pluginSinkConfig;
}

OutputFormatterDescriptor AnonymousSinkLogicalOperator::getOutputFormatterDescriptor() const
{
    return outputFormatterDescriptor;
}

Reflected Reflector<TypedLogicalOperator<AnonymousSinkLogicalOperator>>::operator()(
    const TypedLogicalOperator<AnonymousSinkLogicalOperator>&, const ReflectionContext&) const
{
    PRECONDITION(false, "no serialize for AnonymousSinkLogicalOperator defined. Serialization happens with SinkLogicalOperator");
    std::unreachable();
}

TypedLogicalOperator<AnonymousSinkLogicalOperator>
Unreflector<TypedLogicalOperator<AnonymousSinkLogicalOperator>>::operator()(const Reflected&, const ReflectionContext&) const
{
    PRECONDITION(false, "no serialize for AnonymousSinkLogicalOperator defined. Serialization happens with SinkLogicalOperator");
    std::unreachable();
}

}

uint64_t std::hash<NES::AnonymousSinkLogicalOperator>::operator()(const NES::AnonymousSinkLogicalOperator& op) const noexcept
{
    return folly::hash::hash_combine_generic(NES::Hash{}, op.getSinkType());
}
