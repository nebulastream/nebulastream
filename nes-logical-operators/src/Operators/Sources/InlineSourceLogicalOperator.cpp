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

#include <Operators/Sources/InlineSourceLogicalOperator.hpp>

#include <cstdint>
#include <functional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include <fmt/format.h>
#include <folly/hash/Hash.h>
/// NOLINTNEXTLINE(misc-include-cleaner
#include <Util/Hash.hpp>

#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <ErrorHandling.hpp>

namespace NES
{


InlineSourceLogicalOperator InlineSourceLogicalOperator::withInferredSchema()
{
    PRECONDITION(false, "Schema<Field, Unordered> inference should happen on SourceDescriptorLogicalOperator");
    std::unreachable();
}

const Schema<UnqualifiedUnboundField, Ordered>& InlineSourceLogicalOperator::getSourceSchema() const
{
    return sourceSchema;
}

bool InlineSourceLogicalOperator::operator==(const InlineSourceLogicalOperator& rhs) const
{
    /// Pointer equality because we don't know if two InlineSources with the exact same configuration are the same or not,
    /// this is only determined by the physical source id which is not assigned yet.
    return this == &rhs;
}

std::string InlineSourceLogicalOperator::explain(ExplainVerbosity verbosity, OperatorId id) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("INLINE_SOURCE(opId: {}, type: {} traitSet: {})", id, pluginSourceConfig.getType(), traitSet.explain(verbosity));
    }
    return fmt::format("INLINE_SOURCE({})", pluginSourceConfig.getType());
}

std::string_view InlineSourceLogicalOperator::getName() noexcept
{
    return NAME;
}

InlineSourceLogicalOperator InlineSourceLogicalOperator::withTraitSet(TraitSet traitSet) const
{
    auto copy = *this;
    copy.traitSet = std::move(traitSet);
    return copy;
}

TraitSet InlineSourceLogicalOperator::getTraitSet() const
{
    return traitSet;
}

InlineSourceLogicalOperator InlineSourceLogicalOperator::withChildrenUnsafe(std::vector<LogicalOperator> children) const
{
    auto copy = *this;
    copy.children = std::move(children);
    return copy;
}

/// NOLINTBEGIN(readability-convert-member-functions-to-static, performance-unnecessary-value-param)
InlineSourceLogicalOperator InlineSourceLogicalOperator::withChildren(std::vector<LogicalOperator>) const
{
    PRECONDITION(false, "Schema inference should happen on SourceDescriptorLogicalOperator");
    std::unreachable();
}

/// NOLINTEND(readability-convert-member-functions-to-static, performance-unnecessary-value-param)

Schema<Field, Unordered> InlineSourceLogicalOperator::getOutputSchema()
{
    INVARIANT(false, "Convert InlineSourceLogical Operator to SourceDescriptorLogicalOperator before retrieving output schema");
    std::unreachable();
}

std::vector<LogicalOperator> InlineSourceLogicalOperator::getChildren() const
{
    return children;
}

InlineSourceLogicalOperator::InlineSourceLogicalOperator(
    WeakLogicalOperator self,
    Schema<UnqualifiedUnboundField, Ordered> sourceSchema,
    GeneralSourceConfig generalSourceConfig,
    PluginSourceConfiguration pluginSourceConfig,
    InputFormatterDescriptor inputFormatterDescriptor)
    : ManagedByOperator(std::move(self))
    , sourceSchema(std::move(sourceSchema))
    , generalSourceConfig(std::move(generalSourceConfig))
    , pluginSourceConfig(std::move(pluginSourceConfig))
    , inputFormatterDescriptor(std::move(inputFormatterDescriptor))
{
}

TypedLogicalOperator<InlineSourceLogicalOperator> InlineSourceLogicalOperator::create(
    Schema<UnqualifiedUnboundField, Ordered> sourceSchema,
    GeneralSourceConfig generalSourceConfig,
    PluginSourceConfiguration pluginSourceConfig,
    InputFormatterDescriptor inputFormatterDescriptor)
{
    return TypedLogicalOperator<InlineSourceLogicalOperator>{
        std::move(sourceSchema), std::move(generalSourceConfig), std::move(pluginSourceConfig), std::move(inputFormatterDescriptor)};
}

const GeneralSourceConfig& InlineSourceLogicalOperator::getGeneralSourceConfig() const
{
    return generalSourceConfig;
}

const PluginSourceConfiguration& InlineSourceLogicalOperator::getPluginSourceConfig() const
{
    return pluginSourceConfig;
}

const InputFormatterDescriptor& InlineSourceLogicalOperator::getInputFormatterDescriptor() const
{
    return inputFormatterDescriptor;
}

Reflected
Reflector<TypedLogicalOperator<InlineSourceLogicalOperator>>::operator()(const TypedLogicalOperator<InlineSourceLogicalOperator>&) const
{
    PRECONDITION(false, "no serialize for InlineSourceLogicalOperator defined. Serialization happens with SourceDescriptorLogicalOperator");
    std::unreachable();
}

TypedLogicalOperator<InlineSourceLogicalOperator>
Unreflector<TypedLogicalOperator<InlineSourceLogicalOperator>>::operator()(const Reflected&, const ReflectionContext&) const
{
    PRECONDITION(false, "no serialize for InlineSourceLogicalOperator defined. Serialization happens with SourceDescriptorLogicalOperator");
    std::unreachable();
}

}

uint64_t std::hash<NES::InlineSourceLogicalOperator>::operator()(const NES::InlineSourceLogicalOperator& op) const noexcept
{
    return std::hash<const NES::InlineSourceLogicalOperator*>{}(&op);
}
