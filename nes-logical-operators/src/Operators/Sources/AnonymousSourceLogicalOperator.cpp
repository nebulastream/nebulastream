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

#include <Operators/Sources/AnonymousSourceLogicalOperator.hpp>

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
#include <Sources/SourceCatalog.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <ErrorHandling.hpp>
#include <InputFormatterDescriptor.hpp>

namespace NES
{


AnonymousSourceLogicalOperator AnonymousSourceLogicalOperator::withInferredSchema()
{
    PRECONDITION(false, "Schema<Field, Unordered> inference should happen on SourceDescriptorLogicalOperator");
    std::unreachable();
}

const Schema<UnqualifiedUnboundField, Ordered>& AnonymousSourceLogicalOperator::getSourceSchema() const
{
    return sourceSchema;
}

bool AnonymousSourceLogicalOperator::operator==(const AnonymousSourceLogicalOperator& rhs) const
{
    /// Pointer equality because we don't know if two AnonymousSources with the exact same configuration are the same or not,
    /// this is only determined by the physical source id which is not assigned yet.
    return this == &rhs;
}

std::string AnonymousSourceLogicalOperator::explain(ExplainVerbosity verbosity, OperatorId id) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("INLINE_SOURCE(opId: {}, type: {} traitSet: {})", id, pluginSourceConfig.getType(), traitSet.explain(verbosity));
    }
    return fmt::format("INLINE_SOURCE({})", pluginSourceConfig.getType());
}

std::string_view AnonymousSourceLogicalOperator::getName() noexcept
{
    return NAME;
}

AnonymousSourceLogicalOperator AnonymousSourceLogicalOperator::withTraitSet(TraitSet traitSet) const
{
    auto copy = *this;
    copy.traitSet = std::move(traitSet);
    return copy;
}

TraitSet AnonymousSourceLogicalOperator::getTraitSet() const
{
    return traitSet;
}

AnonymousSourceLogicalOperator AnonymousSourceLogicalOperator::withChildrenUnsafe(std::vector<LogicalOperator> children) const
{
    auto copy = *this;
    copy.children = std::move(children);
    return copy;
}

/// NOLINTBEGIN(readability-convert-member-functions-to-static, performance-unnecessary-value-param)
AnonymousSourceLogicalOperator AnonymousSourceLogicalOperator::withChildren(std::vector<LogicalOperator>) const
{
    PRECONDITION(false, "Schema inference should happen on SourceDescriptorLogicalOperator");
    std::unreachable();
}

/// NOLINTEND(readability-convert-member-functions-to-static, performance-unnecessary-value-param)

Schema<Field, Unordered> AnonymousSourceLogicalOperator::getOutputSchema()
{
    INVARIANT(false, "Convert AnonymousSourceLogical Operator to SourceDescriptorLogicalOperator before retrieving output schema");
    std::unreachable();
}

std::vector<LogicalOperator> AnonymousSourceLogicalOperator::getChildren() const
{
    return children;
}

AnonymousSourceLogicalOperator::AnonymousSourceLogicalOperator(
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

TypedLogicalOperator<AnonymousSourceLogicalOperator> AnonymousSourceLogicalOperator::create(
    Schema<UnqualifiedUnboundField, Ordered> sourceSchema,
    GeneralSourceConfig generalSourceConfig,
    PluginSourceConfiguration pluginSourceConfig,
    InputFormatterDescriptor inputFormatterDescriptor)
{
    return TypedLogicalOperator<AnonymousSourceLogicalOperator>{
        std::move(sourceSchema), std::move(generalSourceConfig), std::move(pluginSourceConfig), std::move(inputFormatterDescriptor)};
}

const GeneralSourceConfig& AnonymousSourceLogicalOperator::getGeneralSourceConfig() const
{
    return generalSourceConfig;
}

const PluginSourceConfiguration& AnonymousSourceLogicalOperator::getPluginSourceConfig() const
{
    return pluginSourceConfig;
}

const InputFormatterDescriptor& AnonymousSourceLogicalOperator::getInputFormatterDescriptor() const
{
    return inputFormatterDescriptor;
}

Reflected Reflector<TypedLogicalOperator<AnonymousSourceLogicalOperator>>::operator()(
    const TypedLogicalOperator<AnonymousSourceLogicalOperator>&, const ReflectionContext&) const
{
    PRECONDITION(
        false, "no serialize for AnonymousSourceLogicalOperator defined. Serialization happens with SourceDescriptorLogicalOperator");
    std::unreachable();
}

TypedLogicalOperator<AnonymousSourceLogicalOperator>
Unreflector<TypedLogicalOperator<AnonymousSourceLogicalOperator>>::operator()(const Reflected&, const ReflectionContext&) const
{
    PRECONDITION(
        false, "no serialize for AnonymousSourceLogicalOperator defined. Serialization happens with SourceDescriptorLogicalOperator");
    std::unreachable();
}

}

uint64_t std::hash<NES::AnonymousSourceLogicalOperator>::operator()(const NES::AnonymousSourceLogicalOperator& op) const noexcept
{
    return std::hash<const NES::AnonymousSourceLogicalOperator*>{}(&op);
}
