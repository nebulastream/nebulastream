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

#include <Operators/Sources/SourceNameLogicalOperator.hpp>

#include <algorithm>
#include <cstddef>
#include <functional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <fmt/format.h>
#include <fmt/ranges.h>

#include <DataTypes/Schema.hpp>
#include <DataTypes/SchemaFwd.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Traits/Trait.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

SourceNameLogicalOperator::SourceNameLogicalOperator(WeakLogicalOperator self, Identifier logicalSourceName)
    : ManagedByOperator(std::move(self)), logicalSourceName(std::move(logicalSourceName))
{
}

TypedLogicalOperator<SourceNameLogicalOperator> SourceNameLogicalOperator::create(Identifier logicalSourceName)
{
    return TypedLogicalOperator<SourceNameLogicalOperator>{std::move(logicalSourceName)};
}

bool SourceNameLogicalOperator::operator==(const SourceNameLogicalOperator& rhs) const
{
    return this->logicalSourceName == rhs.logicalSourceName && getTraitSet() == rhs.getTraitSet();
}

SourceNameLogicalOperator SourceNameLogicalOperator::withInferredSchema() const
{
    PRECONDITION(false, "Schema<Field, Unordered> inference should happen on SourceDescriptorLogicalOperator");
    return *this;
}

std::string SourceNameLogicalOperator::explain(ExplainVerbosity verbosity, OperatorId id) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("SOURCE(opId: {}, name: {}, traitSet: {})", id, logicalSourceName, traitSet.explain(verbosity));
    }
    return fmt::format("SOURCE({})", logicalSourceName);
}

void SourceNameLogicalOperator::inferInputOrigins()
{
    /// Data sources have no input origins.
    NES_INFO("Data sources have no input origins, so inferInputOrigins is a noop.");
}

std::string_view SourceNameLogicalOperator::getName() const noexcept
{
    return "Source";
}

SourceNameLogicalOperator SourceNameLogicalOperator::withTraitSet(TraitSet traitSet) const
{
    auto copy = *this;
    copy.traitSet = std::move(traitSet);
    return copy;
}

TraitSet SourceNameLogicalOperator::getTraitSet() const
{
    return traitSet;
}

SourceNameLogicalOperator SourceNameLogicalOperator::withChildrenUnsafe(std::vector<LogicalOperator> children) const
{
    auto copy = *this;
    copy.children = std::move(children);
    return copy;
}

/// NOLINTBEGIN(readability-convert-member-functions-to-static, performance-unnecessary-value-param)
SourceNameLogicalOperator SourceNameLogicalOperator::withChildren(std::vector<LogicalOperator>) const
{
    PRECONDITION(false, "Schema inference should happen on SourceDescriptorLogicalOperator");
    std::unreachable();
}

/// NOLINTEND(readability-convert-member-functions-to-static, performance-unnecessary-value-param)

Schema<Field, Unordered> SourceNameLogicalOperator::getOutputSchema()
{
    INVARIANT(false, "SourceNameLogicalOperator does not define a output schema");
    std::unreachable();
}

std::vector<LogicalOperator> SourceNameLogicalOperator::getChildren() const
{
    return children;
}

Identifier SourceNameLogicalOperator::getLogicalSourceName() const
{
    return logicalSourceName;
}

Reflected Reflector<TypedLogicalOperator<SourceNameLogicalOperator>>::operator()(
    const TypedLogicalOperator<SourceNameLogicalOperator>&, const ReflectionContext&) const
{
    PRECONDITION(false, "no serialize for SourceNameLogicalOperator defined. Serialization happens with SourceDescriptorLogicalOperator");
    std::unreachable();
}

TypedLogicalOperator<SourceNameLogicalOperator>
Unreflector<TypedLogicalOperator<SourceNameLogicalOperator>>::operator()(const Reflected&, const ReflectionContext&) const
{
    PRECONDITION(false, "no serialize for SourceNameLogicalOperator defined. Serialization happens with SourceDescriptorLogicalOperator");
    std::unreachable();
}

}

std::size_t std::hash<NES::SourceNameLogicalOperator>::operator()(const NES::SourceNameLogicalOperator& sourceNameLogicalOperator) const
{
    return std::hash<NES::Identifier>{}(sourceNameLogicalOperator.getLogicalSourceName());
}
