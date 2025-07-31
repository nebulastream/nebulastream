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

#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>

#include <ranges>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <fmt/format.h>
#include <fmt/ranges.h>

#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Traits/Trait.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/PlanRenderer.hpp>
#include <ErrorHandling.hpp>
#include <SerializableOperator.pb.h>

namespace NES
{

SourceDescriptorLogicalOperator::SourceDescriptorLogicalOperator(SourceDescriptor sourceDescriptor)
    : sourceDescriptor(std::move(sourceDescriptor))
{
    outputSchema = *this->sourceDescriptor.getLogicalSource().getSchema() | std::ranges::to<SchemaBase<UnboundFieldBase<1>, false>>();
}

std::string_view SourceDescriptorLogicalOperator::getName() const noexcept
{
    return NAME;
}

SourceDescriptorLogicalOperator SourceDescriptorLogicalOperator::withInferredSchema() const
{
    auto copy = *this;
    copy.outputSchema = *sourceDescriptor.getLogicalSource().getSchema() | std::ranges::to<SchemaBase<UnboundFieldBase<1>, false>>();
    return copy;
}

bool SourceDescriptorLogicalOperator::operator==(const SourceDescriptorLogicalOperator& rhs) const
{
    const bool descriptorsEqual = sourceDescriptor == rhs.sourceDescriptor;
    return descriptorsEqual && getTraitSet() == rhs.getTraitSet();
}

std::string SourceDescriptorLogicalOperator::explain(ExplainVerbosity verbosity, OperatorId id) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("SOURCE(opId: {}, {}, traitSet: {})", id, sourceDescriptor.explain(verbosity), traitSet.explain(verbosity));
    }
    return fmt::format("SOURCE({})", sourceDescriptor.explain(verbosity));
}

SourceDescriptorLogicalOperator SourceDescriptorLogicalOperator::withTraitSet(TraitSet traitSet) const
{
    auto copy = *this;
    copy.traitSet = std::move(traitSet);
    return copy;
}

TraitSet SourceDescriptorLogicalOperator::getTraitSet() const
{
    return traitSet;
}

SourceDescriptorLogicalOperator SourceDescriptorLogicalOperator::withChildren(std::vector<LogicalOperator> children) const
{
    auto copy = *this;
    copy.children = std::move(children);
    return copy;
}

Schema SourceDescriptorLogicalOperator::getOutputSchema() const
{
    PRECONDITION(outputSchema.has_value(), "Accessed output schema before calling schema inference");
    return NES::bind(self.lock(), outputSchema.value());
}

std::vector<LogicalOperator> SourceDescriptorLogicalOperator::getChildren() const
{
    return children;
}

SourceDescriptor SourceDescriptorLogicalOperator::getSourceDescriptor() const
{
    return sourceDescriptor;
}

void SourceDescriptorLogicalOperator::serialize(SerializableOperator& serializableOperator) const
{
    SerializableSourceDescriptorLogicalOperator proto;
    proto.mutable_sourcedescriptor()->CopyFrom(sourceDescriptor.serialize());

    serializableOperator.mutable_source()->CopyFrom(proto);
}

}

std::size_t std::hash<NES::SourceDescriptorLogicalOperator>::operator()(
    const NES::SourceDescriptorLogicalOperator& sourceDescriptorLogicalOperator) const
{
    return std::hash<NES::SourceDescriptor>{}(sourceDescriptorLogicalOperator.getSourceDescriptor());
}