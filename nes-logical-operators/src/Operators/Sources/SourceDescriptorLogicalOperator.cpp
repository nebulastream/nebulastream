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

#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <fmt/format.h>
#include <fmt/ranges.h>

#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
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
}

std::string_view SourceDescriptorLogicalOperator::getName() const noexcept
{
    return NAME;
}

SourceDescriptorLogicalOperator SourceDescriptorLogicalOperator::withInferredSchema(const std::vector<Schema>&) const
{
    PRECONDITION(false, "Schema is already given by SourceDescriptor. No call ot InferSchema needed");
    return *this;
}

bool SourceDescriptorLogicalOperator::operator==(const SourceDescriptorLogicalOperator& rhs) const
{
    const bool descriptorsEqual = sourceDescriptor == rhs.sourceDescriptor;
    return descriptorsEqual && getOutputSchema() == rhs.getOutputSchema() && getInputSchemas() == rhs.getInputSchemas()
        && getTraitSet() == rhs.getTraitSet();
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

std::vector<Schema> SourceDescriptorLogicalOperator::getInputSchemas() const
{
    return {*sourceDescriptor.getLogicalSource().getSchema()};
};

Schema SourceDescriptorLogicalOperator::getOutputSchema() const
{
    return {*sourceDescriptor.getLogicalSource().getSchema()};
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

struct SerializedSourceDescriptorLogicalOperator
{
    rfl::Box<SerializedSourceDescriptor> descriptor;
};

void SourceDescriptorLogicalOperator::serialized(SerializedOperator& serialized) const
{
    serialized.config = rfl::to_generic(SerializedSourceDescriptorLogicalOperator{
        .descriptor = rfl::make_box<SerializedSourceDescriptor>(sourceDescriptor.serialized())}
    );
}

}
