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

#include <Operators/IngestionTimeWatermarkAssignerLogicalOperator.hpp>

#include <algorithm>
#include <ranges>
#include <string>
#include <string_view>
#include <vector>

#include <fmt/format.h>
#include <fmt/ranges.h>
#include <folly/Hash.h>

#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Serialization/SchemaSerializationUtil.hpp>
#include <Traits/Trait.hpp>
#include <Util/PlanRenderer.hpp>
#include <ErrorHandling.hpp>
#include <LogicalOperatorRegistry.hpp>
#include <SerializableOperator.pb.h>
#include "Schema/Field.hpp"

namespace NES
{

// namespace
// {
// Schema inferOutputSchema(const Schema& inputSchema, const LogicalOperator& newOperator)
// {
//     return Schema{
//         inputSchema
//         | std::views::transform([&newOperator](const auto& field) { return Field{newOperator, field.getLastName(), field.getDataType()}; })
//         | std::ranges::to<std::vector>()};
// }
// }

IngestionTimeWatermarkAssignerLogicalOperator::IngestionTimeWatermarkAssignerLogicalOperator()
{
}

IngestionTimeWatermarkAssignerLogicalOperator::IngestionTimeWatermarkAssignerLogicalOperator(
    LogicalOperator child, DescriptorConfig::Config)
    : child(std::move(child))
{
    this->outputSchema = unbind(this->child->getOutputSchema());
}

std::string_view IngestionTimeWatermarkAssignerLogicalOperator::getName() const noexcept
{
    return NAME;
}

std::string IngestionTimeWatermarkAssignerLogicalOperator::explain(ExplainVerbosity verbosity, OperatorId id) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("INGESTIONTIMEWATERMARKASSIGNER(opId: {}, traitSet: {})", id, traitSet.explain(verbosity));
    }
    return "INGESTION_TIME_WATERMARK_ASSIGNER";
}

bool IngestionTimeWatermarkAssignerLogicalOperator::operator==(const IngestionTimeWatermarkAssignerLogicalOperator& rhs) const
{
    return getOutputSchema() == rhs.getOutputSchema() && getTraitSet() == rhs.getTraitSet();
}

IngestionTimeWatermarkAssignerLogicalOperator IngestionTimeWatermarkAssignerLogicalOperator::withInferredSchema() const
{
    PRECONDITION(child.has_value(), "Child not set when calling schema inference");
    auto copy = *this;
    copy.child = copy.child->withInferredSchema();
    const auto& inputSchema = copy.child->getOutputSchema();
    copy.outputSchema = unbind(inputSchema);
    return copy;
}

TraitSet IngestionTimeWatermarkAssignerLogicalOperator::getTraitSet() const
{
    return traitSet;
}

IngestionTimeWatermarkAssignerLogicalOperator IngestionTimeWatermarkAssignerLogicalOperator::withTraitSet(TraitSet traitSet) const
{
    auto copy = *this;
    copy.traitSet = traitSet;
    return copy;
}

IngestionTimeWatermarkAssignerLogicalOperator
IngestionTimeWatermarkAssignerLogicalOperator::withChildren(std::vector<LogicalOperator> children) const
{
    PRECONDITION(children.size() == 1, "Can only set exactly one child for ingestionTimeWatermarkAssigner, got {}", children.size());
    auto copy = *this;
    copy.child = children.at(0);
    return copy;
}

Schema IngestionTimeWatermarkAssignerLogicalOperator::getOutputSchema() const
{
    PRECONDITION(outputSchema.has_value(), "Accessed output schema before calling schema inference");
    return NES::bind(self.lock(), outputSchema.value());
}

std::vector<LogicalOperator> IngestionTimeWatermarkAssignerLogicalOperator::getChildren() const
{
    if (child.has_value())
    {
        return {*child};
    }
    return {};
}

void IngestionTimeWatermarkAssignerLogicalOperator::serialize(SerializableOperator& serializableOperator) const
{
    SerializableLogicalOperator proto;

    proto.set_operator_type(NAME);

    auto* outSch = proto.mutable_output_schema();
    SchemaSerializationUtil::serializeSchema(getOutputSchema(), outSch);

    for (auto& child : getChildren())
    {
        serializableOperator.add_children_ids(child.getId().getRawValue());
    }

    serializableOperator.mutable_operator_()->CopyFrom(proto);
}

LogicalOperatorRegistryReturnType
LogicalOperatorGeneratedRegistrar::RegisterIngestionTimeWatermarkAssignerLogicalOperator(LogicalOperatorRegistryArguments arguments)
{
    if (arguments.children.size() != 1)
    {
        throw CannotDeserialize(
            "Expected one child for IngestionTimeWatermarkAssignerLogicalOperator, but found {}", arguments.children.size());
    }
    return TypedLogicalOperator<IngestionTimeWatermarkAssignerLogicalOperator>{
        std::move(arguments.children.at(0)), DescriptorConfig::Config{}};
}

LogicalOperator IngestionTimeWatermarkAssignerLogicalOperator::getChild() const
{
    PRECONDITION(child.has_value(), "Child not set when trying to retrieve child");
    return child.value();
}

}

uint64_t std::hash<NES::IngestionTimeWatermarkAssignerLogicalOperator>::operator()(
    const NES::IngestionTimeWatermarkAssignerLogicalOperator&) const noexcept
{
    return 35890319;
}