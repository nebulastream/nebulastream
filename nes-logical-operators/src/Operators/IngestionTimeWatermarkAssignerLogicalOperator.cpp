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

#include <string>
#include <string_view>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Operators/IngestionTimeWatermarkAssignerLogicalOperator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Serialization/SchemaSerializationUtil.hpp>
#include <Traits/Trait.hpp>
#include <Util/PlanRenderer.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <ErrorHandling.hpp>
#include <LogicalOperatorRegistry.hpp>
#include <SerializableOperator.pb.h>

namespace NES
{

IngestionTimeWatermarkAssignerLogicalOperator::IngestionTimeWatermarkAssignerLogicalOperator() = default;

std::string_view IngestionTimeWatermarkAssignerLogicalOperator::getName() const noexcept
{
    return NAME;
}

std::string IngestionTimeWatermarkAssignerLogicalOperator::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        std::string inputOriginIdsStr;
        if (!inputOriginIds.empty())
        {
            inputOriginIdsStr = fmt::format(", inputOriginIds: [{}]", fmt::join(inputOriginIds, ", "));
        }
        return fmt::format("INGESTIONTIMEWATERMARKASSIGNER(opId: {}, inputSchema: {}{})", id, inputSchema, inputOriginIdsStr);
    }
    return "INGESTION_TIME_WATERMARK_ASSIGNER";
}

bool IngestionTimeWatermarkAssignerLogicalOperator::operator==(const LogicalOperatorConcept& rhs) const
{
    if (const auto* const rhsOperator = dynamic_cast<const IngestionTimeWatermarkAssignerLogicalOperator*>(&rhs))
    {
        return getOutputSchema() == rhsOperator->getOutputSchema() && getInputSchemas() == rhsOperator->getInputSchemas()
            && getInputOriginIds() == rhsOperator->getInputOriginIds() && getOutputOriginIds() == rhsOperator->getOutputOriginIds()
            && getTraitSet() == rhsOperator->getTraitSet();
    }
    return false;
}

LogicalOperator IngestionTimeWatermarkAssignerLogicalOperator::withInferredSchema(std::vector<Schema> inputSchemas) const
{
    auto copy = *this;
    PRECONDITION(inputSchemas.size() == 1, "Watermark assigner should have only one input");
    const auto& inputSchema = inputSchemas[0];
    copy.inputSchema = inputSchema;
    copy.outputSchema = inputSchema;
    return copy;
}

TraitSet IngestionTimeWatermarkAssignerLogicalOperator::getTraitSet() const
{
    return traitSet;
}

LogicalOperator IngestionTimeWatermarkAssignerLogicalOperator::withTraitSet(TraitSet traitSet) const
{
    auto copy = *this;
    copy.traitSet = traitSet;
    return copy;
}

LogicalOperator IngestionTimeWatermarkAssignerLogicalOperator::withChildren(std::vector<LogicalOperator> children) const
{
    auto copy = *this;
    copy.children = children;
    return copy;
}

std::vector<Schema> IngestionTimeWatermarkAssignerLogicalOperator::getInputSchemas() const
{
    return {inputSchema};
};

Schema IngestionTimeWatermarkAssignerLogicalOperator::getOutputSchema() const
{
    return outputSchema;
}

std::vector<std::vector<OriginId>> IngestionTimeWatermarkAssignerLogicalOperator::getInputOriginIds() const
{
    return {inputOriginIds};
}

std::vector<OriginId> IngestionTimeWatermarkAssignerLogicalOperator::getOutputOriginIds() const
{
    return outputOriginIds;
}

LogicalOperator IngestionTimeWatermarkAssignerLogicalOperator::withInputOriginIds(std::vector<std::vector<OriginId>> ids) const
{
    PRECONDITION(ids.size() == 1, "Watermark assigner should have only one input");
    auto copy = *this;
    copy.inputOriginIds = ids[0];
    return copy;
}

LogicalOperator IngestionTimeWatermarkAssignerLogicalOperator::withOutputOriginIds(std::vector<OriginId> ids) const
{
    auto copy = *this;
    copy.outputOriginIds = ids;
    return copy;
}

std::vector<LogicalOperator> IngestionTimeWatermarkAssignerLogicalOperator::getChildren() const
{
    return children;
}

SerializableOperator IngestionTimeWatermarkAssignerLogicalOperator::serialize() const
{
    SerializableLogicalOperator proto;

    proto.set_operator_type(NAME);
    auto* traitSetProto = proto.mutable_trait_set();
    for (const auto& trait : getTraitSet())
    {
        *traitSetProto->add_traits() = trait.serialize();
    }

    for (const auto& inputSchema : getInputSchemas())
    {
        auto* schProto = proto.add_input_schemas();
        SchemaSerializationUtil::serializeSchema(inputSchema, schProto);
    }

    for (const auto& originList : getInputOriginIds())
    {
        auto* olist = proto.add_input_origin_lists();
        for (auto originId : originList)
        {
            olist->add_origin_ids(originId.getRawValue());
        }
    }

    for (auto outId : getOutputOriginIds())
    {
        proto.add_output_origin_ids(outId.getRawValue());
    }

    auto* outSch = proto.mutable_output_schema();
    SchemaSerializationUtil::serializeSchema(outputSchema, outSch);

    SerializableOperator serializableOperator;
    serializableOperator.set_operator_id(id.getRawValue());
    for (auto& child : getChildren())
    {
        serializableOperator.add_children_ids(child.getId().getRawValue());
    }

    serializableOperator.mutable_operator_()->CopyFrom(proto);
    return serializableOperator;
}

LogicalOperatorRegistryReturnType
LogicalOperatorGeneratedRegistrar::RegisterIngestionTimeWatermarkAssignerLogicalOperator(NES::LogicalOperatorRegistryArguments arguments)
{
    auto logicalOperator = IngestionTimeWatermarkAssignerLogicalOperator();
    if (auto& id = arguments.id)
    {
        logicalOperator.id = *id;
    }
    return logicalOperator.withInferredSchema(arguments.inputSchemas)
        .withInputOriginIds(arguments.inputOriginIds)
        .withOutputOriginIds(arguments.outputOriginIds);
}

}
