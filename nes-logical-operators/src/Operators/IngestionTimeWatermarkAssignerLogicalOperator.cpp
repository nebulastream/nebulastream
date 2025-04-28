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
#include <sstream>
#include <Identifiers/Identifiers.hpp>
#include <Serialization/SchemaSerializationUtil.hpp>
#include <SerializableOperator.pb.h>
#include <Operators/IngestionTimeWatermarkAssignerLogicalOperator.hpp>

#include "LogicalOperatorRegistry.hpp"

namespace NES
{

IngestionTimeWatermarkAssignerLogicalOperator::IngestionTimeWatermarkAssignerLogicalOperator() = default;

std::string_view IngestionTimeWatermarkAssignerLogicalOperator::getName() const noexcept
{
    return NAME;
}

std::string IngestionTimeWatermarkAssignerLogicalOperator::toString() const
{
    std::stringstream ss;
    ss << "INGESTIONTIMEWATERMARKASSIGNER(" << id << ")";
    return ss.str();
}

bool IngestionTimeWatermarkAssignerLogicalOperator::operator==(const LogicalOperatorConcept& rhs) const
{
    return dynamic_cast<const IngestionTimeWatermarkAssignerLogicalOperator*>(&rhs);
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

Optimizer::TraitSet IngestionTimeWatermarkAssignerLogicalOperator::getTraitSet() const
{
    return {};
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
    SerializableOperator_LogicalOperator proto;

    proto.set_operator_type(NAME);
    auto* traitSetProto = proto.mutable_trait_set();
    for (auto const& trait : getTraitSet()) {
        *traitSetProto->add_traits() = trait.serialize();
    }

    for (auto const& inputSchema : getInputSchemas()) {
        auto* schProto = proto.add_input_schemas();
        SchemaSerializationUtil::serializeSchema(inputSchema, schProto);
    }

    for (auto const& originList : getInputOriginIds()) {
        auto* olist = proto.add_input_origin_lists();
        for (auto originId : originList) {
            olist->add_origin_ids(originId.getRawValue());
        }
    }

    for (auto outId : getOutputOriginIds()) {
        proto.add_output_origin_ids(outId.getRawValue());
    }

    auto* outSch = proto.mutable_output_schema();
    SchemaSerializationUtil::serializeSchema(outputSchema, outSch);

    SerializableOperator serializableOperator;
    serializableOperator.set_operator_id(id.getRawValue());
    for (auto& child : getChildren()) {
        serializableOperator.add_children_ids(child.getId().getRawValue());
    }

    serializableOperator.mutable_operator_()->CopyFrom(proto);
    return serializableOperator;
}

LogicalOperatorRegistryReturnType
LogicalOperatorGeneratedRegistrar::RegisterIngestionTimeWatermarkAssignerLogicalOperator(NES::LogicalOperatorRegistryArguments)
{
    return IngestionTimeWatermarkAssignerLogicalOperator();
}

}
