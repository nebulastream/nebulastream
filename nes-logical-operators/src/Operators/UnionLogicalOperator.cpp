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

#include <algorithm>
#include <string>
#include <string_view>
#include <vector>
#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/UnionLogicalOperator.hpp>
#include <Serialization/SchemaSerializationUtil.hpp>
#include <Traits/Trait.hpp>
#include <Util/PlanRenderer.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <LogicalOperatorRegistry.hpp>
#include <SerializableOperator.pb.h>

namespace NES
{

UnionLogicalOperator::UnionLogicalOperator() = default;

std::string_view UnionLogicalOperator::getName() const noexcept
{
    return NAME;
}

bool UnionLogicalOperator::operator==(const LogicalOperatorConcept& rhs) const
{
    if (const auto* const rhsOperator = dynamic_cast<const UnionLogicalOperator*>(&rhs))
    {
        return leftInputSchema == rhsOperator->leftInputSchema && rightInputSchema == rhsOperator->rightInputSchema
            && getOutputSchema() == rhsOperator->getOutputSchema() && getInputOriginIds() == rhsOperator->getInputOriginIds()
            && getOutputOriginIds() == rhsOperator->getOutputOriginIds() && getTraitSet() == rhsOperator->getTraitSet();
    }
    return false;
}

std::string UnionLogicalOperator::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("unionWith(OpId: {}, leftSchema: {}, rightSchema: {})", id, leftInputSchema, rightInputSchema);
    }
    return "unionWith";
}

LogicalOperator UnionLogicalOperator::withInferredSchema(std::vector<Schema> inputSchemas) const
{
    const auto& leftInputSchema = inputSchemas[0];
    const auto& rightInputSchema = inputSchemas[1];

    auto copy = *this;
    std::vector<Schema> distinctSchemas;

    /// Identify different type of schemas from children operators
    for (const auto& child : children)
    {
        auto childOutputSchema = child.getInputSchemas()[0];
        auto found = std::ranges::find_if(
            distinctSchemas,

            [&](const Schema& distinctSchema) { return (childOutputSchema == distinctSchema); });
        if (found == distinctSchemas.end())
        {
            distinctSchemas.push_back(childOutputSchema);
        }
    }

    copy.leftInputSchema = Schema{copy.leftInputSchema.memoryLayoutType};
    copy.rightInputSchema = Schema{copy.rightInputSchema.memoryLayoutType};
    if (distinctSchemas.size() == 1)
    {
        copy.leftInputSchema.appendFieldsFromOtherSchema(distinctSchemas[0]);
        copy.rightInputSchema.appendFieldsFromOtherSchema(distinctSchemas[0]);
    }
    else
    {
        copy.leftInputSchema.appendFieldsFromOtherSchema(distinctSchemas[0]);
        copy.rightInputSchema.appendFieldsFromOtherSchema(distinctSchemas[1]);
    }

    if (std::ranges::none_of(leftInputSchema.getFields(), [&](const auto& field) { return leftInputSchema.contains(field.name); }))
    {
        throw CannotInferSchema(
            "Found Schema mismatch for left and right schema types. Left schema {} and Right schema {}", leftInputSchema, rightInputSchema);
    }

    if (leftInputSchema.memoryLayoutType != rightInputSchema.memoryLayoutType)
    {
        throw CannotInferSchema("Left and right should have same memory layout");
    }

    ///Copy the schema of left input
    copy.outputSchema = Schema{};
    copy.outputSchema.appendFieldsFromOtherSchema(leftInputSchema);
    return copy;
}

TraitSet UnionLogicalOperator::getTraitSet() const
{
    return traitSet;
}

LogicalOperator UnionLogicalOperator::withTraitSet(TraitSet traitSet) const
{
    auto copy = *this;
    copy.traitSet = traitSet;
    return copy;
}

LogicalOperator UnionLogicalOperator::withChildren(std::vector<LogicalOperator> children) const
{
    auto copy = *this;
    copy.children = children;
    return copy;
}

std::vector<Schema> UnionLogicalOperator::getInputSchemas() const
{
    return {leftInputSchema, rightInputSchema};
};

Schema UnionLogicalOperator::getOutputSchema() const
{
    return outputSchema;
}

std::vector<std::vector<OriginId>> UnionLogicalOperator::getInputOriginIds() const
{
    return inputOriginIds;
}

std::vector<OriginId> UnionLogicalOperator::getOutputOriginIds() const
{
    return outputOriginIds;
}

LogicalOperator UnionLogicalOperator::withInputOriginIds(std::vector<std::vector<OriginId>> ids) const
{
    PRECONDITION(ids.size() == 2, "Union should have two inputs");
    auto copy = *this;
    copy.inputOriginIds = ids;
    return copy;
}

LogicalOperator UnionLogicalOperator::withOutputOriginIds(std::vector<OriginId> ids) const
{
    auto copy = *this;
    copy.outputOriginIds = ids;
    return copy;
}

std::vector<LogicalOperator> UnionLogicalOperator::getChildren() const
{
    return children;
}

SerializableOperator UnionLogicalOperator::serialize() const
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

LogicalOperator UnionLogicalOperator::setInputSchemas(std::vector<Schema> inputSchemas) const
{
    auto copy = *this;
    INVARIANT(inputSchemas.size() == 2, "Expected 2 input schemas.");
    copy.leftInputSchema.appendFieldsFromOtherSchema(inputSchemas[0]);
    copy.rightInputSchema.appendFieldsFromOtherSchema(inputSchemas[1]);
    return copy;
}

LogicalOperator UnionLogicalOperator::setOutputSchema(const Schema& outputSchema) const
{
    auto copy = *this;
    copy.outputSchema.appendFieldsFromOtherSchema(outputSchema);
    return copy;
}

LogicalOperator LogicalOperatorGeneratedRegistrar::RegisterUnionLogicalOperator(NES::LogicalOperatorRegistryArguments arguments)
{
    auto logicalOperator = UnionLogicalOperator();
    if (auto& id = arguments.id)
    {
        logicalOperator.id = *id;
    }
    auto logicalOp = logicalOperator.withInputOriginIds(arguments.inputOriginIds)
                         .withOutputOriginIds(arguments.outputOriginIds)
                         .get<UnionLogicalOperator>()
                         .setInputSchemas(arguments.inputSchemas)
                         .get<UnionLogicalOperator>()
                         .setOutputSchema(arguments.outputSchema);
    return logicalOp;
}

}
