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

#include <Operators/UnionLogicalOperator.hpp>

#include <algorithm>
#include <string>
#include <string_view>
#include <vector>
#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
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

UnionLogicalOperator::UnionLogicalOperator(bool keepSourceQualifiers) : keepSourceQualifiers(keepSourceQualifiers) {}

std::string_view UnionLogicalOperator::getName() const noexcept
{
    return NAME;
}

bool UnionLogicalOperator::operator==(const LogicalOperatorConcept& rhs) const
{
    if (const auto* const rhsOperator = dynamic_cast<const UnionLogicalOperator*>(&rhs))
    {
        if (inputSchemas.size() != rhsOperator->inputSchemas.size())
        {
            return false;
        }
        for (size_t i = 0; i < inputSchemas.size(); ++i)
        {
            if (inputSchemas[i] != rhsOperator->inputSchemas[i])
            {
                return false;
            }
        }
        return getOutputSchema() == rhsOperator->getOutputSchema() && getInputOriginIds() == rhsOperator->getInputOriginIds()
            && getOutputOriginIds() == rhsOperator->getOutputOriginIds();
    }
    return false;
}

std::string UnionLogicalOperator::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        if (inputSchemas.size() == 1 && inputSchemas[0].getNumberOfFields() != 0)
        {
            return fmt::format("UnionWith(OpId: {}, schema: {})", id, inputSchemas[0]);
        }
        if (inputSchemas.size() == 2 && inputSchemas[0].getNumberOfFields() != 0)
        {
            return fmt::format("UnionWith(OpId: {}, leftSchema: {}, rightSchema: {})", id, inputSchemas[0], inputSchemas[1]);
        }

        return fmt::format("UnionWith(OpId: {})", id);
    }
    return "UnionWith";
}

LogicalOperator UnionLogicalOperator::withInferredSchema(std::vector<Schema> inputSchemas) const
{
    if (!inputSchemas.empty()) //prevent unsigned int underflow when inputSchemas.size() == 0
    {
        for (size_t i = 0; i < inputSchemas.size()-1; ++i)
        {
            auto& schema1 = inputSchemas[i];
            auto& schema2 = inputSchemas[i+1];
            if (withoutSourceQualifier(schema1) != withoutSourceQualifier(schema2))
            {
                throw CannotInferSchema(
                    "Missmatch between input schemas.\nSchema1: {}\nSchema2:{}",
                    withoutSourceQualifier(schema1),
                    withoutSourceQualifier(schema2));
            }
            if (schema1.memoryLayoutType != schema2.memoryLayoutType)
            {
                throw CannotInferSchema("Input schemas should have same memory layout");
            }
            if (keepSourceQualifiers && (schema1.getSourceNameQualifier() != schema2.getSourceNameQualifier()))
            {
                throw CannotInferSchema("Source qualifiers can only be kept if they are all the same");
            }

        }
    }


    auto copy = *this;
    copy.inputSchemas = std::move(inputSchemas);
    if (keepSourceQualifiers)
    {
        copy.outputSchema = copy.inputSchemas[0];
    } else
    {
        copy.outputSchema = withoutSourceQualifier(copy.inputSchemas[0]);
    }
    return copy;
}

TraitSet UnionLogicalOperator::getTraitSet() const
{
    return {};
}

LogicalOperator UnionLogicalOperator::withChildren(std::vector<LogicalOperator> children) const
{
    auto copy = *this;
    copy.children = children;
    return copy;
}

std::vector<Schema> UnionLogicalOperator::getInputSchemas() const
{
    return inputSchemas;
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
    copy.inputSchemas = std::move(inputSchemas);
    return copy;
}

LogicalOperator UnionLogicalOperator::setOutputSchema(const Schema& outputSchema) const
{
    auto copy = *this;
    copy.outputSchema = outputSchema;
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
