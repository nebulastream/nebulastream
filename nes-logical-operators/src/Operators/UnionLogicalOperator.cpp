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
#include <API/Schema.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/UnionLogicalOperator.hpp>
#include <Serialization/SchemaSerializationUtil.hpp>
#include <Util/Common.hpp>
#include <magic_enum/magic_enum.hpp>
#include <ErrorHandling.hpp>
#include <LogicalOperatorRegistry.hpp>
#include <SerializableOperator.pb.h>
#include <SerializableSchema.pb.h>

namespace NES
{

UnionLogicalOperator::UnionLogicalOperator() = default;

std::string_view UnionLogicalOperator::getName() const noexcept
{
    return NAME;
}

bool UnionLogicalOperator::operator==(const LogicalOperatorConcept& rhs) const
{
    if (const auto rhsOperator = dynamic_cast<const UnionLogicalOperator*>(&rhs))
    {
        return (leftInputSchema == rhsOperator->leftInputSchema) && (rightInputSchema == rhsOperator->rightInputSchema);
    }
    return false;
}

std::string UnionLogicalOperator::toString() const
{
    std::stringstream ss;
    ss << "unionWith(" << id << ")";
    return ss.str();
}

LogicalOperator UnionLogicalOperator::withInferredSchema(Schema) const
{
    auto copy = *this;
    std::vector<Schema> distinctSchemas;

    /// Identify different type of schemas from children operators
    for (const auto& child : children)
    {
        auto childOutputSchema = child.getInputSchemas()[0];
        auto found = std::find_if(
            distinctSchemas.begin(),
            distinctSchemas.end(),
            [&](const Schema& distinctSchema) { return (childOutputSchema == distinctSchema); });
        if (found == distinctSchemas.end())
        {
            distinctSchemas.push_back(childOutputSchema);
        }
    }

    ///validate that only two different type of schema were present
    INVARIANT(distinctSchemas.size() == 2, "BinaryOperator: this node should have exactly two distinct schemas");

    copy.leftInputSchema.clear();
    copy.rightInputSchema.clear();
    if (distinctSchemas.size() == 1)
    {
        copy.leftInputSchema.copyFields(distinctSchemas[0]);
        copy.rightInputSchema.copyFields(distinctSchemas[0]);
    }
    else
    {
        copy.leftInputSchema.copyFields(distinctSchemas[0]);
        copy.rightInputSchema.copyFields(distinctSchemas[1]);
    }

    if (!(leftInputSchema == rightInputSchema))
    {
        throw CannotInferSchema(
            "Found Schema mismatch for left and right schema types. Left schema {} and Right schema {}",
            leftInputSchema.toString(),
            rightInputSchema.toString());
    }

    if (leftInputSchema.getLayoutType() != rightInputSchema.getLayoutType())
    {
        throw CannotInferSchema("Left and right should have same memory layout");
    }

    ///Copy the schema of left input
    copy.outputSchema.clear();
    copy.outputSchema.copyFields(leftInputSchema);
    copy.outputSchema.setLayoutType(leftInputSchema.getLayoutType());

    std::vector<LogicalOperator> newChildren;
    for (auto& child : children)
    {
        newChildren.push_back(child.withInferredSchema(copy.outputSchema));
    }
    copy.children = newChildren;
    return copy;
}

Optimizer::TraitSet UnionLogicalOperator::getTraitSet() const
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

void UnionLogicalOperator::setOutputOriginIds(std::vector<OriginId> ids)
{
    outputOriginIds = ids;
}

void UnionLogicalOperator::setInputOriginIds(std::vector<std::vector<OriginId>> ids)
{
    inputOriginIds = ids;
}

std::vector<LogicalOperator> UnionLogicalOperator::getChildren() const
{
    return children;
}

NES::Configurations::DescriptorConfig::Config UnionLogicalOperator::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return NES::Configurations::DescriptorConfig::validateAndFormat<UnionLogicalOperator::ConfigParameters>(std::move(config), NAME);
}

LogicalOperator LogicalOperatorGeneratedRegistrar::RegisterUnionLogicalOperator(NES::LogicalOperatorRegistryArguments)
{
    /// nothing to da as this operator has no members
    return UnionLogicalOperator();
}

SerializableOperator UnionLogicalOperator::serialize() const
{
    SerializableOperator serializedOperator;

    auto* opDesc = new SerializableOperator_LogicalOperator();
    opDesc->set_operatortype(NAME);
    serializedOperator.set_operatorid(this->id.getRawValue());
    serializedOperator.add_childrenids(this->getChildren()[0].getId().getRawValue());

    auto* binaryOpDesc = new SerializableOperator_BinaryLogicalOperator();
    auto* rightInputSchema = new SerializableSchema();
    SchemaSerializationUtil::serializeSchema(this->getInputSchemas()[0], rightInputSchema);
    binaryOpDesc->set_allocated_rightinputschema(rightInputSchema);

    auto* leftInputSchema = new SerializableSchema();
    SchemaSerializationUtil::serializeSchema(this->getInputSchemas()[1], leftInputSchema);
    binaryOpDesc->set_allocated_leftinputschema(leftInputSchema);

    for (const auto& originId : getInputOriginIds()[0])
    {
        binaryOpDesc->add_leftoriginids(originId.getRawValue());
    }
    for (const auto& originId : getInputOriginIds()[1])
    {
        binaryOpDesc->add_rightoriginids(originId.getRawValue());
    }

    opDesc->set_allocated_binaryoperator(binaryOpDesc);

    auto* outputSchema = new SerializableSchema();
    SchemaSerializationUtil::serializeSchema(this->getOutputSchema(), outputSchema);
    serializedOperator.set_allocated_outputschema(outputSchema);
    serializedOperator.set_allocated_operator_(opDesc);

    return serializedOperator;
}

}
