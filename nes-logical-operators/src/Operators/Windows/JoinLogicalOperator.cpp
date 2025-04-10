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
#include <unordered_set>
#include <Operators/Windows/JoinLogicalOperator.hpp>
#include <API/Schema.hpp>
#include <ErrorHandling.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Serialization/SchemaSerializationUtil.hpp>
#include <SerializableOperator.pb.h>
#include <LogicalOperatorRegistry.hpp>

namespace NES
{

JoinLogicalOperator::JoinLogicalOperator(LogicalFunction joinFunction,
                                         std::shared_ptr<Windowing::WindowType> windowType,
                                         uint64_t numberOfInputEdgesLeft,
                                         uint64_t numberOfInputEdgesRight,
                                         JoinType joinType)
    :  joinFunction(joinFunction), windowType(std::move(windowType)), numberOfInputEdgesLeft(numberOfInputEdgesLeft), numberOfInputEdgesRight(numberOfInputEdgesRight), joinType(joinType)
{
}

std::string_view JoinLogicalOperator::getName() const noexcept
{
    return NAME;
}

bool JoinLogicalOperator::operator==(LogicalOperatorConcept const& rhs) const
{
    if (const auto rhsOperator = dynamic_cast<const JoinLogicalOperator*>(&rhs)) {
        return (getWindowType() == rhsOperator->getWindowType())
            && (getJoinFunction() == rhsOperator->getJoinFunction())
            && (getOutputSchema() == rhsOperator->outputSchema)
            && (getRightSchema() == rhsOperator->getRightSchema())
            && (getLeftSchema() == rhsOperator->getLeftSchema());
    }
    return false;
}

std::string JoinLogicalOperator::toString() const
{
    return fmt::format(
        "Join({}, windowType = {}, joinFunction = {})",
        id,
        getWindowType().toString(),
        getJoinFunction());
}


LogicalOperator JoinLogicalOperator::withInferredSchema(Schema) const
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

    ///validate that only two different type of schema were present
    if (distinctSchemas.size() != 2)
    {
        throw CannotInferSchema("Found {} distinct schemas but expected 2 distinct schemas", distinctSchemas.size());
    }

    ///reset left and right schema
    copy.leftSourceSchema.clear();
    copy.rightSourceSchema.clear();

    std::vector<LogicalOperator> newChildren;
    for (auto& child : children)
    {
        newChildren.push_back(child.withInferredSchema(copy.outputSchema));
    }
    return copy.withChildren(newChildren);
/*
    NES_DEBUG("JoinLogicalOperator: Iterate over all LogicalFunction to check if join field is in schema.");
    /// Maintain a list of visited nodes as there are multiple root nodes
    std::unordered_set<LogicalFunction*> visitedFunctions;
    for (auto itr : BFSRange(&getJoinFunction()))
    {
        if (auto visitingOp = dynamic_cast<BinaryLogicalFunction*>(itr); visitingOp)
        {
            if (not visitedFunctions.contains(visitingOp))
            {
                visitedFunctions.insert(visitingOp);
                if (!dynamic_cast<BinaryLogicalFunction*>(&visitingOp->getLeftChild()))
                {
                    ///Find the schema for left and right join key
                    auto leftJoinKey = dynamic_cast<FieldAccessLogicalFunction*>(&dynamic_cast<BinaryLogicalFunction*>(itr)->getLeftChild());
                    auto leftJoinKeyName = leftJoinKey->getFieldName();
                    const auto foundLeftKey = findSchemaInDistinctSchemas(*leftJoinKey, leftInputSchema);
                    if (!foundLeftKey)
                    {
                        throw CannotInferSchema("unable to find left join key \"{}\" in schemas", leftJoinKeyName);
                    }
                    const auto rightJoinKey = dynamic_cast<FieldAccessLogicalFunction*>(&dynamic_cast<BinaryLogicalFunction*>(itr)->getRightChild());
                    const auto rightJoinKeyName = rightJoinKey->getFieldName();
                    const auto foundRightKey = findSchemaInDistinctSchemas(*rightJoinKey, rightInputSchema);
                    if (!foundRightKey)
                    {
                        throw CannotInferSchema("unable to find right join key \"{}\" in schemas", rightJoinKeyName);
                    }
                    NES_DEBUG("JoinLogicalOperator: Inserting operator in collection of already visited node.");
                    visitedFunctions.insert(visitingOp);
                }
            }
        }
    }
    /// Clearing now the distinct schemas
    distinctSchemas.clear();

    /// Checking if left and right input schema are not empty and are not equal
    if (leftInputSchema.getSchemaSizeInBytes() == 0)
    {
        throw CannotInferSchema("left schema is empty");
    }
    if (rightInputSchema.getSchemaSizeInBytes() == 0)
    {
        throw CannotInferSchema("right schema is empty");
    }
    if (rightInputSchema == leftInputSchema)
    {
        throw CannotInferSchema("found both left and right input schema to be same.");
    }

    ///Infer stamp of window definition
    getWindowType().withInferredStamp(leftInputSchema);

    ///Reset output schema and add fields from left and right input schema
    outputSchema.clear();
    const auto& sourceNameLeft = leftInputSchema.getQualifierNameForSystemGeneratedFields();
    const auto& sourceNameRight = rightInputSchema.getQualifierNameForSystemGeneratedFields();
    const auto& newQualifierForSystemField = sourceNameLeft + sourceNameRight;

    windowStartFieldName = newQualifierForSystemField + "$start";
    windowEndFieldName = newQualifierForSystemField + "$end";
    outputSchema.addField(windowStartFieldName, BasicType::UINT64);
    outputSchema.addField(windowEndFieldName, BasicType::UINT64);

    /// create dynamic fields to store all fields from left and right sources
    for (const auto& field : leftInputSchema)
    {
        outputSchema.addField(field.getName(), field.getDataType().clone());
    }

    for (const auto& field : rightInputSchema)
    {
        outputSchema.addField(field.getName(), field.getDataType().clone());
    }

    NES_DEBUG("LeftInput schema for join={}", leftInputSchema.toString());
    NES_DEBUG("RightInput schema for join={}", rightInputSchema.toString());
    NES_DEBUG("Output schema for join={}", outputSchema.toString());
    setOutputSchema(outputSchema);
    updateSchemas(leftInputSchema, rightInputSchema);
    return true;
    */
}

Optimizer::TraitSet JoinLogicalOperator::getTraitSet() const
{
    return {};
}

LogicalOperator JoinLogicalOperator::withChildren(std::vector<LogicalOperator> children) const
{
    auto copy = *this;
    copy.children = children;
    return copy;
}

std::vector<Schema> JoinLogicalOperator::getInputSchemas() const
{
    return {leftSourceSchema, rightSourceSchema};
};

Schema JoinLogicalOperator::getOutputSchema() const
{
    return outputSchema;
}

std::vector<std::vector<OriginId>> JoinLogicalOperator::getInputOriginIds() const
{
    return inputOriginIds;
}

std::vector<OriginId> JoinLogicalOperator::getOutputOriginIds() const
{
    return outputOriginIds;
}

void JoinLogicalOperator::setOutputOriginIds(std::vector<OriginId> ids)
{
    outputOriginIds = ids;
}

void JoinLogicalOperator::setInputOriginIds(std::vector<std::vector<OriginId>> ids)
{
    inputOriginIds = ids;
}

std::vector<LogicalOperator> JoinLogicalOperator::getChildren() const
{
    return children;
}

Schema JoinLogicalOperator::getLeftSchema() const
{
    return leftSourceSchema;
}

Schema JoinLogicalOperator::getRightSchema() const
{
    return rightSourceSchema;
}

Windowing::WindowType& JoinLogicalOperator::getWindowType() const
{
    return *windowType;
}

std::string JoinLogicalOperator::getWindowStartFieldName() const {
    return windowStartFieldName;
}

std::string JoinLogicalOperator::getWindowEndFieldName() const {
    return windowEndFieldName;
}

LogicalFunction JoinLogicalOperator::getJoinFunction() const
{
    return joinFunction;
}

SerializableOperator JoinLogicalOperator::serialize() const
{
    SerializableOperator serializedOperator;

    auto* opDesc = new SerializableOperator_LogicalOperator();
    opDesc->set_operatortype(NAME);
    serializedOperator.set_operatorid(this->id.getRawValue());
    serializedOperator.add_childrenids(getChildren()[0].getId().getRawValue());

    /// TODO we need to serialize the window type as a trait

    NES::FunctionList list;
    auto* serializedFunction = list.add_functions();
    serializedFunction->CopyFrom(getJoinFunction().serialize());

    NES::Configurations::DescriptorConfig::ConfigType configVariant = list;
    SerializableVariantDescriptor variantDescriptor = Configurations::descriptorConfigTypeToProto(configVariant);
    (*opDesc->mutable_config())["joinType"] = variantDescriptor;
    (*opDesc->mutable_config())["windowStartFieldName"] = Configurations::descriptorConfigTypeToProto(windowStartFieldName);
    (*opDesc->mutable_config())["windowEndFieldName"] = Configurations::descriptorConfigTypeToProto(windowEndFieldName);

    auto* binaryOpDesc = new SerializableOperator_BinaryLogicalOperator();
    auto* leftInputSchema = new SerializableSchema();
    auto inputSchemas = this->getInputSchemas();
    SchemaSerializationUtil::serializeSchema(inputSchemas[0], leftInputSchema);
    binaryOpDesc->set_allocated_leftinputschema(leftInputSchema);

    auto* rightInputSchema = new SerializableSchema();
    SchemaSerializationUtil::serializeSchema(inputSchemas[1], rightInputSchema);
    binaryOpDesc->set_allocated_rightinputschema(rightInputSchema);

    for (const auto& originId : this->getInputOriginIds()[0]) {
        binaryOpDesc->add_rightoriginids(originId.getRawValue());
    }
    for (const auto& originId : this->getInputOriginIds()[1]) {
        binaryOpDesc->add_leftoriginids(originId.getRawValue());
    }
    opDesc->set_allocated_binaryoperator(binaryOpDesc);
    auto* outputSchema = new SerializableSchema();
    SchemaSerializationUtil::serializeSchema(this->outputSchema, outputSchema);
    serializedOperator.set_allocated_outputschema(outputSchema);
    serializedOperator.set_allocated_operator_(opDesc);

    return serializedOperator;
}

LogicalOperatorRegistryReturnType
LogicalOperatorGeneratedRegistrar::RegisterJoinLogicalOperator(NES::LogicalOperatorRegistryArguments)
{
    // TODO
    throw UnknownLogicalOperator();
}

}
