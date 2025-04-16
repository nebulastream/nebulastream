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
    auto result = fmt::format("Join({}, windowType = {}, joinFunction = {})", id, getWindowType()->toString(), getJoinFunction());

    if (!inputOriginIds.empty() || !outputOriginIds.empty())
    {
        result.append(", originIds = {");

        if (inputOriginIds.size() == 2)
        {
            result.append("left: [");
            bool first = true;
            for (const auto& oid : inputOriginIds[1])
            {
                if (!first)
                {
                    result.append(", ");
                }
                result.append(oid.toString());
                first = false;
            }
            result.append("], ");

            result.append("right: [");
            first = true;
            for (const auto& oid : inputOriginIds[0])
            {
                if (!first)
                {
                    result.append(", ");
                }
                result.append(oid.toString());
                first = false;
            }
            result.append("]");
        }

        if (!outputOriginIds.empty())
        {
            if (inputOriginIds.size() == 2)
            {
                result.append(", ");
            }
            result.append("output: [");
            bool first = true;
            for (const auto& oid : outputOriginIds)
            {
                if (!first)
                {
                    result.append(", ");
                }
                result.append(oid.toString());
                first = false;
            }
            result.append("]");
        }
        result.append("}");
    }

    result.append(")");
    return result;
}

LogicalOperator JoinLogicalOperator::withInferredSchema(std::vector<Schema> inputSchemas) const
{
    PRECONDITION(inputSchemas.size() == 2, "Join should have two inputs");
    const auto& leftInputSchema = inputSchemas[0];
    const auto& rightInputSchema = inputSchemas[1];

    auto copy = *this;
    copy.outputSchema.clear();
    copy.leftInputSchema = leftInputSchema;
    copy.rightInputSchema = rightInputSchema;

    auto sourceNameLeft = leftInputSchema.getQualifierNameForSystemGeneratedFields();
    auto sourceNameRight = rightInputSchema.getQualifierNameForSystemGeneratedFields();
    auto newQualifierForSystemField = sourceNameLeft + sourceNameRight;

    copy.windowStartFieldName = newQualifierForSystemField + "$start";
    copy.windowEndFieldName = newQualifierForSystemField + "$end";
    copy.outputSchema.addField(copy.windowStartFieldName, BasicType::UINT64);
    copy.outputSchema.addField(copy.windowEndFieldName, BasicType::UINT64);

    for (auto field : leftInputSchema)
    {
        copy.outputSchema.addField(field.getName(), field.getDataType());
    }
    for (auto field : rightInputSchema)
    {
        copy.outputSchema.addField(field.getName(), field.getDataType());
    }

    auto inputSchema = leftInputSchema;
    auto combinedSchema = inputSchema.copyFields(rightInputSchema);
    copy.joinFunction = joinFunction.withInferredStamp(combinedSchema);
    copy.windowType->inferStamp(combinedSchema);
    return copy;
}

Optimizer::TraitSet JoinLogicalOperator::getTraitSet() const
{
    return {originIdTrait};
}

LogicalOperator JoinLogicalOperator::withChildren(std::vector<LogicalOperator> children) const
{
    auto copy = *this;
    copy.children = children;
    return copy;
}

std::vector<Schema> JoinLogicalOperator::getInputSchemas() const
{
    return {leftInputSchema, rightInputSchema};
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

LogicalOperator JoinLogicalOperator::withInputOriginIds(std::vector<std::vector<OriginId>> ids) const
{
    PRECONDITION(ids.size() == 2, "Join should have only two inputs");
    auto copy = *this;
    copy.inputOriginIds = ids;
    return copy;
}

LogicalOperator JoinLogicalOperator::withOutputOriginIds(std::vector<OriginId> ids) const
{
    auto copy = *this;
    copy.outputOriginIds = ids;
    return copy;
}

std::vector<LogicalOperator> JoinLogicalOperator::getChildren() const
{
    return children;
}

Schema JoinLogicalOperator::getLeftSchema() const
{
    return leftInputSchema;
}

Schema JoinLogicalOperator::getRightSchema() const
{
    return rightInputSchema;
}

std::shared_ptr<Windowing::WindowType> JoinLogicalOperator::getWindowType() const
{
    return windowType;
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
