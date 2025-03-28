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
#include <Common/DataTypes/BasicTypes.hpp>
#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <ErrorHandling.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/FunctionSerializationUtil.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Functions/LogicalFunctions/EqualsLogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Iterators/BFSIterator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Serialization/SchemaSerializationUtil.hpp>
#include <SerializableOperator.pb.h>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <WindowTypes/Types/TimeBasedWindowType.hpp>
#include <LogicalOperatorRegistry.hpp>

namespace NES
{

JoinLogicalOperator::JoinLogicalOperator(const std::shared_ptr<LogicalFunction>& joinFunction,
                                         const std::shared_ptr<Windowing::WindowType>& windowType,
                                         uint64_t numberOfInputEdgesLeft,
                                         uint64_t numberOfInputEdgesRight,
                                         JoinType joinType,

                                         const OriginId originId)
    :  Operator(), BinaryLogicalOperator(), OriginIdAssignmentOperator(originId), joinFunction(joinFunction), windowType(windowType), numberOfInputEdgesLeft(numberOfInputEdgesLeft), numberOfInputEdgesRight(numberOfInputEdgesRight), joinType(joinType)
{
}

bool JoinLogicalOperator::isIdentical(const Operator& rhs) const
{
    return *this == rhs && dynamic_cast<const JoinLogicalOperator*>(&rhs)->id == id;
}

bool JoinLogicalOperator::operator==(Operator const& rhs) const
{
    if (const auto rhsOperator = dynamic_cast<const JoinLogicalOperator*>(&rhs)) {
        return (getWindowType() == rhsOperator->getWindowType())
            && (getJoinFunction() == rhsOperator->getJoinFunction())
            && (*getOutputSchema() == *rhsOperator->outputSchema)
            && (*getRightSchema() == *rhsOperator->getRightSchema())
            && (*getLeftSchema() == *rhsOperator->getLeftSchema());
    }
    return false;
}

std::string JoinLogicalOperator::toString() const
{
    return fmt::format(
        "Join({}, windowType = {}, joinFunction = {})",
        id,
        getWindowType()->toString(),
        *getJoinFunction());
}

bool JoinLogicalOperator::inferSchema()
{
    if (!BinaryLogicalOperator::inferSchema())
    {
        return false;
    }

    ///validate that only two different type of schema were present
    if (distinctSchemas.size() != 2)
    {
        throw CannotInferSchema("Found {} distinct schemas but expected 2 distinct schemas", distinctSchemas.size());
    }

    ///reset left and right schema
    leftInputSchema->clear();
    rightInputSchema->clear();

    /// Finds the join schema that contains the joinKey and copies the fields to the input schema, if found
    auto findSchemaInDistinctSchemas = [&](FieldAccessLogicalFunction& joinKey, const std::shared_ptr<Schema>& inputSchema)
    {
        for (const auto& distinctSchema : distinctSchemas)
        {
            const auto& joinKeyName = joinKey.getFieldName();
            if (const auto attributeField = distinctSchema->getFieldByName(joinKeyName); attributeField.has_value())
            {
                /// If we have not copied the fields from the schema, copy them for the first time
                if (inputSchema->getSchemaSizeInBytes() == 0)
                {
                    inputSchema->copyFields(distinctSchema);
                }
                joinKey.inferStamp(*inputSchema);
                return true;
            }
        }
        return false;
    };

    NES_DEBUG("JoinLogicalOperator: Iterate over all LogicalFunction to check if join field is in schema.");
    /// Maintain a list of visited nodes as there are multiple root nodes
    std::unordered_set<std::shared_ptr<LogicalFunction>> visitedFunctions;
    for (auto itr : BFSRange(getJoinFunction()))
    {
        if (NES::Util::instanceOf<BinaryLogicalFunction>(itr))
        {
            auto visitingOp = NES::Util::as<BinaryLogicalFunction>(itr);
            if (not visitedFunctions.contains(visitingOp))
            {
                visitedFunctions.insert(visitingOp);
                if (!Util::instanceOf<BinaryLogicalFunction>(Util::as<BinaryLogicalFunction>(itr)->getLeftChild()))
                {
                    ///Find the schema for left and right join key
                    const auto leftJoinKey = Util::as<FieldAccessLogicalFunction>(Util::as<BinaryLogicalFunction>(itr)->getLeftChild());
                    const auto leftJoinKeyName = leftJoinKey->getFieldName();
                    const auto foundLeftKey = findSchemaInDistinctSchemas(*leftJoinKey, leftInputSchema);
                    if (!foundLeftKey)
                    {
                        throw CannotInferSchema("unable to find left join key \"{}\" in schemas", leftJoinKeyName);
                    }
                    const auto rightJoinKey = Util::as<FieldAccessLogicalFunction>(Util::as<BinaryLogicalFunction>(itr)->getRightChild());
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
    if (leftInputSchema->getSchemaSizeInBytes() == 0)
    {
        throw CannotInferSchema("left schema is empty");
    }
    if (rightInputSchema->getSchemaSizeInBytes() == 0)
    {
        throw CannotInferSchema("right schema is empty");
    }
    if (*rightInputSchema == *leftInputSchema)
    {
        throw CannotInferSchema("found both left and right input schema to be same.");
    }

    ///Infer stamp of window definition
    const auto windowType = Util::as<Windowing::TimeBasedWindowType>(getWindowType());
    windowType->inferStamp(*leftInputSchema);

    ///Reset output schema and add fields from left and right input schema
    outputSchema->clear();
    const auto& sourceNameLeft = leftInputSchema->getQualifierNameForSystemGeneratedFields();
    const auto& sourceNameRight = rightInputSchema->getQualifierNameForSystemGeneratedFields();
    const auto& newQualifierForSystemField = sourceNameLeft + sourceNameRight;

    windowStartFieldName = newQualifierForSystemField + "$start";
    windowEndFieldName = newQualifierForSystemField + "$end";
    outputSchema->addField(windowStartFieldName, BasicType::UINT64);
    outputSchema->addField(windowEndFieldName, BasicType::UINT64);

    /// create dynamic fields to store all fields from left and right sources
    for (const auto& field : *leftInputSchema)
    {
        outputSchema->addField(field->getName(), field->getDataType());
    }

    for (const auto& field : *rightInputSchema)
    {
        outputSchema->addField(field->getName(), field->getDataType());
    }

    NES_DEBUG("LeftInput schema for join={}", leftInputSchema->toString());
    NES_DEBUG("RightInput schema for join={}", rightInputSchema->toString());
    NES_DEBUG("Output schema for join={}", outputSchema->toString());
    setOutputSchema(outputSchema);
    updateSchemas(leftInputSchema, rightInputSchema);
    return true;
}

std::shared_ptr<Operator> JoinLogicalOperator::clone() const
{
    auto copy = std::make_shared<JoinLogicalOperator>(joinFunction, windowType, numberOfInputEdgesLeft, numberOfInputEdgesRight, joinType);
    copy->setLeftInputOriginIds(leftInputOriginIds);
    copy->setRightInputOriginIds(rightInputOriginIds);
    copy->setLeftInputSchema(leftInputSchema);
    copy->setRightInputSchema(rightInputSchema);
    copy->setOutputSchema(outputSchema);
    copy->setOriginId(originId);
    copy->windowEndFieldName = windowEndFieldName;
    copy->windowStartFieldName = windowStartFieldName;
    return copy;
}

std::string JoinLogicalOperator::getWindowStartFieldName() const {
    return windowStartFieldName;
}

std::string JoinLogicalOperator::getWindowEndFieldName() const {
    return windowEndFieldName;
}

std::vector<OriginId> JoinLogicalOperator::getOutputOriginIds() const
{
    return OriginIdAssignmentOperator::getOutputOriginIds();
}

void JoinLogicalOperator::setOriginId(const OriginId originId)
{
    OriginIdAssignmentOperator::setOriginId(originId);
}

std::shared_ptr<LogicalFunction> JoinLogicalOperator::getJoinFunction() const
{
    return joinFunction;
}

SerializableOperator JoinLogicalOperator::serialize() const
{
    SerializableOperator serializedOperator;

    auto* opDesc = new SerializableOperator_LogicalOperator();
    opDesc->set_operatortype(NAME);
    serializedOperator.set_operatorid(this->id.getRawValue());
    serializedOperator.add_childrenids(children[0]->id.getRawValue());

    /// TODO we need to serialize the window type as a trait

    NES::FunctionList list;
    auto* serializedFunction = list.add_functions();
    FunctionSerializationUtil::serializeFunction(this->getJoinFunction(), serializedFunction);

    Configurations::DescriptorConfig::ConfigType configVariant = list;
    SerializableVariantDescriptor variantDescriptor = Configurations::descriptorConfigTypeToProto(configVariant);
    (*opDesc->mutable_config())["joinType"] = variantDescriptor;
    (*opDesc->mutable_config())["windowStartFieldName"] = Configurations::descriptorConfigTypeToProto(windowStartFieldName);
    (*opDesc->mutable_config())["windowEndFieldName"] = Configurations::descriptorConfigTypeToProto(windowEndFieldName);

    auto* binaryOpDesc = new SerializableOperator_BinaryLogicalOperator();
    auto* leftInputSchema = new SerializableSchema();
    SchemaSerializationUtil::serializeSchema(this->getLeftInputSchema(), leftInputSchema);
    binaryOpDesc->set_allocated_leftinputschema(leftInputSchema);

    auto* rightInputSchema = new SerializableSchema();
    SchemaSerializationUtil::serializeSchema(this->getRightInputSchema(), rightInputSchema);
    binaryOpDesc->set_allocated_rightinputschema(rightInputSchema);

    for (const auto& originId : this->getRightInputOriginIds()) {
        binaryOpDesc->add_rightoriginids(originId.getRawValue());
    }
    for (const auto& originId : this->getLeftInputOriginIds()) {
        binaryOpDesc->add_leftoriginids(originId.getRawValue());
    }
    opDesc->set_allocated_binaryoperator(binaryOpDesc);
    auto* outputSchema = new SerializableSchema();
    SchemaSerializationUtil::serializeSchema(this->outputSchema, outputSchema);
    serializedOperator.set_allocated_outputschema(outputSchema);
    serializedOperator.set_allocated_operator_(opDesc);

    return serializedOperator;
}

std::unique_ptr<LogicalOperator>
LogicalOperatorGeneratedRegistrar::RegisterJoinLogicalOperator(NES::LogicalOperatorRegistryArguments)
{
    // TODO
    return nullptr;
}

}
