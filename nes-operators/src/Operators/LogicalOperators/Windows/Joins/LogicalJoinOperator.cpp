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

#include <unordered_set>
#include <utility>
#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Functions/LogicalFunctions/NodeFunctionEquals.hpp>
#include <Functions/NodeFunctionBinary.hpp>
#include <Functions/NodeFunctionFieldAccess.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Nodes/Iterators/BreadthFirstNodeIterator.hpp>
#include <Operators/LogicalOperators/Windows/Joins/LogicalJoinDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/Joins/LogicalJoinOperator.hpp>
#include <Types/TimeBasedWindowType.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <Common/DataTypes/BasicTypes.hpp>

namespace NES
{

LogicalJoinOperator::LogicalJoinOperator(Join::LogicalJoinDescriptorPtr joinDefinition, const OperatorId id, const OriginId originId)
    : Operator(id), LogicalBinaryOperator(id), OriginIdAssignmentOperator(id, originId), joinDefinition(std::move(joinDefinition))
{
}

bool LogicalJoinOperator::isIdentical(NodePtr const& rhs) const
{
    return equal(rhs) && NES::Util::as<LogicalJoinOperator>(rhs)->getId() == id;
}

std::string LogicalJoinOperator::toString() const
{
    return fmt::format(
        "Join({}, windowType = {}, joinFunction = {})",
        id,
        joinDefinition->getWindowType()->toString(),
        *joinDefinition->getJoinFunction());
}

Join::LogicalJoinDescriptorPtr LogicalJoinOperator::getJoinDefinition() const
{
    return joinDefinition;
}

bool LogicalJoinOperator::inferSchema()
{
    if (!LogicalBinaryOperator::inferSchema())
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
    auto findSchemaInDistinctSchemas = [&](NodeFunctionFieldAccess& joinKey, const SchemaPtr& inputSchema)
    {
        for (auto& distinctSchema : distinctSchemas)
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

    NES_DEBUG("LogicalJoinOperator: Iterate over all NodeFunction to check if join field is in schema.");
    /// Maintain a list of visited nodes as there are multiple root nodes
    std::unordered_set<std::shared_ptr<NodeFunctionBinary>> visitedFunctions;
    auto bfsIterator = BreadthFirstNodeIterator(joinDefinition->getJoinFunction());
    for (auto itr = bfsIterator.begin(); itr != BreadthFirstNodeIterator::end(); ++itr)
    {
        if (NES::Util::instanceOf<NodeFunctionBinary>(*itr))
        {
            auto visitingOp = NES::Util::as<NodeFunctionBinary>(*itr);
            if (not visitedFunctions.contains(visitingOp))
            {
                visitedFunctions.insert(visitingOp);
                if (!Util::instanceOf<NodeFunctionBinary>(Util::as<NodeFunctionBinary>(*itr)->getLeft()))
                {
                    ///Find the schema for left and right join key
                    const auto leftJoinKey = Util::as<NodeFunctionFieldAccess>(Util::as<NodeFunctionBinary>(*itr)->getLeft());
                    const auto leftJoinKeyName = leftJoinKey->getFieldName();
                    const auto foundLeftKey = findSchemaInDistinctSchemas(*leftJoinKey, leftInputSchema);
                    if (!foundLeftKey)
                    {
                        throw CannotInferSchema("unable to find left join key \"{}\" in schemas", leftJoinKeyName);
                    }
                    const auto rightJoinKey = Util::as<NodeFunctionFieldAccess>(Util::as<NodeFunctionBinary>(*itr)->getRight());
                    const auto rightJoinKeyName = rightJoinKey->getFieldName();
                    const auto foundRightKey = findSchemaInDistinctSchemas(*rightJoinKey, rightInputSchema);
                    if (!foundRightKey)
                    {
                        throw CannotInferSchema("unable to find right join key \"{}\" in schemas", rightJoinKeyName);
                    }
                    NES_DEBUG("LogicalJoinOperator: Inserting operator in collection of already visited node.");
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
    const auto windowType = Util::as<Windowing::TimeBasedWindowType>(joinDefinition->getWindowType());
    windowType->inferStamp(*leftInputSchema);

    ///Reset output schema and add fields from left and right input schema
    outputSchema->clear();
    const auto& sourceNameLeft = leftInputSchema->getQualifierNameForSystemGeneratedFields();
    const auto& sourceNameRight = rightInputSchema->getQualifierNameForSystemGeneratedFields();
    const auto& newQualifierForSystemField = sourceNameLeft + sourceNameRight;

    windowMetaData.windowStartFieldName = newQualifierForSystemField + "$start";
    windowMetaData.windowEndFieldName = newQualifierForSystemField + "$end";
    outputSchema->addField(windowMetaData.windowStartFieldName, BasicType::UINT64);
    outputSchema->addField(windowMetaData.windowEndFieldName, BasicType::UINT64);

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
    joinDefinition->updateOutputDefinition(outputSchema);
    joinDefinition->updateSourceTypes(leftInputSchema, rightInputSchema);
    return true;
}

std::shared_ptr<Operator> LogicalJoinOperator::copy()
{
    auto copy = std::make_shared<LogicalJoinOperator>(joinDefinition, id);
    copy->setLeftInputOriginIds(leftInputOriginIds);
    copy->setRightInputOriginIds(rightInputOriginIds);
    copy->setLeftInputSchema(leftInputSchema);
    copy->setRightInputSchema(rightInputSchema);
    copy->setOutputSchema(outputSchema);
    copy->setZ3Signature(z3Signature);
    copy->setHashBasedSignature(hashBasedSignature);
    copy->setOriginId(originId);
    copy->windowMetaData = windowMetaData;
    copy->setOperatorState(operatorState);
    for (const auto& [key, value] : properties)
    {
        copy->addProperty(key, value);
    }
    return copy;
}

bool LogicalJoinOperator::equal(NodePtr const& rhs) const
{
    if (NES::Util::instanceOf<LogicalJoinOperator>(rhs))
    {
        const auto rhsJoin = NES::Util::as<LogicalJoinOperator>(rhs);
        return joinDefinition->getWindowType()->equal(rhsJoin->joinDefinition->getWindowType())
            && joinDefinition->getJoinFunction()->equal(rhsJoin->joinDefinition->getJoinFunction())
            && (*joinDefinition->getOutputSchema() == *rhsJoin->joinDefinition->getOutputSchema())
            && (*joinDefinition->getRightSourceType() == *rhsJoin->joinDefinition->getRightSourceType())
            && (*joinDefinition->getLeftSourceType() == *rhsJoin->joinDefinition->getLeftSourceType());
    }
    return false;
}

void LogicalJoinOperator::inferStringSignature()
{
    std::shared_ptr<Operator> operatorNode = NES::Util::as<Operator>(shared_from_this());
    NES_TRACE("Inferring String signature for {}", *operatorNode);
    PRECONDITION(children.size() == 2, "Join should have 2 children, but got: {}", children.size());
    ///Infer query signatures for child operators
    for (const auto& child : children)
    {
        const LogicalOperatorPtr childOperator = NES::Util::as<LogicalOperator>(child);
        childOperator->inferStringSignature();
    }

    std::stringstream signatureStream;
    signatureStream << "WINDOW-DEFINITION=" << joinDefinition->getWindowType()->toString() << ",";

    auto rightChildSignature = NES::Util::as<LogicalOperator>(children[0])->getHashBasedSignature();
    auto leftChildSignature = NES::Util::as<LogicalOperator>(children[1])->getHashBasedSignature();
    signatureStream << *rightChildSignature.begin()->second.begin() + ").";
    signatureStream << *leftChildSignature.begin()->second.begin();

    ///Update the signature
    auto hashCode = hashGenerator(signatureStream.str());
    hashBasedSignature[hashCode] = {signatureStream.str()};
}

std::vector<OriginId> LogicalJoinOperator::getOutputOriginIds() const
{
    return OriginIdAssignmentOperator::getOutputOriginIds();
}

void LogicalJoinOperator::setOriginId(const OriginId originId)
{
    OriginIdAssignmentOperator::setOriginId(originId);
    joinDefinition->setOriginId(originId);
}

const NodeFunctionPtr LogicalJoinOperator::getJoinFunction() const
{
    return joinDefinition->getJoinFunction();
}

}
