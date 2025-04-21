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
#include <utility>
#include <API/Schema.hpp>
#include <Functions/Expression.hpp>
#include <Functions/FunctionExpression.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Nodes/Iterators/BreadthFirstNodeIterator.hpp>
#include <Nodes/Node.hpp>
#include <Operators/LogicalOperators/LogicalOperator.hpp>
#include <Operators/LogicalOperators/Windows/Joins/LogicalJoinDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/Joins/LogicalJoinOperator.hpp>
#include <Types/TimeBasedWindowType.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
LogicalJoinOperator::LogicalJoinOperator(std::shared_ptr<Join::LogicalJoinDescriptor> joinDefinition, const OperatorId id)
    : LogicalJoinOperator(std::move(joinDefinition), id, INVALID_ORIGIN_ID)
{
}
LogicalJoinOperator::LogicalJoinOperator(
    std::shared_ptr<Join::LogicalJoinDescriptor> joinDefinition, const OperatorId id, const OriginId originId)
    : Operator(id), LogicalBinaryOperator(id), OriginIdAssignmentOperator(id, originId), joinDefinition(std::move(joinDefinition))
{
}

bool LogicalJoinOperator::isIdentical(const std::shared_ptr<Node>& rhs) const
{
    return equal(rhs) && NES::Util::as<LogicalJoinOperator>(rhs)->getId() == id;
}

std::string LogicalJoinOperator::toString() const
{
    return fmt::format(
        "Join({}-{}, windowType = {}, joinFunction = {})",
        id,
        originId,
        joinDefinition->getWindowType()->toString(),
        joinDefinition->getJoinFunction());
}

std::shared_ptr<Join::LogicalJoinDescriptor> LogicalJoinOperator::getJoinDefinition() const
{
    return joinDefinition;
}

bool LogicalJoinOperator::inferSchema()
{
    if (!LogicalBinaryOperator::inferSchema())
    {
        return false;
    }

    throw NotImplemented("I don't know yet");
}

std::shared_ptr<Operator> LogicalJoinOperator::copy()
{
    auto copy = std::make_shared<LogicalJoinOperator>(joinDefinition, id);
    copy->setLeftInputOriginIds(leftInputOriginIds);
    copy->setRightInputOriginIds(rightInputOriginIds);
    copy->setLeftInputSchema(leftInputSchema);
    copy->setRightInputSchema(rightInputSchema);
    copy->setOutputSchema(outputSchema);
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

bool LogicalJoinOperator::equal(const std::shared_ptr<Node>& rhs) const
{
    if (NES::Util::instanceOf<LogicalJoinOperator>(rhs))
    {
        const auto rhsJoin = NES::Util::as<LogicalJoinOperator>(rhs);
        return joinDefinition->getWindowType()->equal(rhsJoin->joinDefinition->getWindowType())
            && joinDefinition->getJoinFunction() == rhsJoin->joinDefinition->getJoinFunction()
            && (joinDefinition->getOutputSchema() == rhsJoin->joinDefinition->getOutputSchema())
            && (joinDefinition->getRightSourceType() == rhsJoin->joinDefinition->getRightSourceType())
            && (joinDefinition->getLeftSourceType() == rhsJoin->joinDefinition->getLeftSourceType());
    }
    return false;
}

void LogicalJoinOperator::inferStringSignature()
{
    const std::shared_ptr<Operator> operatorNode = NES::Util::as<Operator>(shared_from_this());
    NES_TRACE("Inferring String signature for {}", *operatorNode);
    PRECONDITION(children.size() == 2, "Join should have 2 children, but got: {}", children.size());
    ///Infer query signatures for child operators
    for (const auto& child : children)
    {
        const std::shared_ptr<LogicalOperator> childOperator = NES::Util::as<LogicalOperator>(child);
        childOperator->inferStringSignature();
    }

    std::stringstream signatureStream;
    signatureStream << "WINDOW-DEFINITION=" << joinDefinition->getWindowType()->toString() << ",";

    const auto rightChildSignature = NES::Util::as<LogicalOperator>(children.at(0))->getHashBasedSignature();
    const auto leftChildSignature = NES::Util::as<LogicalOperator>(children.at(1))->getHashBasedSignature();
    signatureStream << *rightChildSignature.begin()->second.begin() + ").";
    signatureStream << *leftChildSignature.begin()->second.begin();

    ///Update the signature
    const auto hashCode = hashGenerator(signatureStream.str());
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

ExpressionValue LogicalJoinOperator::getJoinFunction() const
{
    return joinDefinition->getJoinFunction();
}

}
