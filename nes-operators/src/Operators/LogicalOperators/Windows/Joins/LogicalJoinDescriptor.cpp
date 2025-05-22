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

#include <cstdint>
#include <memory>
#include <unordered_set>
#include <utility>
#include <API/Schema.hpp>
#include <Functions/LogicalFunctions/NodeFunctionEquals.hpp>
#include <Functions/NodeFunction.hpp>
#include <Functions/NodeFunctionBinary.hpp>
#include <Functions/NodeFunctionFieldAccess.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Nodes/Iterators/BreadthFirstNodeIterator.hpp>
#include <Operators/LogicalOperators/Windows/Joins/LogicalJoinDescriptor.hpp>
#include <Types/WindowType.hpp>
#include <Util/Logger/Logger.hpp>
#include <magic_enum/magic_enum.hpp>
#include <ErrorHandling.hpp>

namespace NES::Join
{

LogicalJoinDescriptor::LogicalJoinDescriptor(
    std::shared_ptr<NodeFunction> joinFunction,
    std::shared_ptr<Windowing::WindowType> windowType,
    const uint64_t numberOfInputEdgesLeft,
    const uint64_t numberOfInputEdgesRight,
    const JoinType joinType,
    const OriginId originId)
    : joinFunction(std::move(joinFunction))
    , leftSourceType(Schema::create())
    , rightSourceType(Schema::create())
    , outputSchema(Schema::create())
    , windowType(std::move(windowType))
    , numberOfInputEdgesLeft(numberOfInputEdgesLeft)
    , numberOfInputEdgesRight(numberOfInputEdgesRight)
    , joinType(joinType)
    , originId(originId)
{
    PRECONDITION(this->windowType, "Invalid window type");
    PRECONDITION(this->numberOfInputEdgesLeft > 0, "Number of left edges was 0, which is invalid.");
    PRECONDITION(this->numberOfInputEdgesRight > 0, "Number of right edges was 0, which is invalid.");
    PRECONDITION(
        (this->joinType == JoinType::INNER_JOIN || this->joinType == JoinType::CARTESIAN_PRODUCT),
        "Invalid join type expected Inner join or cartesian product, but got: {}",
        magic_enum::enum_name(this->joinType));
}

std::shared_ptr<LogicalJoinDescriptor> LogicalJoinDescriptor::create(
    const std::shared_ptr<NodeFunction>& joinFunctions,
    const std::shared_ptr<Windowing::WindowType>& windowType,
    uint64_t numberOfInputEdgesLeft,
    uint64_t numberOfInputEdgesRight,
    JoinType joinType)
{
    return std::make_shared<Join::LogicalJoinDescriptor>(
        joinFunctions, windowType, numberOfInputEdgesLeft, numberOfInputEdgesRight, joinType);
}

std::shared_ptr<Schema> LogicalJoinDescriptor::getLeftSourceType() const
{
    return leftSourceType;
}

std::shared_ptr<Schema> LogicalJoinDescriptor::getRightSourceType() const
{
    return rightSourceType;
}

std::shared_ptr<Windowing::WindowType> LogicalJoinDescriptor::getWindowType() const
{
    return windowType;
}

Join::LogicalJoinDescriptor::JoinType LogicalJoinDescriptor::getJoinType() const
{
    return joinType;
}

uint64_t LogicalJoinDescriptor::getNumberOfInputEdgesLeft() const
{
    return numberOfInputEdgesLeft;
}

uint64_t LogicalJoinDescriptor::getNumberOfInputEdgesRight() const
{
    return numberOfInputEdgesRight;
}

void LogicalJoinDescriptor::updateSourceTypes(std::shared_ptr<Schema> leftSourceType, std::shared_ptr<Schema> rightSourceType)
{
    if (leftSourceType)
    {
        this->leftSourceType = std::move(leftSourceType);
    }
    if (rightSourceType)
    {
        this->rightSourceType = std::move(rightSourceType);
    }
}

void LogicalJoinDescriptor::updateOutputDefinition(std::shared_ptr<Schema> outputSchema)
{
    if (outputSchema)
    {
        this->outputSchema = std::move(outputSchema);
    }
}

std::shared_ptr<Schema> LogicalJoinDescriptor::getOutputSchema() const
{
    return outputSchema;
}

void LogicalJoinDescriptor::setNumberOfInputEdgesLeft(const uint64_t numberOfInputEdgesLeft)
{
    LogicalJoinDescriptor::numberOfInputEdgesLeft = numberOfInputEdgesLeft;
}

void LogicalJoinDescriptor::setNumberOfInputEdgesRight(const uint64_t numberOfInputEdgesRight)
{
    LogicalJoinDescriptor::numberOfInputEdgesRight = numberOfInputEdgesRight;
}

OriginId LogicalJoinDescriptor::getOriginId() const
{
    return originId;
}

void LogicalJoinDescriptor::setOriginId(const OriginId originId)
{
    this->originId = originId;
}

std::shared_ptr<NodeFunction> LogicalJoinDescriptor::getJoinFunction()
{
    return this->joinFunction;
}

bool LogicalJoinDescriptor::equals(const LogicalJoinDescriptor& other) const
{
    return (*leftSourceType == *other.leftSourceType) && (*rightSourceType == *other.rightSourceType)
        && (*outputSchema == *other.outputSchema) && windowType->equal(other.windowType) && joinFunction->equal(other.joinFunction)
        && numberOfInputEdgesLeft == other.numberOfInputEdgesLeft && numberOfInputEdgesRight == other.numberOfInputEdgesRight
        && joinType == other.joinType && originId == other.originId;
}

}; /// namespace NES::Join
