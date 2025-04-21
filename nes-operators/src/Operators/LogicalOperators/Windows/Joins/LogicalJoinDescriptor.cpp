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
#include <Functions/Expression.hpp>
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
    ExpressionValue joinFunction,
    std::shared_ptr<Windowing::WindowType> windowType,
    const uint64_t numberOfInputEdgesLeft,
    const uint64_t numberOfInputEdgesRight,
    const JoinType joinType,
    const OriginId originId)
    : joinFunction(std::move(joinFunction))
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
    ExpressionValue joinFunctions,
    const std::shared_ptr<Windowing::WindowType>& windowType,
    uint64_t numberOfInputEdgesLeft,
    uint64_t numberOfInputEdgesRight,
    JoinType joinType)
{
    return std::make_shared<Join::LogicalJoinDescriptor>(
        joinFunctions, windowType, numberOfInputEdgesLeft, numberOfInputEdgesRight, joinType);
}

const Schema& LogicalJoinDescriptor::getLeftSourceType() const
{
    return leftSourceType.value();
}

const Schema& LogicalJoinDescriptor::getRightSourceType() const
{
    return rightSourceType.value();
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

void LogicalJoinDescriptor::updateSourceTypes(Schema leftSourceType, Schema rightSourceType)
{
    this->leftSourceType = std::move(leftSourceType);
    this->rightSourceType = std::move(rightSourceType);
}

void LogicalJoinDescriptor::updateOutputDefinition(Schema outputSchema)
{
    this->outputSchema = std::move(outputSchema);
}

const Schema& LogicalJoinDescriptor::getOutputSchema() const
{
    return outputSchema.value();
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

ExpressionValue LogicalJoinDescriptor::getJoinFunction()
{
    return this->joinFunction;
}

bool LogicalJoinDescriptor::equals(const LogicalJoinDescriptor& other) const
{
    return (*leftSourceType == *other.leftSourceType) && (*rightSourceType == *other.rightSourceType)
        && (*outputSchema == *other.outputSchema) && windowType->equal(other.windowType) && joinFunction == other.joinFunction
        && numberOfInputEdgesLeft == other.numberOfInputEdgesLeft && numberOfInputEdgesRight == other.numberOfInputEdgesRight
        && joinType == other.joinType && originId == other.originId;
}

}; /// namespace NES::Join
