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
#include <string>
#include <unordered_set>
#include <utility>
#include <ranges>
#include <vector>
#include <Functions/Expression.hpp>
#include <Functions/FieldAccessExpression.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Nodes/Iterators/DepthFirstNodeIterator.hpp>
#include <Nodes/Node.hpp>
#include <Operators/LogicalOperators/LogicalOperator.hpp>
#include <Operators/LogicalOperators/LogicalSelectionOperator.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

LogicalSelectionOperator::LogicalSelectionOperator(ExpressionValue predicate, OperatorId id)
    : Operator(id), LogicalUnaryOperator(id), predicate(std::move(predicate))
{
}

ExpressionValue LogicalSelectionOperator::getPredicate() const
{
    return predicate;
}

void LogicalSelectionOperator::setPredicate(ExpressionValue newPredicate)
{
    predicate = std::move(newPredicate);
}

bool LogicalSelectionOperator::isIdentical(const std::shared_ptr<Node>& rhs) const
{
    return equal(rhs) && NES::Util::as<LogicalSelectionOperator>(rhs)->getId() == id;
}

bool LogicalSelectionOperator::equal(const std::shared_ptr<Node>& rhs) const
{
    if (NES::Util::instanceOf<LogicalSelectionOperator>(rhs))
    {
        const auto filterOperator = NES::Util::as<LogicalSelectionOperator>(rhs);
        return predicate == filterOperator->predicate;
    }
    return false;
};

std::string LogicalSelectionOperator::toString() const
{
    std::stringstream ss;
    ss << "FILTER(opId: " << id << ": predicate: " << format_as(predicate) << ")";
    return ss.str();
}

bool LogicalSelectionOperator::inferSchema()
{
    if (!LogicalUnaryOperator::inferSchema())
    {
        return false;
    }
    predicate.inferStamp(inputSchema);
    return predicate.getStamp()->name() == "Bool";
}

std::shared_ptr<Operator> LogicalSelectionOperator::copy()
{
    auto copy = std::make_shared<LogicalSelectionOperator>(predicate, id);
    copy->setInputOriginIds(inputOriginIds);
    copy->setInputSchema(inputSchema);
    copy->setOutputSchema(outputSchema);
    copy->setHashBasedSignature(hashBasedSignature);
    copy->setOperatorState(operatorState);
    for (const auto& [key, value] : properties)
    {
        copy->addProperty(key, value);
    }
    return copy;
}

void LogicalSelectionOperator::inferStringSignature()
{
    const std::shared_ptr<Operator> operatorNode = NES::Util::as<Operator>(shared_from_this());
    NES_TRACE("LogicalSelectionOperator: Inferring String signature for {}", *operatorNode);
    INVARIANT(!children.empty(), "LogicalSelectionOperator: Filter should have children");

    ///Infer query signatures for child operators
    for (const auto& child : children)
    {
        const std::shared_ptr<LogicalOperator> childOperator = NES::Util::as<LogicalOperator>(child);
        childOperator->inferStringSignature();
    }

    std::stringstream signatureStream;
    const auto childSignature = NES::Util::as<LogicalOperator>(children.at(0))->getHashBasedSignature();
    signatureStream << "FILTER(" << format_as(predicate) << ")." << *childSignature.begin()->second.begin();

    ///Update the signature
    const auto hashCode = hashGenerator(signatureStream.str());
    hashBasedSignature[hashCode] = {signatureStream.str()};
}
float LogicalSelectionOperator::getSelectivity() const
{
    return selectivity;
}
void LogicalSelectionOperator::setSelectivity(float newSelectivity)
{
    selectivity = newSelectivity;
}

std::vector<std::string> LogicalSelectionOperator::getFieldNamesUsedByFilterPredicate() const
{
    NES_TRACE("LogicalSelectionOperator: Find all field names used in filter operator");
    auto fieldAccesses = predicate.dfs() | std::views::filter([](const auto& expr) { return expr.template as<FieldAccessExpression>() != nullptr; })
        | std::views::transform([](const auto& expr) { return expr.template as<FieldAccessExpression>()->getFieldName().name; })
        | std::ranges::to<std::vector>();

    return {fieldAccesses.begin(), fieldAccesses.end()};
}

}
