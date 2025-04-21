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
#include <set>
#include <vector>

#include <Functions/FunctionExpression.hpp>
#include <Functions/WellKnownFunctions.hpp>
#include <Operators/LogicalOperators/LogicalSelectionOperator.hpp>
#include <Optimizer/QueryRewrite/FilterSplitUpRule.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Optimizer
{

std::shared_ptr<FilterSplitUpRule> FilterSplitUpRule::create()
{
    return std::make_shared<FilterSplitUpRule>(FilterSplitUpRule());
}

FilterSplitUpRule::FilterSplitUpRule() = default;

std::shared_ptr<QueryPlan> FilterSplitUpRule::apply(std::shared_ptr<QueryPlan> queryPlan)
{
    NES_INFO("Applying FilterSplitUpRule to query {}", queryPlan->toString());
    const auto rootOperators = queryPlan->getRootOperators();
    std::set<std::shared_ptr<LogicalSelectionOperator>> filterOperatorsSet;
    for (const std::shared_ptr<Operator>& rootOperator : rootOperators)
    {
        auto filters = rootOperator->getNodesByType<LogicalSelectionOperator>();
        filterOperatorsSet.insert(filters.begin(), filters.end());
    }
    std::vector<std::shared_ptr<LogicalSelectionOperator>> filterOperators(filterOperatorsSet.begin(), filterOperatorsSet.end());
    NES_DEBUG("FilterSplitUpRule: Sort all filter nodes in increasing order of the operator id")
    std::sort(
        filterOperators.begin(),
        filterOperators.end(),
        [](const std::shared_ptr<LogicalSelectionOperator>& lhs, const std::shared_ptr<LogicalSelectionOperator>& rhs)
        { return lhs->getId() < rhs->getId(); });
    auto originalQueryPlan = queryPlan->copy();
    try
    {
        NES_DEBUG("FilterSplitUpRule: Iterate over all the filter operators to split them");
        for (auto filterOperator : filterOperators)
        {
            splitUpFilters(filterOperator);
        }
        return queryPlan;
    }
    catch (std::exception& exc)
    {
        NES_ERROR("FilterSplitUpRule: Error while applying FilterSplitUpRule: {}", exc.what());
        NES_ERROR("FilterSplitUpRule: Returning unchanged original query plan {}", originalQueryPlan->toString());
        return originalQueryPlan;
    }
}

bool demorgan(const std::shared_ptr<LogicalSelectionOperator>& filterOperator)
{
    auto predicateExpression = filterOperator->getPredicate();
    PRECONDITION(
        filterOperator->getPredicate().as<FunctionExpression>()
            && filterOperator->getPredicate().as<FunctionExpression>()->getFunctionName() == WellKnown::Negate,
        "This function assume a `Negate` function");

    auto childExpression = predicateExpression.getChildren().at(0);
    auto possibleOrFunction = childExpression.as<FunctionExpression>();
    if (!possibleOrFunction)
    {
        return false;
    }

    if (possibleOrFunction->getFunctionName() != WellKnown::Or)
    {
        return false;
    }

    filterOperator->setPredicate(make_expression<FunctionExpression>(
        {
            make_expression<FunctionExpression>({childExpression.getChildren().at(0)}, WellKnown::Negate),
            make_expression<FunctionExpression>({childExpression.getChildren().at(1)}, WellKnown::Negate),
        },
        WellKnown::And));
    return true;
}

void FilterSplitUpRule::splitUpFilters(const std::shared_ptr<LogicalSelectionOperator>& filterOperator)
{
    /// if our query plan contains a parentOperaters->filter(function1 && function2)->childOperator.
    /// We can rewrite this plan to parentOperaters->filter(function1)->filter(function2)->childOperator.
    auto predicateExpression = filterOperator->getPredicate();
    if (!filterOperator->getPredicate().as<FunctionExpression>())
    {
        return;
    }

    if (predicateExpression.as<FunctionExpression>()->getFunctionName() == WellKnown::Negate)
    {
        if (demorgan(filterOperator))
        {
            splitUpFilters(filterOperator);
        }
        return;
    }

    if (predicateExpression.as<FunctionExpression>()->getFunctionName() != WellKnown::Equal)
    {
        return;
    }

    auto child1 = Util::as<LogicalSelectionOperator>(filterOperator->copy());
    child1->setId(getNextOperatorId());
    child1->setPredicate(predicateExpression.getChildren().at(0));

    auto child2 = Util::as<LogicalSelectionOperator>(filterOperator->copy());
    child2->setId(getNextOperatorId());
    child2->setPredicate(predicateExpression.getChildren().at(1));

    /// insert new filter with function1 of the andFunction
    if (!filterOperator->insertBetweenThisAndChildNodes(child1))
    {
        NES_ERROR("FilterSplitUpRule: Error while trying to insert a filterOperator into the queryPlan");
        throw std::logic_error("FilterSplitUpRule: query plan not valid anymore");
    }
    /// insert new filter with function2 of the andFunction
    if (!child1->insertBetweenThisAndChildNodes(child2))
    {
        NES_ERROR("FilterSplitUpRule: Error while trying to insert a filterOperator into the queryPlan");
        throw std::logic_error("FilterSplitUpRule: query plan not valid anymore");
    }
    /// remove old filter that had the andFunction
    if (!filterOperator->removeAndJoinParentAndChildren())
    {
        NES_ERROR("FilterSplitUpRule: Error while trying to remove a filterOperator from the queryPlan");
        throw std::logic_error("FilterSplitUpRule: query plan not valid anymore");
    }

    splitUpFilters(child1);
    splitUpFilters(child2);
}

}
