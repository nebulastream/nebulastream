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
#include <Functions/LogicalFunctions/AndBinaryLogicalFunction.hpp>
#include <Functions/LogicalFunctions/NegateUnaryLogicalFunction.hpp>
#include <Functions/LogicalFunctions/OrBinaryLogicalFunction.hpp>
#include <Operators/LogicalOperators/LogicalSelectionOperator.hpp>
#include <Optimizer/QueryRewrite/FilterSplitUpRule.hpp>
#include <Plans/QueryPlan.hpp>
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
        auto filters = rootOperator->getOperatorsByType<LogicalSelectionOperator>();
        filterOperatorsSet.insert(filters.begin(), filters.end());
    }
    std::vector<std::shared_ptr<LogicalSelectionOperator>> filterOperators(filterOperatorsSet.begin(), filterOperatorsSet.end());
    NES_DEBUG("FilterSplitUpRule: Sort all filter nodes in increasing order of the operator id")
    std::sort(
        filterOperators.begin(),
        filterOperators.end(),
        [](const std::shared_ptr<LogicalSelectionOperator>& lhs, const std::shared_ptr<LogicalSelectionOperator>& rhs) { return lhs->getId() < rhs->getId(); });
    auto originalQueryPlan = queryPlan->clone();
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

void FilterSplitUpRule::splitUpFilters(std::shared_ptr<LogicalSelectionOperator> filterOperator)
{
    /// if our query plan contains a parentOperaters->filter(function1 && function2)->childOperator.
    /// We can rewrite this plan to parentOperaters->filter(function1)->filter(function2)->childOperator.
    if (Util::instanceOf<AndBinaryLogicalFunction>(filterOperator->getPredicate()))
    {
        /// create filter that contains function1 of the andFunction
        auto child1 = Util::as<LogicalSelectionOperator>(filterOperator->clone());
        child1->setId(getNextOperatorId());
        child1->setPredicate(Util::as<LogicalFunction>(filterOperator->getPredicate()->children[0]));

        /// create filter that contains function2 of the andFunction
        auto child2 = Util::as<LogicalSelectionOperator>(filterOperator->clone());
        child2->setId(getNextOperatorId());
        child2->setPredicate(Util::as<LogicalFunction>(filterOperator->getPredicate()->children[1]));

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

        /// newly created filters could also be andFunctions that can be further split up
        splitUpFilters(child1);
        splitUpFilters(child2);
    }
    /// it might be possible to reformulate negated functions
    else if (Util::instanceOf<NegateUnaryLogicalFunction>(filterOperator->getPredicate()))
    {
        /// In the case that the predicate is of the form !( function1 || function2 ) it can be reformulated to ( !function1 && !function2 ).
        /// The reformulated predicate can be used to apply the split up filter rule again.
        if (Util::instanceOf<OrBinaryLogicalFunction>(filterOperator->getPredicate()->children[0]))
        {
            auto orFunction = filterOperator->getPredicate()->children[0];
            auto negatedChild1 = NegateUnaryLogicalFunction::create(Util::as<LogicalFunction>(orFunction->children[0]));
            auto negatedChild2 = NegateUnaryLogicalFunction::create(Util::as<LogicalFunction>(orFunction->children[1]));

            auto equivalentAndFunction = AndBinaryLogicalFunction::create(negatedChild1, negatedChild2);
            filterOperator->setPredicate(equivalentAndFunction); /// changing predicate to equivalent AndFunction
            splitUpFilters(filterOperator); /// splitting up the filter
        }
        /// Reformulates predicates in the form (!!function) to (function)
        else if (Util::instanceOf<NegateUnaryLogicalFunction>(filterOperator->getPredicate()->children[0]))
        {
            /// getPredicate() is the first FunctionNegate; first children[0] is the second FunctionNegate;
            /// second children[0] is the LogicalFunction that was negated twice. copy() only copies children of this LogicalFunction. (probably not mandatory but no reference to the negations needs to be kept)
            filterOperator->setPredicate(Util::as<LogicalFunction>(filterOperator->getPredicate()->children[0]->children[0])->clone());
            splitUpFilters(filterOperator);
        }
    }
}

}
