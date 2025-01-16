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
#include <queue>
#include <set>
#include <API/Schema.hpp>
#include <Functions/LogicalFunctions/NodeFunctionEquals.hpp>
#include <Functions/NodeFunction.hpp>
#include <Functions/NodeFunctionBinary.hpp>
#include <Functions/NodeFunctionFieldAccess.hpp>
#include <Functions/NodeFunctionFieldAssignment.hpp>
#include <Functions/NodeFunctionFieldRename.hpp>
#include <Nodes/Iterators/DepthFirstNodeIterator.hpp>
#include <Nodes/Node.hpp>
#include <Operators/LogicalOperators/LogicalMapOperator.hpp>
#include <Operators/LogicalOperators/LogicalProjectionOperator.hpp>
#include <Operators/LogicalOperators/LogicalSelectionOperator.hpp>
#include <Operators/LogicalOperators/LogicalUnionOperator.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/LogicalOperators/Sources/SourceNameLogicalOperator.hpp>
#include <Operators/LogicalOperators/Watermarks/WatermarkAssignerLogicalOperator.hpp>
#include <Operators/LogicalOperators/Windows/Aggregations/WindowAggregationDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/Joins/LogicalJoinDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/Joins/LogicalJoinOperator.hpp>
#include <Operators/LogicalOperators/Windows/LogicalWindowDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/LogicalWindowOperator.hpp>
#include <Optimizer/QueryRewrite/FilterPushDownRule.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Types/ContentBasedWindowType.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/UtilityFunction.hpp>

namespace NES::Optimizer
{

std::shared_ptr<FilterPushDownRule> FilterPushDownRule::create()
{
    return std::make_shared<FilterPushDownRule>(FilterPushDownRule());
}

FilterPushDownRule::FilterPushDownRule() = default;

std::shared_ptr<QueryPlan> FilterPushDownRule::apply(std::shared_ptr<QueryPlan> queryPlan)
{
    NES_INFO("Applying FilterPushDownRule to query {}", queryPlan->toString());
    const std::vector<std::shared_ptr<Operator>> rootOperators = queryPlan->getRootOperators();
    std::set<std::shared_ptr<LogicalSelectionOperator>> filterOperatorsSet;
    for (const std::shared_ptr<Operator>& rootOperator : rootOperators)
    {
        std::vector<std::shared_ptr<LogicalSelectionOperator>> filters = rootOperator->getNodesByType<LogicalSelectionOperator>();
        filterOperatorsSet.insert(filters.begin(), filters.end());
    }
    std::vector<std::shared_ptr<LogicalSelectionOperator>> filterOperators(filterOperatorsSet.begin(), filterOperatorsSet.end());
    NES_TRACE("FilterPushDownRule: Sort all filter nodes in increasing order of the operator id");
    std::sort(
        filterOperators.begin(),
        filterOperators.end(),
        [](const std::shared_ptr<LogicalSelectionOperator>& lhs, const std::shared_ptr<LogicalSelectionOperator>& rhs)
        { return lhs->getId() < rhs->getId(); });
    auto originalQueryPlan = queryPlan->copy();
    try
    {
        NES_TRACE("FilterPushDownRule: Iterate over all the filter operators to push them down in the query plan");
        for (const std::shared_ptr<LogicalSelectionOperator>& filterOperator : filterOperators)
        {
            /// method calls itself recursively until it can not push the filter further down(upstream).
            pushDownFilter(filterOperator, filterOperator->getChildren()[0], filterOperator);
        }
        NES_INFO("FilterPushDownRule: Return the updated query plan {}", queryPlan->toString());
        return queryPlan;
    }
    catch (std::exception& exc)
    {
        NES_ERROR("FilterPushDownRule: Error while applying FilterPushDownRule: {}", exc.what());
        NES_ERROR("FilterPushDownRule: Returning unchanged original query plan {}", originalQueryPlan->toString());
        return originalQueryPlan;
    }
}

void FilterPushDownRule::pushDownFilter(
    std::shared_ptr<LogicalSelectionOperator> filterOperator, std::shared_ptr<Node> curOperator, std::shared_ptr<Node> parOperator)

{
    if (NES::Util::instanceOf<LogicalProjectionOperator>(curOperator))
    {
        pushBelowProjection(filterOperator, curOperator);
    }
    else if (NES::Util::instanceOf<LogicalMapOperator>(curOperator))
    {
        auto mapOperator = NES::Util::as<LogicalMapOperator>(curOperator);
        pushFilterBelowMap(filterOperator, mapOperator);
    }
    else if (NES::Util::instanceOf<LogicalJoinOperator>(curOperator))
    {
        auto joinOperator = NES::Util::as<LogicalJoinOperator>(curOperator);
        pushFilterBelowJoin(filterOperator, joinOperator, parOperator);
    }
    else if (NES::Util::instanceOf<LogicalUnionOperator>(curOperator))
    {
        pushFilterBelowUnion(filterOperator, curOperator);
    }
    else if (NES::Util::instanceOf<LogicalWindowOperator>(curOperator))
    {
        pushFilterBelowWindowAggregation(filterOperator, curOperator, parOperator);
    }
    else if (NES::Util::instanceOf<WatermarkAssignerLogicalOperator>(curOperator))
    {
        pushDownFilter(filterOperator, curOperator->getChildren()[0], curOperator);
    }
    /// if we have a source operator or some unsupported operator we are not able to push the filter below this operator.
    else
    {
        insertFilterIntoNewPosition(filterOperator, curOperator, parOperator);
    }
}

void FilterPushDownRule::pushFilterBelowJoin(
    const std::shared_ptr<LogicalSelectionOperator>& filterOperator,
    const std::shared_ptr<LogicalJoinOperator>& joinOperator,
    const std::shared_ptr<Node>& parentOperator)
{
    /// we might be able to push the filter to both branches of the join, and we check this first.
    bool pushed = pushFilterBelowJoinSpecialCase(filterOperator, joinOperator);

    /// field names that are used by the filter
    std::vector<std::string> predicateFields = filterOperator->getFieldNamesUsedByFilterPredicate();
    /// better readability
    const std::shared_ptr<LogicalJoinOperator> curOperatorAsJoin = NES::Util::as<LogicalJoinOperator>(joinOperator);
    /// we might have pushed it already within the special condition before
    if (!pushed)
    {
        /// if any inputSchema contains all the fields that are used by the filter we can push the filter to the corresponding site
        const std::shared_ptr<Schema> leftSchema = curOperatorAsJoin->getLeftInputSchema();
        const std::shared_ptr<Schema> rightSchema = curOperatorAsJoin->getRightInputSchema();
        bool leftBranchPossible = true;
        bool rightBranchPossible = true;

        /// if any field that is used by the filter is not part of the branches schema, we can't push the filter to that branch.
        NES_TRACE("FilterPushDownRule.pushFilterBelowJoin: Checking if filter can be pushed below left or right branch of join");
        for (const std::string& fieldUsedByFilter : predicateFields)
        {
            if (!leftSchema->contains(fieldUsedByFilter))
            {
                leftBranchPossible = false;
            }
            if (!rightSchema->contains(fieldUsedByFilter))
            {
                rightBranchPossible = false;
            }
        }

        if (leftBranchPossible)
        {
            NES_TRACE("FilterPushDownRule.pushFilterBelowJoin: Pushing filter below left branch of join");
            pushed = true;
            pushDownFilter(filterOperator, curOperatorAsJoin->getLeftOperators()[0], joinOperator);
        }
        else if (rightBranchPossible)
        {
            NES_TRACE("FilterPushDownRule.pushFilterBelowJoin: Pushing filter below right branch of join");
            pushed = true;
            pushDownFilter(filterOperator, curOperatorAsJoin->getRightOperators()[0], joinOperator);
        }
    }

    if (!pushed)
    {
        NES_TRACE("FilterPushDownRule.pushFilterBelowJoin: Filter was not pushed, we insert filter at this position");
        insertFilterIntoNewPosition(filterOperator, joinOperator, parentOperator);
    }
}

bool FilterPushDownRule::pushFilterBelowJoinSpecialCase(
    const std::shared_ptr<LogicalSelectionOperator>& filterOperator, const std::shared_ptr<LogicalJoinOperator>& joinOperator)
{
    std::vector<std::string> predicateFields = filterOperator->getFieldNamesUsedByFilterPredicate();
    const std::shared_ptr<Join::LogicalJoinDescriptor> joinDefinition = joinOperator->getJoinDefinition();
    NES_TRACE("FilterPushDownRule.pushFilterBelowJoinSpecialCase: Extracted field names that are used by the filter");

    /// returns the following pair:  std::make_pair(leftJoinKeyNameEqui,rightJoinKeyNameEqui);
    auto equiJoinKeyNames = NES::findEquiJoinKeyNames(joinDefinition->getJoinFunction());

    if (equiJoinKeyNames.first == predicateFields[0])
    {
        NES_TRACE("FilterPushDownRule.pushFilterBelowJoinSpecialCase: Filter field name found in the left side of the join");
        auto copyOfFilter = NES::Util::as<LogicalSelectionOperator>(filterOperator->copy());
        copyOfFilter->setId(getNextOperatorId());
        NES_TRACE("FilterPushDownRule.pushFilterBelowJoinSpecialCase: Created a copy of the filter");

        const std::shared_ptr<NodeFunction> newPredicate = filterOperator->getPredicate()->deepCopy();

        renameNodeFunctionFieldAccesss(newPredicate, equiJoinKeyNames.first, equiJoinKeyNames.second);

        copyOfFilter->setPredicate(newPredicate);
        NES_TRACE("FilterPushDownRule.pushFilterBelowJoinSpecialCase: Set the right side field name to the predicate field");

        pushDownFilter(filterOperator, joinOperator->getLeftOperators()[0], joinOperator);
        NES_TRACE("FilterPushDownRule.pushFilterBelowJoinSpecialCase: Pushed original filter to one branch");
        pushDownFilter(copyOfFilter, joinOperator->getRightOperators()[0], joinOperator);
        NES_TRACE("FilterPushDownRule.pushFilterBelowJoinSpecialCase: Pushed copy of the filter to other branch");
        return true;
    }
    else if (equiJoinKeyNames.second == predicateFields[0])
    {
        NES_TRACE("FilterPushDownRule.pushFilterBelowJoinSpecialCase: Filter field name found in the right side of the join");
        auto copyOfFilter = NES::Util::as<LogicalSelectionOperator>(filterOperator->copy());
        copyOfFilter->setId(getNextOperatorId());
        NES_TRACE("FilterPushDownRule.pushFilterBelowJoinSpecialCase: Created a copy of the filter");

        const std::shared_ptr<NodeFunction> newPredicate = filterOperator->getPredicate()->deepCopy();
        renameNodeFunctionFieldAccesss(newPredicate, equiJoinKeyNames.second, equiJoinKeyNames.first);
        copyOfFilter->setPredicate(newPredicate);
        NES_TRACE("FilterPushDownRule.pushFilterBelowJoinSpecialCase: Set the left side field name to the predicate field");

        pushDownFilter(filterOperator, joinOperator->getRightOperators()[0], joinOperator);
        NES_TRACE("FilterPushDownRule.pushFilterBelowJoinSpecialCase: Pushed original filter to one branch");
        pushDownFilter(copyOfFilter, joinOperator->getLeftOperators()[0], joinOperator);
        NES_TRACE("FilterPushDownRule.pushFilterBelowJoinSpecialCase: Pushed copy of the filter to other branch");
        return true;
    }
    return false;
}

void FilterPushDownRule::pushFilterBelowMap(
    const std::shared_ptr<LogicalSelectionOperator>& filterOperator, const std::shared_ptr<LogicalMapOperator>& mapOperator)
{
    std::string assignmentFieldName = getAssignmentFieldFromMapOperator(mapOperator);
    substituteFilterAttributeWithMapTransformation(filterOperator, mapOperator, assignmentFieldName);
    pushDownFilter(filterOperator, mapOperator->getChildren()[0], mapOperator);
}

void FilterPushDownRule::pushFilterBelowUnion(
    const std::shared_ptr<LogicalSelectionOperator>& filterOperator, const std::shared_ptr<Node>& unionOperator)
{
    std::vector<std::shared_ptr<Node>> grandChildren = unionOperator->getChildren();

    auto copyOfFilterOperator = NES::Util::as<LogicalSelectionOperator>(filterOperator->copy());
    copyOfFilterOperator->setId(getNextOperatorId());
    copyOfFilterOperator->setPredicate(filterOperator->getPredicate()->deepCopy());
    NES_TRACE("FilterPushDownRule.pushFilterBelowUnion: Created a copy of the filter operator");

    NES_TRACE("FilterPushDownRule.pushFilterBelowUnion: Pushing filter into both branches below union operator...");
    pushDownFilter(filterOperator, grandChildren[0], unionOperator);
    NES_TRACE("FilterPushDownRule.pushFilterBelowUnion: Pushed original filter to one branch");
    pushDownFilter(copyOfFilterOperator, grandChildren[1], unionOperator);
    NES_TRACE("FilterPushDownRule.pushFilterBelowUnion: Pushed copy of the filter to other branch");
}

void FilterPushDownRule::pushFilterBelowWindowAggregation(
    const std::shared_ptr<LogicalSelectionOperator>& filterOperator,
    const std::shared_ptr<Node>& windowOperator,
    const std::shared_ptr<Node>& parOperator)
{
    auto groupByKeyNames = NES::Util::as<LogicalWindowOperator>(windowOperator)->getGroupByKeyNames();
    const std::vector<std::shared_ptr<NodeFunctionFieldAccess>> groupByKeys
        = NES::Util::as<LogicalWindowOperator>(windowOperator)->getWindowDefinition()->getKeys();
    std::vector<std::string> fieldNamesUsedByFilter = filterOperator->getFieldNamesUsedByFilterPredicate();
    NES_TRACE("FilterPushDownRule.pushFilterBelowWindowAggregation: Retrieved the group by keys of the window operator");

    bool areAllFilterAttributesInGroupByKeys = true;
    for (const auto& filterAttribute : fieldNamesUsedByFilter)
    {
        if (std::find(groupByKeyNames.begin(), groupByKeyNames.end(), filterAttribute) == groupByKeyNames.end())
        {
            areAllFilterAttributesInGroupByKeys = false;
        }
    }
    NES_TRACE("FilterPushDownRule.pushFilterBelowWindowAggregation: Checked if all attributes used by the filter are also used "
              "in the group by keys the group by keys of the window operator");

    if (areAllFilterAttributesInGroupByKeys)
    {
        NES_TRACE("FilterPushDownRule.pushFilterBelowWindowAggregation: All attributes used by the filter are also used in the"
                  " group by keys, we can push the filter below the window operator")
        NES_TRACE("FilterPushDownRule.pushFilterBelowWindowAggregation: Pushing filter below window operator...");
        pushDownFilter(filterOperator, windowOperator->getChildren()[0], windowOperator);
    }
    else
    {
        NES_TRACE("FilterPushDownRule.pushFilterBelowWindowAggregation: Attributes used by filter are not all used in "
                  "group by keys, inserting the filter into a new position...");
        insertFilterIntoNewPosition(filterOperator, windowOperator, parOperator);
    }
}

void FilterPushDownRule::pushBelowProjection(
    const std::shared_ptr<LogicalSelectionOperator>& filterOperator, const std::shared_ptr<Node>& projectionOperator)
{
    renameFilterAttributesByNodeFunctions(filterOperator, NES::Util::as<LogicalProjectionOperator>(projectionOperator)->getFunctions());
    pushDownFilter(filterOperator, projectionOperator->getChildren()[0], projectionOperator);
}

void FilterPushDownRule::insertFilterIntoNewPosition(
    const std::shared_ptr<LogicalSelectionOperator>& filterOperator,
    const std::shared_ptr<Node>& childOperator,
    const std::shared_ptr<Node>& parOperator)
{
    /// If the parent operator of the current operator is not the original filter operator, the filter has been pushed below some operators.
    /// so we have to remove it from its original position and insert at the new position (above the current operator, which it can't be pushed below)
    if (filterOperator->getId() != NES::Util::as<LogicalOperator>(parOperator)->getId())
    {
        /// if we remove the first child (which is the left branch) of a binary operator and insert our filter below this binary operator,
        /// it will be inserted as the second children (which is the right branch). To conserve order we will swap the branches after the insertion
        bool swapBranches = false;
        if (NES::Util::instanceOf<BinaryOperator>(parOperator) && parOperator->getChildren()[0]->equal(childOperator))
        {
            swapBranches = true;
        }

        /// removes filter operator from its original position and connects the parents and children of the filter operator at that position
        if (!filterOperator->removeAndJoinParentAndChildren())
        {
            /// if we did not manage to remove the operator we can't insert it at the new position
            NES_WARNING("FilterPushDownRule wanted to change the position of a filter, but was not able to do so.")
            return;
        }

        /// inserts the operator between the operator that it wasn't able to push below and the parent of this operator.
        bool success1 = childOperator->removeParent(parOperator); /// also removes childOperator as a child from parOperator
        bool success2 = childOperator->addParent(filterOperator); /// also adds childOperator as a child to filterOperator
        bool success3 = filterOperator->addParent(parOperator); /// also adds filterOperator as a child to parOperator
        if (!success1 || !success2 || !success3)
        {
            /// if we did manage to remove the filter from the queryPlan but now the insertion is not successful, that means that the queryPlan is invalid now.
            NES_ERROR("FilterPushDownRule removed a Filter from a query plan but was not able to insert it into the query plan again.")
            throw std::logic_error("FilterPushDownRule: query plan not valid anymore");
        }

        /// the input schema of the filter is going to be the same as the output schema of the node below. Its output schema is the same as its input schema.
        filterOperator->setInputSchema(NES::Util::as<Operator>(filterOperator->getChildren()[0])->getOutputSchema()->copy());
        NES::Util::as<Operator>(filterOperator)
            ->setOutputSchema(NES::Util::as<Operator>(filterOperator->getChildren()[0])->getOutputSchema()->copy());

        /// conserve order
        if (swapBranches)
        {
            parOperator->swapLeftAndRightBranch();
        }
    }
}

std::vector<std::shared_ptr<NodeFunctionFieldAccess>>
FilterPushDownRule::getFilterAccessFunctions(const std::shared_ptr<NodeFunction>& filterPredicate)
{
    std::vector<std::shared_ptr<NodeFunctionFieldAccess>> filterAccessFunctions;
    NES_TRACE("FilterPushDownRule: Create an iterator for traversing the filter predicates");
    DepthFirstNodeIterator depthFirstNodeIterator(filterPredicate);
    for (auto itr = depthFirstNodeIterator.begin(); itr != NES::DepthFirstNodeIterator::end(); ++itr)
    {
        NES_TRACE("FilterPushDownRule: Iterate and find the predicate with FieldAccessFunction Node");
        if ((NES::Util::instanceOf<NodeFunctionFieldAccess>(*itr)))
        {
            const std::shared_ptr<NodeFunctionFieldAccess> accessNodeFunction = NES::Util::as<NodeFunctionFieldAccess>(*itr);
            NES_TRACE("FilterPushDownRule: Add the field name to the list of filter attribute names");
            filterAccessFunctions.push_back(accessNodeFunction);
        }
    }
    return filterAccessFunctions;
}

void FilterPushDownRule::renameNodeFunctionFieldAccesss(
    const std::shared_ptr<NodeFunction>& nodeFunction, const std::string& toReplace, const std::string& replacement)
{
    DepthFirstNodeIterator depthFirstNodeIterator(nodeFunction);
    for (auto itr = depthFirstNodeIterator.begin(); itr != NES::DepthFirstNodeIterator::end(); ++itr)
    {
        if (NES::Util::instanceOf<NodeFunctionFieldAccess>(*itr))
        {
            const std::shared_ptr<NodeFunctionFieldAccess> accessNodeFunction = NES::Util::as<NodeFunctionFieldAccess>(*itr);
            if (accessNodeFunction->getFieldName() == toReplace)
            {
                accessNodeFunction->updateFieldName(replacement);
            }
        }
    }
}

void FilterPushDownRule::renameFilterAttributesByNodeFunctions(
    const std::shared_ptr<LogicalSelectionOperator>& filterOperator, const std::vector<std::shared_ptr<NodeFunction>>& nodeFunctions)
{
    const std::shared_ptr<NodeFunction> predicateCopy = filterOperator->getPredicate()->deepCopy();
    NES_TRACE("FilterPushDownRule: Iterate over all functions in the projection operator");

    for (auto& nodeFunction : nodeFunctions)
    {
        NES_TRACE("FilterPushDownRule: Check if the function node is of type NodeFunctionFieldRename")
        if (Util::instanceOf<NodeFunctionFieldRename>(nodeFunction))
        {
            const std::shared_ptr<NodeFunctionFieldRename> fieldRenameNodeFunction = Util::as<NodeFunctionFieldRename>(nodeFunction);
            std::string newFieldName = fieldRenameNodeFunction->getNewFieldName();
            std::string originalFieldName = fieldRenameNodeFunction->getOriginalField()->getFieldName();
            renameNodeFunctionFieldAccesss(predicateCopy, newFieldName, originalFieldName);
        }
    }

    filterOperator->setPredicate(predicateCopy);
}

std::string FilterPushDownRule::getAssignmentFieldFromMapOperator(const std::shared_ptr<Node>& node)
{
    NES_TRACE("FilterPushDownRule: Find the field name used in map operator");
    const std::shared_ptr<LogicalMapOperator> mapLogicalOperator = NES::Util::as<LogicalMapOperator>(node);
    const std::shared_ptr<NodeFunctionFieldAssignment> mapFunction = mapLogicalOperator->getMapFunction();
    const std::shared_ptr<NodeFunctionFieldAccess> field = mapFunction->getField();
    return field->getFieldName();
}

void FilterPushDownRule::substituteFilterAttributeWithMapTransformation(
    const std::shared_ptr<LogicalSelectionOperator>& filterOperator,
    const std::shared_ptr<LogicalMapOperator>& mapOperator,
    const std::string& fieldName)
{
    NES_TRACE("Substitute filter predicate's field access function with map transformation.");
    NES_TRACE("Current map transformation: {}", *mapOperator->getMapFunction());
    const std::shared_ptr<NodeFunctionFieldAssignment> mapFunction = mapOperator->getMapFunction();
    const std::shared_ptr<NodeFunction> mapTransformation = mapFunction->getAssignment();
    const std::vector<std::shared_ptr<NodeFunctionFieldAccess>> filterAccessFunctions
        = getFilterAccessFunctions(filterOperator->getPredicate());
    /// iterate over all filter access functions
    /// replace the filter predicate's field access function with map transformation
    /// if the field name of the field access function is the same as the field name used in map transformation
    for (auto& filterAccessFunction : filterAccessFunctions)
    {
        if (filterAccessFunction->getFieldName() == fieldName)
        {
            filterAccessFunction->replace(mapTransformation->deepCopy());
        }
    }
    NES_TRACE("New filter predicate: {}", *filterOperator->getPredicate());
}
}
