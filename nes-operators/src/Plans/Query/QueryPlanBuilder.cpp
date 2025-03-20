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

#include <API/AttributeField.hpp>
#include <Expressions/FieldAssignmentExpressionNode.hpp>
#include <Expressions/FieldRenameExpressionNode.hpp>
#include <Expressions/LogicalExpressions/AndExpressionNode.hpp>
#include <Expressions/LogicalExpressions/EqualsExpressionNode.hpp>
#include <Measures/TimeCharacteristic.hpp>
#include <Operators/LogicalOperators/LogicalBatchJoinDescriptor.hpp>
#include <Operators/LogicalOperators/LogicalBinaryOperator.hpp>
#include <Operators/LogicalOperators/LogicalIntervalJoinDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/LogicalOperators/Sources/LogicalSourceDescriptor.hpp>
#include <Operators/LogicalOperators/UDFs/UDFDescriptor.hpp>
#include <Operators/LogicalOperators/Watermarks/EventTimeWatermarkStrategyDescriptor.hpp>
#include <Operators/LogicalOperators/Watermarks/IngestionTimeWatermarkStrategyDescriptor.hpp>
#include <Operators/LogicalOperators/Watermarks/WatermarkAssignerLogicalOperator.hpp>
#include <Operators/LogicalOperators/Windows/LogicalWindowDescriptor.hpp>
#include <Plans/Query/QueryPlanBuilder.hpp>
#include <Types/TimeBasedWindowType.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <iostream>
#include <limits>
#include <utility>

namespace NES {

QueryPlanPtr QueryPlanBuilder::createQueryPlan(std::string sourceName) {
    NES_DEBUG("QueryPlanBuilder: create query plan for input source  {}", sourceName);
    auto sourceOperator = LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create(sourceName));
    auto queryPlanPtr = QueryPlan::create(sourceOperator);
    queryPlanPtr->setSourceConsumed(sourceName);
    return queryPlanPtr;
}

QueryPlanPtr QueryPlanBuilder::addProjection(const std::vector<ExpressionNodePtr>& expressions, QueryPlanPtr queryPlan) {
    NES_DEBUG("QueryPlanBuilder: add projection operator to query plan");
    OperatorPtr op = LogicalOperatorFactory::createProjectionOperator(expressions);
    queryPlan->appendOperatorAsNewRoot(op);
    return queryPlan;
}

QueryPlanPtr QueryPlanBuilder::addRename(std::string const& newSourceName, QueryPlanPtr queryPlan) {
    NES_DEBUG("QueryPlanBuilder: add rename operator to query plan");
    auto op = LogicalOperatorFactory::createRenameSourceOperator(newSourceName);
    queryPlan->appendOperatorAsNewRoot(op);
    return queryPlan;
}

std::shared_ptr<ExpressionNode> QueryPlanBuilder::findFilter(ExpressionNodePtr joinExpression) {
    auto filterExpression = joinExpression;
    std::set<std::shared_ptr<BinaryExpressionNode>> expressionNodes;
    auto firstItr = BreadthFirstNodeIterator(joinExpression);
    std::set<std::shared_ptr<ExpressionNode>> filterExpressions;
    for (auto curr = firstItr.begin(); curr != BreadthFirstNodeIterator::end(); ++curr) {
        if ((*curr)->instanceOf<BinaryExpressionNode>()
            && !(*curr)->as<BinaryExpressionNode>()->getLeft()->instanceOf<BinaryExpressionNode>()) {
            auto visitedNode = (*curr)->as<BinaryExpressionNode>();
            if (expressionNodes.contains(visitedNode)) {
                continue;
            } else {
                auto left = (*curr)->as<BinaryExpressionNode>()->getLeft();
                auto right = (*curr)->as<BinaryExpressionNode>()->getRight();

                if (left->instanceOf<ConstantValueExpressionNode>() || right->instanceOf<ConstantValueExpressionNode>()) {
                    filterExpression = (*curr)->as<BinaryExpressionNode>();
                    filterExpressions.insert(filterExpression);
                    continue;
                }
                expressionNodes.insert(visitedNode);
            }
        }
    }
    /*
     * if we do not find an expression with a constant value,
     * we return the original join expression and use it to check whether we
     * need to append a filter to the query.
     */
    if (filterExpressions.empty()) {
        return joinExpression;
    }
    if (filterExpressions.size() == 1) {
        return filterExpression;
    }
    std::shared_ptr<ExpressionNode> combinedFilters = *filterExpressions.begin();
    for (auto curr = filterExpressions.begin(); curr != filterExpressions.end(); ++curr) {
        auto toCombine = (*curr)->as<BinaryExpressionNode>();
        if (toCombine->equal(combinedFilters)) {
            continue;
        }
        combinedFilters = AndExpressionNode::create(toCombine, combinedFilters);
        return combinedFilters;
    }
    return filterExpression;
}

QueryPlanPtr QueryPlanBuilder::addFilter(ExpressionNodePtr const& filterExpression, QueryPlanPtr queryPlan) {
    NES_DEBUG("QueryPlanBuilder: add filter operator to query plan");
    if (!filterExpression->getNodesByType<FieldRenameExpressionNode>().empty()) {
        NES_THROW_RUNTIME_ERROR("QueryPlanBuilder: Filter predicate cannot have a FieldRenameExpression");
    }
    OperatorPtr op = LogicalOperatorFactory::createFilterOperator(filterExpression);
    queryPlan->appendOperatorAsNewRoot(op);
    return queryPlan;
}

QueryPlanPtr QueryPlanBuilder::addLimit(const uint64_t limit, QueryPlanPtr queryPlan) {
    NES_DEBUG("QueryPlanBuilder: add limit operator to query plan");
    OperatorPtr op = LogicalOperatorFactory::createLimitOperator(limit);
    queryPlan->appendOperatorAsNewRoot(op);
    return queryPlan;
}

QueryPlanPtr QueryPlanBuilder::addMapUDF(Catalogs::UDF::UDFDescriptorPtr const& descriptor, QueryPlanPtr queryPlan) {
    NES_DEBUG("QueryPlanBuilder: add map java udf operator to query plan");
    auto op = LogicalOperatorFactory::createMapUDFLogicalOperator(descriptor);
    queryPlan->appendOperatorAsNewRoot(op);
    return queryPlan;
}

QueryPlanPtr QueryPlanBuilder::addFlatMapUDF(Catalogs::UDF::UDFDescriptorPtr const& descriptor, QueryPlanPtr queryPlan) {
    NES_DEBUG("QueryPlanBuilder: add flat map java udf operator to query plan");
    auto op = LogicalOperatorFactory::createFlatMapUDFLogicalOperator(descriptor);
    queryPlan->appendOperatorAsNewRoot(op);
    return queryPlan;
}

QueryPlanPtr QueryPlanBuilder::addMap(FieldAssignmentExpressionNodePtr const& mapExpression, QueryPlanPtr queryPlan) {
    NES_DEBUG("QueryPlanBuilder: add map operator to query plan");
    if (!mapExpression->getNodesByType<FieldRenameExpressionNode>().empty()) {
        NES_THROW_RUNTIME_ERROR("QueryPlanBuilder: Map expression cannot have a FieldRenameExpression");
    }
    OperatorPtr op = LogicalOperatorFactory::createMapOperator(mapExpression);
    queryPlan->appendOperatorAsNewRoot(op);
    return queryPlan;
}

QueryPlanPtr QueryPlanBuilder::addUnion(QueryPlanPtr leftQueryPlan, QueryPlanPtr rightQueryPlan) {
    NES_DEBUG("QueryPlanBuilder: unionWith the subQuery to current query plan");
    OperatorPtr op = LogicalOperatorFactory::createUnionOperator();
    leftQueryPlan = addBinaryOperatorAndUpdateSource(op, leftQueryPlan, rightQueryPlan);
    return leftQueryPlan;
}

QueryPlanPtr QueryPlanBuilder::addUnion(QueryPlanPtr leftQueryPlan, std::vector<QueryPlanPtr> subQueryPlans) {
    NES_DEBUG("QueryPlanBuilder: unionWith the subQuery to current query plan");
    if (subQueryPlans.size()<2) {
        NES_THROW_RUNTIME_ERROR("At least two subqueries are required for a union expression");
    }
    OperatorPtr op = LogicalOperatorFactory::createUnionOperator();
    std::string newSourceName;
    //we add the root of the right query plan as root of the left query plan
    // thus, children of the binary join operator are
    // [0] left and [1] right
    for (size_t i=0; i< subQueryPlans.size(); i++) {
        leftQueryPlan->addRootOperator(subQueryPlans[i]->getRootOperators()[0]);
        if (newSourceName.empty()) {
            newSourceName = Util::updateSourceName(leftQueryPlan->getSourceConsumed(), subQueryPlans[i]->getSourceConsumed());
        }else {
            newSourceName = Util::updateSourceName(newSourceName, subQueryPlans[i]->getSourceConsumed());
        }
    }
    leftQueryPlan->appendOperatorAsNewRoot(op);
    NES_DEBUG("QueryPlanBuilder: addBinaryOperatorAndUpdateSource: update the source names");
    leftQueryPlan->setSourceConsumed(newSourceName);
    return leftQueryPlan;
}

QueryPlanPtr QueryPlanBuilder::addStatisticBuildOperator(Windowing::WindowTypePtr window,
                                                         Statistic::WindowStatisticDescriptorPtr statisticDescriptor,
                                                         Statistic::StatisticMetricHash metricHash,
                                                         Statistic::SendingPolicyPtr sendingPolicy,
                                                         Statistic::TriggerConditionPtr triggerCondition,
                                                         QueryPlanPtr queryPlan) {
    queryPlan = checkAndAddWatermarkAssignment(queryPlan, window);
    auto op = LogicalOperatorFactory::createStatisticBuildOperator(window,
                                                                   std::move(statisticDescriptor),
                                                                   metricHash,
                                                                   sendingPolicy,
                                                                   triggerCondition);
    queryPlan->appendOperatorAsNewRoot(op);
    return queryPlan;
}

QueryPlanPtr QueryPlanBuilder::addJoin(
    QueryPlanPtr leftQueryPlan,
    QueryPlanPtr rightQueryPlan,
    ExpressionNodePtr joinExpression,
    const Windowing::WindowTypePtr& windowType,
    Join::LogicalJoinDescriptor::JoinType joinType = Join::LogicalJoinDescriptor::JoinType::CARTESIAN_PRODUCT) {
    NES_DEBUG("QueryPlanBuilder: joinWith the subQuery to current query");

    NES_DEBUG("QueryPlanBuilder: Iterate over all ExpressionNode to check join field.");
    std::unordered_set<std::shared_ptr<BinaryExpressionNode>> visitedExpressions;
    std::set<std::shared_ptr<BinaryExpressionNode>> expressionNodes;
    auto firstItr = BreadthFirstNodeIterator(joinExpression);
    for (auto curr = firstItr.begin(); curr != BreadthFirstNodeIterator::end(); ++curr) {
        if ((*curr)->instanceOf<BinaryExpressionNode>()
            && !(*curr)->as<BinaryExpressionNode>()->getLeft()->instanceOf<BinaryExpressionNode>()) {
            auto visitedNode = (*curr)->as<BinaryExpressionNode>();
            if (expressionNodes.contains(visitedNode)) {// check if we already visited that one
                continue;
            } else {
                auto left = (*curr)->as<BinaryExpressionNode>()->getLeft();
                auto right = (*curr)->as<BinaryExpressionNode>()->getRight();
                if ((left->instanceOf<ConstantValueExpressionNode>() || right->instanceOf<ConstantValueExpressionNode>())) {
                    continue;
                }
                expressionNodes.insert(visitedNode);
            }
        }
    }
    std::shared_ptr<ExpressionNode> newJoinExpression = *expressionNodes.begin();
    for (auto curr = expressionNodes.begin(); curr != expressionNodes.end(); ++curr) {
        auto toCombine = (*curr)->as<BinaryExpressionNode>();
        if (toCombine->equal(newJoinExpression)) {
            continue;
        }
        newJoinExpression = AndExpressionNode::create(toCombine, newJoinExpression);
    }
    joinExpression = newJoinExpression;
    auto bfsIterator = BreadthFirstNodeIterator(joinExpression);
    for (auto itr = bfsIterator.begin(); itr != BreadthFirstNodeIterator::end(); ++itr) {
        if ((*itr)->instanceOf<BinaryExpressionNode>()
            && !(*itr)->as<BinaryExpressionNode>()->getLeft()->instanceOf<BinaryExpressionNode>()) {
            auto visitingOp = (*itr)->as<BinaryExpressionNode>();
            if (visitedExpressions.contains(visitingOp)) {
                // skip rest of the steps as the node found in already visited node list
                continue;
            } else {
                visitedExpressions.insert(visitingOp);
                auto onLeftKey = (*itr)->as<BinaryExpressionNode>()->getLeft();
                auto onRightKey = (*itr)->as<BinaryExpressionNode>()->getRight();
                if (!onLeftKey->instanceOf<BinaryExpressionNode>() && !onRightKey->instanceOf<BinaryExpressionNode>()) {
                    NES_DEBUG("QueryPlanBuilder: Check if Expressions are FieldExpressions.");
                    auto leftKeyFieldAccess = checkExpression(onLeftKey, "leftSide");
                    auto rightQueryPlanKeyFieldAccess = checkExpression(onRightKey, "rightSide");
                }
            }
        }
    }

    NES_ASSERT(rightQueryPlan && !rightQueryPlan->getRootOperators().empty(), "invalid rightQueryPlan query plan");
    auto rootOperatorRhs = rightQueryPlan->getRootOperators()[0];
    auto leftJoinType = leftQueryPlan->getRootOperators()[0]->getOutputSchema();
    auto rightQueryPlanJoinType = rootOperatorRhs->getOutputSchema();

    // check if query contain watermark assigner, and add if missing (as default behaviour)
    leftQueryPlan = checkAndAddWatermarkAssignment(leftQueryPlan, windowType);
    rightQueryPlan = checkAndAddWatermarkAssignment(rightQueryPlan, windowType);

    //TODO 1,1 should be replaced once we have distributed joins with the number of child input edges
    //TODO(Ventura?>Steffen) can we know this at this query submission time?
    auto joinDefinition = Join::LogicalJoinDescriptor::create(joinExpression, windowType, 1, 1, joinType);

    NES_DEBUG("QueryPlanBuilder: add join operator to query plan");
    auto op = LogicalOperatorFactory::createJoinOperator(joinDefinition);
    leftQueryPlan = addBinaryOperatorAndUpdateSource(op, leftQueryPlan, rightQueryPlan);
    return leftQueryPlan;
}

QueryPlanPtr QueryPlanBuilder::addBatchJoin(QueryPlanPtr leftQueryPlan,
                                            QueryPlanPtr rightQueryPlan,
                                            ExpressionNodePtr onProbeKey,
                                            ExpressionNodePtr onBuildKey) {
    NES_DEBUG("Query: joinWith the subQuery to current query");
    auto probeKeyFieldAccess = checkExpression(onProbeKey, "onProbeKey");
    auto buildKeyFieldAccess = checkExpression(onBuildKey, "onBuildKey");

    NES_ASSERT(rightQueryPlan && !rightQueryPlan->getRootOperators().empty(), "invalid rightQueryPlan query plan");
    auto rootOperatorRhs = rightQueryPlan->getRootOperators()[0];
    auto leftJoinType = leftQueryPlan->getRootOperators()[0]->getOutputSchema();
    auto rightQueryPlanJoinType = rootOperatorRhs->getOutputSchema();

    // todo here again we wan't to extend to distributed joins:
    //TODO 1,1 should be replaced once we have distributed joins with the number of child input edges
    //TODO(Ventura?>Steffen) can we know this at this query submission time?
    auto joinDefinition = Join::Experimental::LogicalBatchJoinDescriptor::create(buildKeyFieldAccess, probeKeyFieldAccess, 1, 1);

    auto op = LogicalOperatorFactory::createBatchJoinOperator(joinDefinition);
    leftQueryPlan = addBinaryOperatorAndUpdateSource(op, leftQueryPlan, rightQueryPlan);
    return leftQueryPlan;
}

QueryPlanPtr QueryPlanBuilder::addIntervalJoin(QueryPlanPtr leftQueryPlan,
                                               QueryPlanPtr rightQueryPlan,
                                               ExpressionNodePtr joinExpression,
                                               Windowing::TimeCharacteristicPtr timeCharacteristic,
                                               int64_t lowerBound,
                                               Windowing::TimeUnit lowerTimeUnit,
                                               bool lowerBoundInclusive,
                                               int64_t upperBound,
                                               Windowing::TimeUnit upperTimeUnit,
                                               bool upperBoundInclusive) {
    NES_DEBUG("Query: intervalJoinWith the subQuery to current query");

    NES_ASSERT(timeCharacteristic->getType() == Windowing::TimeCharacteristic::Type::EventTime,
               "IngestionTime is currently not supported for the intervalJoin");

    NES_ASSERT(rightQueryPlan && !rightQueryPlan->getRootOperators().empty(), "invalid rightQueryPlan query plan");

    NES_DEBUG("QueryPlanBuilder: Iterate over all ExpressionNode to check join field.");
    std::unordered_set<std::shared_ptr<BinaryExpressionNode>> visitedExpressions;
    auto bfsIterator = BreadthFirstNodeIterator(joinExpression);
    for (auto itr = bfsIterator.begin(); itr != BreadthFirstNodeIterator::end(); ++itr) {
        if ((*itr)->instanceOf<BinaryExpressionNode>()) {
            auto visitingOp = (*itr)->as<BinaryExpressionNode>();
            if (visitedExpressions.contains(visitingOp)) {
                // skip rest of the steps as the node found in already visited node list
                continue;
            } else {
                visitedExpressions.insert(visitingOp);
                auto onLeftKey = (*itr)->as<BinaryExpressionNode>()->getLeft();
                auto onRightKey = (*itr)->as<BinaryExpressionNode>()->getRight();
                NES_DEBUG("QueryPlanBuilder: Check if Expressions are FieldExpressions.");
                auto leftKeyFieldAccess = checkExpression(onLeftKey, "leftSide");
                auto rightQueryPlanKeyFieldAccess = checkExpression(onRightKey, "rightSide");
            }
        }
    }

    // check if query contain watermark assigner, and add if missing (as default behaviour)
    leftQueryPlan = assignWatermark(leftQueryPlan,
                                    Windowing::EventTimeWatermarkStrategyDescriptor::create(
                                        FieldAccessExpressionNode::create(timeCharacteristic->getField()->getName()),
                                        Windowing::TimeMeasure(0),
                                        timeCharacteristic->getTimeUnit()));

    rightQueryPlan = assignWatermark(rightQueryPlan,
                                     Windowing::EventTimeWatermarkStrategyDescriptor::create(
                                         FieldAccessExpressionNode::create(timeCharacteristic->getField()->getName()),
                                         Windowing::TimeMeasure(0),
                                         timeCharacteristic->getTimeUnit()));

    /** converting lowerBound, lowerTimeUnit and lowerBoundType to the final lower bound in milliseconds analog for upper bound
    a negative value for a bound indicates the bound being in the past*/

    // checking whether the bounds exceed the min or max value of int64
    auto lowerBoundExceedMax =
        lowerBound > std::numeric_limits<int64_t>::max() / (int64_t) lowerTimeUnit.getMillisecondsConversionMultiplier();
    auto lowerBoundExceedMin =
        lowerBound < std::numeric_limits<int64_t>::min() / (int64_t) lowerTimeUnit.getMillisecondsConversionMultiplier();
    auto upperBoundExceedMax =
        upperBound > std::numeric_limits<int64_t>::max() / (int64_t) upperTimeUnit.getMillisecondsConversionMultiplier();
    auto upperBoundExceedMin =
        upperBound < std::numeric_limits<int64_t>::min() / (int64_t) upperTimeUnit.getMillisecondsConversionMultiplier();

    // temporary assignment as a placeholder
    auto lowerBoundInMS = lowerBound;
    auto upperBoundInMS = upperBound;

    // converting the lower bound
    if (!lowerBoundExceedMax && !lowerBoundExceedMin) {
        // converting the lower bound value and timeUnit to milliseconds
        lowerBoundInMS = lowerBound * (int64_t) lowerTimeUnit.getMillisecondsConversionMultiplier();

        // adding a milliseconds to the lower bound if the bound is exclusive
        if (!lowerBoundInclusive) {
            lowerBoundInMS = lowerBoundInMS + 1;
        }
    } else if (lowerBoundExceedMax) {
        NES_INFO("lowerBound will exceed the max value of int after conversion to milliseconds from the given timeUnit so "
                 "it's set to int.max")
        lowerBoundInMS = std::numeric_limits<int64_t>::max();
    } else if (lowerBoundExceedMin) {
        NES_INFO("lowerBound will exceed the min value of int after conversion to milliseconds from the given timeUnit so "
                 "it's set to int.min")
        lowerBoundInMS = std::numeric_limits<int64_t>::min();
    } else {
        NES_DEBUG("QueryPlanBuilder: addIntervalJoin: Error in bound conversion")
    }

    // converting the upper bound
    if (!upperBoundExceedMax && !upperBoundExceedMin) {
        //converting the upper bound value and timeUnit to milliseconds
        upperBoundInMS = upperBound * (int64_t) upperTimeUnit.getMillisecondsConversionMultiplier();

        // subtracting a milliseconds to the upper bound if the bound is exclusive
        if (!upperBoundInclusive) {
            upperBoundInMS = upperBoundInMS - 1;
        }
    } else if (upperBoundExceedMax) {
        NES_INFO("upperBound will exceed the max value of int after conversion to milliseconds from the given timeUnit so "
                 "it's set to int.max")
        upperBoundInMS = std::numeric_limits<int64_t>::max();
    } else if (upperBoundExceedMin) {
        NES_INFO("upperBound will exceed the min value of int after conversion to milliseconds from the given timeUnit so "
                 "it's set to int.min")
        upperBoundInMS = std::numeric_limits<int64_t>::min();
    } else {
        NES_DEBUG("QueryPlanBuilder: addIntervalJoin: Error in bound conversion")
    }

    // check if the lower and upper bound are valid
    if (lowerBoundInMS > upperBoundInMS) {
        if (NES::Logger::getInstance()->getCurrentLogLevel() == NES::LogLevel::LOG_DEBUG) {
            NES_DEBUG("Query: IntervalJoin: the specified interval bounds are invalid");
        } else {
            NES_ERROR("Query: IntervalJoin: the specified interval bounds are invalid");
            NES_THROW_RUNTIME_ERROR("the interval bounds specified for the interval join are invalid");
        }
    }

    auto joinDefinition =
        Join::LogicalIntervalJoinDescriptor::create(joinExpression, timeCharacteristic, lowerBoundInMS, upperBoundInMS);

    NES_DEBUG("QueryPlanBuilder: add interval join operator to query plan");

    auto op = LogicalOperatorFactory::createIntervalJoinOperator(joinDefinition);
    leftQueryPlan = addBinaryOperatorAndUpdateSource(op, leftQueryPlan, rightQueryPlan);
    return leftQueryPlan;
}

QueryPlanPtr QueryPlanBuilder::addSink(QueryPlanPtr queryPlan, SinkDescriptorPtr sinkDescriptor, WorkerId workerId) {
    OperatorPtr op = LogicalOperatorFactory::createSinkOperator(sinkDescriptor, workerId);
    queryPlan->appendOperatorAsNewRoot(op);
    return queryPlan;
}

QueryPlanPtr QueryPlanBuilder::assignWatermark(QueryPlanPtr queryPlan,
                                               Windowing::WatermarkStrategyDescriptorPtr const& watermarkStrategyDescriptor) {
    OperatorPtr op = LogicalOperatorFactory::createWatermarkAssignerOperator(watermarkStrategyDescriptor);
    queryPlan->appendOperatorAsNewRoot(op);
    return queryPlan;
}

QueryPlanPtr QueryPlanBuilder::checkAndAddWatermarkAssignment(QueryPlanPtr queryPlan, const Windowing::WindowTypePtr windowType) {
    NES_DEBUG("QueryPlanBuilder: checkAndAddWatermarkAssignment for a (sub)query plan");
    auto timeBasedWindowType = windowType->as<Windowing::TimeBasedWindowType>();

    if (queryPlan->getOperatorByType<WatermarkAssignerLogicalOperator>().empty()) {
        if (timeBasedWindowType->getTimeCharacteristic()->getType() == Windowing::TimeCharacteristic::Type::IngestionTime) {
            return assignWatermark(queryPlan, Windowing::IngestionTimeWatermarkStrategyDescriptor::create());
        } else if (timeBasedWindowType->getTimeCharacteristic()->getType() == Windowing::TimeCharacteristic::Type::EventTime) {
            return assignWatermark(
                queryPlan,
                Windowing::EventTimeWatermarkStrategyDescriptor::create(
                    FieldAccessExpressionNode::create(timeBasedWindowType->getTimeCharacteristic()->getField()->getName()),
                    Windowing::TimeMeasure(0),
                    timeBasedWindowType->getTimeCharacteristic()->getTimeUnit()));
        }
    }
    return queryPlan;
}

QueryPlanPtr QueryPlanBuilder::addBinaryOperatorAndUpdateSource(OperatorPtr operatorNode,
                                                                QueryPlanPtr leftQueryPlan,
                                                                QueryPlanPtr rightQueryPlan) {
    //we add the root of the right query plan as root of the left query plan
    // thus, children of the binary join operator are
    // [0] left and [1] right
    leftQueryPlan->addRootOperator(rightQueryPlan->getRootOperators()[0]);
    leftQueryPlan->appendOperatorAsNewRoot(operatorNode);
    NES_DEBUG("QueryPlanBuilder: addBinaryOperatorAndUpdateSource: update the source names");
    auto newSourceName = Util::updateSourceName(leftQueryPlan->getSourceConsumed(), rightQueryPlan->getSourceConsumed());
    leftQueryPlan->setSourceConsumed(newSourceName);
    return leftQueryPlan;
}

std::shared_ptr<FieldAccessExpressionNode> QueryPlanBuilder::checkExpression(ExpressionNodePtr expression, std::string side) {
    if (!expression->instanceOf<FieldAccessExpressionNode>()) {
        NES_ERROR("QueryPlanBuilder: window key ({}) has to be an FieldAccessExpression but it was a  {}",
                  side,
                  expression->toString());
        NES_THROW_RUNTIME_ERROR("QueryPlanBuilder: window key has to be an FieldAccessExpression");
    }
    return expression->as<FieldAccessExpressionNode>();
}
}// namespace NES
