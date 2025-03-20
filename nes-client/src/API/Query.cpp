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
#include <API/Expressions/Expressions.hpp>
#include <API/Expressions/LogicalExpressions.hpp>
#include <API/Query.hpp>
#include <API/WindowedQuery.hpp>
#include <API/Windowing.hpp>
#include <Common/ValueTypes/ValueType.hpp>
#include <Expressions/FieldAssignmentExpressionNode.hpp>
#include <Expressions/FieldRenameExpressionNode.hpp>
#include <Expressions/LogicalExpressions/EqualsExpressionNode.hpp>
#include <Measures/TimeCharacteristic.hpp>
#include <Operators/LogicalOperators/LogicalBinaryOperator.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/LogicalOperators/Watermarks/EventTimeWatermarkStrategyDescriptor.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Query/QueryPlanBuilder.hpp>
#include <Types/TimeBasedWindowType.hpp>
#include <Util/Logger/Logger.hpp>
#include <regex>
#include <utility>

namespace NES {

ExpressionNodePtr getExpressionNodePtr(ExpressionItem& expressionItem) { return expressionItem.getExpressionNode(); }

JoinOperatorBuilder::Join Query::joinWith(const Query& subQueryRhs) { return JoinOperatorBuilder::Join(subQueryRhs, *this); }

NES::Experimental::BatchJoinOperatorBuilder::Join Query::batchJoinWith(const Query& subQueryRhs) {
    return NES::Experimental::BatchJoinOperatorBuilder::Join(subQueryRhs, *this);
}

CrossJoinOperatorBuilder::CrossJoin Query::crossJoinWith(const Query& subQueryRhs) {
    return CrossJoinOperatorBuilder::CrossJoin(subQueryRhs, *this);
}

IntervalJoinOperatorBuilder::IntervalJoin Query::intervalJoinWith(const Query& subQueryRhs) {
    return IntervalJoinOperatorBuilder::IntervalJoin(subQueryRhs, *this);
}

CEPOperatorBuilder::And Query::andWith(const Query& subQueryRhs) { return CEPOperatorBuilder::And(subQueryRhs, *this); }

CEPOperatorBuilder::Seq Query::seqWith(const Query& subQueryRhs) { return CEPOperatorBuilder::Seq(subQueryRhs, *this); }

CEPOperatorBuilder::Times Query::times(const uint64_t minOccurrences, const uint64_t maxOccurrences) {
    return CEPOperatorBuilder::Times(minOccurrences, maxOccurrences, *this);
}

CEPOperatorBuilder::Times Query::times(const uint64_t occurrences) { return CEPOperatorBuilder::Times(occurrences, *this); }

CEPOperatorBuilder::Times Query::times() { return CEPOperatorBuilder::Times(*this); }

namespace JoinOperatorBuilder {

JoinWhere Join::where(ExpressionNodePtr joinExpression) const { return JoinWhere(subQueryRhs, originalQuery, joinExpression); }

Join::Join(const Query& subQueryRhs, Query& originalQuery) : subQueryRhs(subQueryRhs), originalQuery(originalQuery) {}

JoinWhere::JoinWhere(const Query& subQueryRhs, Query& originalQuery, ExpressionNodePtr joinExpression)
    : subQueryRhs(subQueryRhs), originalQuery(originalQuery), joinExpressions(joinExpression) {}

Query& JoinWhere::window(const Windowing::WindowTypePtr& windowType) const {
    return originalQuery.joinWith(subQueryRhs, joinExpressions, windowType);//call original joinWith() function
}

}// namespace JoinOperatorBuilder

namespace Experimental::BatchJoinOperatorBuilder {

Join::Join(const Query& subQueryRhs, Query& originalQuery) : subQueryRhs(subQueryRhs), originalQuery(originalQuery) {}

Query& Join::where(const ExpressionNodePtr joinExpression) const {
    return originalQuery.batchJoinWith(subQueryRhs, joinExpression);
}

}// namespace Experimental::BatchJoinOperatorBuilder

namespace CrossJoinOperatorBuilder {

CrossJoin::CrossJoin(const Query& subQueryRhs, Query& subQueryLhs)
    : subQueryRhs(const_cast<Query&>(subQueryRhs)), subQueryLhs(subQueryLhs) {
    NES_DEBUG("Query: add map operator to crossJoin to add virtual key to originalQuery");
    //here, we add artificial key attributes to the sources in order to reuse the join-logic later
    // That is a quick fix cause the depencies between Operator and Nodes currently prevent both, unary expressions as well as no join expression
    auto leftKey = "leftKey";
    auto rightKey = "rightKey";
    //next: map the attributes with value 1 to the left and right source
    this->subQueryLhs.map(Attribute(leftKey) = 1);
    this->subQueryRhs.map(Attribute(rightKey) = 1);
    joinExpressions =
        ExpressionItem(Attribute(leftKey)).getExpressionNode() == ExpressionItem(Attribute(rightKey)).getExpressionNode();
}

Query& CrossJoin::window(const Windowing::WindowTypePtr& windowType) const {
    return subQueryLhs.joinWith(subQueryRhs, joinExpressions, windowType);
}
}// namespace CrossJoinOperatorBuilder

namespace IntervalJoinOperatorBuilder {

JoinWhere IntervalJoin::where(ExpressionNodePtr joinExpression) const {
    return JoinWhere(subQueryRhs, originalQuery, joinExpression);
}

IntervalJoin::IntervalJoin(const Query& subQueryRhs, Query& originalQuery)
    : subQueryRhs(subQueryRhs), originalQuery(originalQuery) {}

JoinWhere::JoinWhere(const Query& subQueryRhs, Query& originalQuery, ExpressionNodePtr joinExpression)
    : subQueryRhs(subQueryRhs), originalQuery(originalQuery), joinExpression(joinExpression) {}

JoinLowerBound JoinWhere::lowerBound(Windowing::TimeCharacteristicPtr timeCharacteristic,
                                     int64_t lowerBound,
                                     NES::Windowing::TimeUnit lowerTimeUnit,
                                     bool lowerBoundInclusive) const {
    return JoinLowerBound(subQueryRhs,
                          originalQuery,
                          joinExpression,
                          timeCharacteristic,
                          lowerBound,
                          lowerTimeUnit,
                          lowerBoundInclusive);
}

JoinLowerBound::JoinLowerBound(const NES::Query& subQueryRhs,
                               NES::Query& originalQuery,
                               NES::ExpressionNodePtr joinExpression,
                               Windowing::TimeCharacteristicPtr timeCharacteristic,
                               int64_t lowerBound,
                               NES::Windowing::TimeUnit lowerTimeUnit,
                               bool lowerBoundInclusive)
    : subQueryRhs(subQueryRhs), originalQuery(originalQuery), joinExpression(joinExpression),
      timeCharacteristic(timeCharacteristic), lowerBound(lowerBound), lowerTimeUnit(lowerTimeUnit),
      lowerBoundInclusive(lowerBoundInclusive) {}

Query& JoinLowerBound::upperBound(int64_t upperBound, NES::Windowing::TimeUnit upperTimeUnit, bool upperBoundInclusive) const {
    return originalQuery.intervalJoinWith(subQueryRhs,
                                          joinExpression,
                                          timeCharacteristic,
                                          lowerBound,
                                          lowerTimeUnit,
                                          lowerBoundInclusive,
                                          upperBound,
                                          upperTimeUnit,
                                          upperBoundInclusive);
}

}// namespace IntervalJoinOperatorBuilder

namespace CEPOperatorBuilder {

And::And(const Query& subQueryRhs, Query& originalQuery)
    : subQueryRhs(const_cast<Query&>(subQueryRhs)), originalQuery(originalQuery) {
    NES_DEBUG("Query: add map operator to andWith to add virtual key to originalQuery");
    //here, we add artificial key attributes to the sources in order to reuse the join-logic later
    auto cepLeftKey = "cep_leftKey";
    auto cepRightKey = "cep_rightKey";
    //next: map the attributes with value 1 to the left and right source
    originalQuery.map(Attribute(cepLeftKey) = 1);
    this->subQueryRhs.map(Attribute(cepRightKey) = 1);
    //last, define the artificial attributes as key attributes
    NES_DEBUG("Query: add name cepLeftKey {}", cepLeftKey);
    NES_DEBUG("Query: add name cepRightKey {}", cepRightKey);
    joinExpression =
        ExpressionItem(Attribute(cepLeftKey)).getExpressionNode() == ExpressionItem(Attribute(cepRightKey)).getExpressionNode();
}

Query& And::window(const Windowing::WindowTypePtr& windowType) const {
    return originalQuery.andWith(subQueryRhs, joinExpression, windowType);//call original andWith() function
}

Seq::Seq(const Query& subQueryRhs, Query& originalQuery)
    : subQueryRhs(const_cast<Query&>(subQueryRhs)), originalQuery(originalQuery) {
    NES_DEBUG("Query: add map operator to seqWith to add virtual key to originalQuery");
    //here, we add artificial key attributes to the sources in order to reuse the join-logic later
    auto cepLeftKey = "cep_leftKey";
    auto cepRightKey = "cep_rightKey";
    //next: map the attributes with value 1 to the left and right source
    originalQuery.map(Attribute(cepLeftKey) = 1);
    this->subQueryRhs.map(Attribute(cepRightKey) = 1);
    //last, define the artificial attributes as key attributes
    joinExpression =
        ExpressionItem(Attribute(cepLeftKey)).getExpressionNode() == ExpressionItem(Attribute(cepRightKey)).getExpressionNode();
}

Query& Seq::window(const Windowing::WindowTypePtr& windowType) const {
    NES_DEBUG("Sequence enters window function");
    auto timestamp = windowType->as<Windowing::TimeBasedWindowType>()
                         ->getTimeCharacteristic()
                         ->getField()
                         ->getName();// assume time-based windows
    std::string sourceNameLeft = originalQuery.getQueryPlan()->getSourceConsumed();
    std::string sourceNameRight = subQueryRhs.getQueryPlan()->getSourceConsumed();
    std::regex r1("-_+");
    sourceNameRight = std::regex_replace(sourceNameRight, r1, "");
    sourceNameLeft = std::regex_replace(sourceNameLeft, r1, "");
    // to guarantee a correct order of events by time (sequence) we need to identify the correct source and its timestamp
    // in case of composed streams on the right branch
    if (sourceNameRight.find("_") != std::string::npos) {
        // we find the most left source and use its timestamp for the filter constraint
        uint64_t posStart = sourceNameRight.find("_");
        uint64_t posEnd = sourceNameRight.find("_", posStart + 1);
        sourceNameRight = sourceNameRight.substr(posStart + 1, posEnd - 2) + "$" + timestamp;
    }// in case the right branch only contains 1 source we can just use it
    else {
        sourceNameRight = sourceNameRight + "$" + timestamp;
    }
    // in case of composed sources on the left branch
    if (sourceNameLeft.find("_") != std::string::npos) {
        // we find the most right source and use its timestamp for the filter constraint
        uint64_t posStart = sourceNameLeft.find_first_of("_");
        sourceNameLeft = sourceNameLeft.substr(0, posStart) + "$" + timestamp;
    }// in case the left branch only contains 1 source we can just use it
    else {
        sourceNameLeft = sourceNameLeft + "$" + timestamp;
    }
    NES_DEBUG("ExpressionItem for Left Source {}", sourceNameLeft);
    NES_DEBUG("ExpressionItem for Right Source {}", sourceNameRight);
    return originalQuery.seqWith(subQueryRhs, joinExpression, windowType)
        .filter(Attribute(sourceNameLeft) < Attribute(sourceNameRight));//call original seqWith() function
}

Times::Times(const uint64_t minOccurrences, const uint64_t maxOccurrences, Query& originalQuery)
    : originalQuery(originalQuery), minOccurrences(minOccurrences), maxOccurrences(maxOccurrences), bounded(true) {
    // add a new count attribute to the schema which is later used to derive the number of occurrences
    originalQuery.map(Attribute("Count") = 1);
}

Times::Times(const uint64_t occurrences, Query& originalQuery)
    : originalQuery(originalQuery), minOccurrences(0), maxOccurrences(occurrences), bounded(true) {
    // add a new count attribute to the schema which is later used to derive the number of occurrences
    originalQuery.map(Attribute("Count") = 1);
}

Times::Times(Query& originalQuery) : originalQuery(originalQuery), minOccurrences(0), maxOccurrences(0), bounded(false) {
    // add a new count attribute to the schema which is later used to derive the number of occurrences
    originalQuery.map(Attribute("Count") = 1);
}

Query& Times::window(const Windowing::WindowTypePtr& windowType) const {
    auto timestamp = windowType->as<Windowing::TimeBasedWindowType>()->getTimeCharacteristic()->getField()->getName();
    // if no min and max occurrence is defined, apply count without filter
    if (!bounded) {
        return originalQuery.window(windowType).apply(API::Sum(Attribute("Count")), API::Max(Attribute(timestamp)));
    } else {
        // if user passed 0 occurrences which is not wanted
        if ((minOccurrences == 0) && (maxOccurrences == 0)) {
            NES_THROW_RUNTIME_ERROR("Number of occurrences must be at least 1.");
        }
        // if min and/or max occurrence are defined, apply count without filter
        if (maxOccurrences == 0) {
            return originalQuery.window(windowType)
                .apply(API::Sum(Attribute("Count")), API::Max(Attribute(timestamp)))
                .filter(Attribute("Count") >= minOccurrences);
        }

        if (minOccurrences == 0) {
            return originalQuery.window(windowType)
                .apply(API::Sum(Attribute("Count")), API::Max(Attribute(timestamp)))
                .filter(Attribute("Count") == maxOccurrences);
        }

        return originalQuery.window(windowType)
            .apply(API::Sum(Attribute("Count")), API::Max(Attribute(timestamp)))
            .filter(Attribute("Count") >= minOccurrences && Attribute("Count") <= maxOccurrences);
    }

    return originalQuery;
}

}// namespace CEPOperatorBuilder

Query::Query(QueryPlanPtr queryPlan) : queryPlan(std::move(queryPlan)) {}

Query::Query(const Query& query) = default;

Query Query::from(const std::string& sourceName) {
    NES_DEBUG("Query: create new Query with source {}", sourceName);
    auto queryPlan = QueryPlanBuilder::createQueryPlan(sourceName);
    return Query(queryPlan);
}

Query& Query::buildStatistic(Windowing::WindowTypePtr window,
                             Statistic::WindowStatisticDescriptorPtr statisticDescriptor,
                             Statistic::StatisticMetricHash metricHash,
                             Statistic::SendingPolicyPtr sendingPolicy,
                             Statistic::TriggerConditionPtr triggerCondition) {
    this->queryPlan = QueryPlanBuilder::addStatisticBuildOperator(std::move(window),
                                                                  std::move(statisticDescriptor),
                                                                  metricHash,
                                                                  sendingPolicy,
                                                                  triggerCondition,
                                                                  this->queryPlan);
    return *this;
}

Query& Query::project(std::vector<ExpressionNodePtr> expressions) {
    NES_DEBUG("Query: add projection to query");
    this->queryPlan = QueryPlanBuilder::addProjection(expressions, this->queryPlan);
    return *this;
}

Query& Query::as(const std::string& newSourceName) {
    NES_DEBUG("Query: add rename operator to query");
    this->queryPlan = QueryPlanBuilder::addRename(newSourceName, this->queryPlan);
    return *this;
}

Query& Query::unionWith(const Query& subQuery) {
    NES_DEBUG("Query: unionWith the subQuery to current query");
    this->queryPlan = QueryPlanBuilder::addUnion(this->queryPlan, subQuery.getQueryPlan());
    return *this;
}

Query& Query::unionWith2(const std::vector<QueryPlanPtr>& subQueries) {
    NES_DEBUG("Query: unionWith the subQuery to current query");
    this->queryPlan = QueryPlanBuilder::addUnion( this->queryPlan, subQueries);
    return *this;
}

Query& Query::joinWith(const Query& subQueryRhs, ExpressionNodePtr joinExpression, const Windowing::WindowTypePtr& windowType) {
    Join::LogicalJoinDescriptor::JoinType joinType = identifyJoinType(joinExpression);
    auto newFilter = QueryPlanBuilder::findFilter(joinExpression);
    if (newFilter->toString() != joinExpression->toString()) {
        this->queryPlan = QueryPlanBuilder::addFilter(newFilter, this->queryPlan);
    }
    this->queryPlan =
        QueryPlanBuilder::addJoin(this->queryPlan, subQueryRhs.getQueryPlan(), joinExpression, windowType, joinType);
    return *this;
}

Query& Query::batchJoinWith(const Query& subQueryRhs, ExpressionNodePtr joinExpression) {
    NES_DEBUG("Query: add Batch Join Operator to Query");
    if (joinExpression->as<EqualsExpressionNode>()) {
        auto onProbeKey = joinExpression->as<BinaryExpressionNode>()->getLeft();
        auto onBuildKey = joinExpression->as<BinaryExpressionNode>()->getRight();

        this->queryPlan = QueryPlanBuilder::addBatchJoin(this->queryPlan, subQueryRhs.getQueryPlan(), onProbeKey, onBuildKey);
    } else {
        NES_THROW_RUNTIME_ERROR("Query:joinExpression has to be a EqualsExpressionNode");
    }
    return *this;
}

Query& Query::intervalJoinWith(const NES::Query& subQueryRhs,
                               NES::ExpressionNodePtr joinExpression,
                               Windowing::TimeCharacteristicPtr timeCharacteristic,
                               int64_t lowerBound,
                               Windowing::TimeUnit lowerTimeUnit,
                               bool lowerBoundInclusive,
                               int64_t upperBound,
                               Windowing::TimeUnit upperTimeUnit,
                               bool upperBoundInclusive) {
    NES_DEBUG("Query: add Interval Join Operator to Query");

    this->queryPlan = QueryPlanBuilder::addIntervalJoin(this->queryPlan,
                                                        subQueryRhs.getQueryPlan(),
                                                        joinExpression,
                                                        timeCharacteristic,
                                                        lowerBound,
                                                        lowerTimeUnit,
                                                        lowerBoundInclusive,
                                                        upperBound,
                                                        upperTimeUnit,
                                                        upperBoundInclusive);
    return *this;
}

Query& Query::andWith(const Query& subQueryRhs, ExpressionNodePtr joinExpression, const Windowing::WindowTypePtr& windowType) {
    Join::LogicalJoinDescriptor::JoinType joinType = identifyJoinType(joinExpression);
    this->queryPlan =
        QueryPlanBuilder::addJoin(this->queryPlan, subQueryRhs.getQueryPlan(), joinExpression, windowType, joinType);
    return *this;
}

Query& Query::seqWith(const Query& subQueryRhs, ExpressionNodePtr joinExpression, const Windowing::WindowTypePtr& windowType) {
    Join::LogicalJoinDescriptor::JoinType joinType = identifyJoinType(joinExpression);
    this->queryPlan =
        QueryPlanBuilder::addJoin(this->queryPlan, subQueryRhs.getQueryPlan(), joinExpression, windowType, joinType);
    return *this;
}

Query& Query::orWith(const Query& subQueryRhs) {
    NES_DEBUG("Query: finally we translate the OR into a union OP ");
    this->queryPlan = QueryPlanBuilder::addUnion(this->queryPlan, subQueryRhs.getQueryPlan());
    return *this;
}

Query& Query::filter(const ExpressionNodePtr& filterExpression) {
    NES_DEBUG("Query: add filter operator to query");
    this->queryPlan = QueryPlanBuilder::addFilter(filterExpression, this->queryPlan);
    return *this;
}

Query& Query::limit(const uint64_t limit) {
    NES_DEBUG("Query: add limit operator to query");
    this->queryPlan = QueryPlanBuilder::addLimit(limit, this->queryPlan);
    return *this;
}

Query& Query::mapUDF(const Catalogs::UDF::UDFDescriptorPtr& descriptor) {
    NES_DEBUG("Query: add map java udf operator to query");
    this->queryPlan = QueryPlanBuilder::addMapUDF(descriptor, this->queryPlan);
    return *this;
}

Query& Query::flatMapUDF(const Catalogs::UDF::UDFDescriptorPtr& descriptor) {
    NES_DEBUG("Query: add flat map java udf operator to query");
    this->queryPlan = QueryPlanBuilder::addFlatMapUDF(descriptor, this->queryPlan);
    return *this;
}

Query& Query::map(const FieldAssignmentExpressionNodePtr& mapExpression) {
    NES_DEBUG("Query: add map operator to query");
    this->queryPlan = QueryPlanBuilder::addMap(mapExpression, this->queryPlan);
    return *this;
}

Query& Query::inferModel(const std::string model,
                         const std::initializer_list<ExpressionItem> inputFields,
                         const std::initializer_list<ExpressionItem> outputFields) {
    NES_DEBUG("Query: add map inferModel to query");
    auto inputFieldVector = std::vector(inputFields);
    auto outputFieldVector = std::vector(outputFields);
    std::vector<ExpressionNodePtr> inputFieldsPtr;
    std::vector<ExpressionNodePtr> outputFieldsPtr;
    for (const auto& inputField : inputFieldVector) {
        inputFieldsPtr.push_back(inputField.getExpressionNode());
    }
    for (const auto& outputField : outputFieldVector) {
        outputFieldsPtr.push_back(outputField.getExpressionNode());
    }

    OperatorPtr op = LogicalOperatorFactory::createInferModelOperator(model, inputFieldsPtr, outputFieldsPtr);
    NES_DEBUG("Query::inferModel: Current Operator: {}", op->toString());
    queryPlan->appendOperatorAsNewRoot(op);
    return *this;
}

Query& Query::sink(const SinkDescriptorPtr sinkDescriptor, WorkerId workerId) {
    NES_DEBUG("Query: add sink operator to query");
    this->queryPlan = QueryPlanBuilder::addSink(this->queryPlan, sinkDescriptor, workerId);
    return *this;
}

Query Query::sink2(const SinkDescriptorPtr sinkDescriptor, const std::vector<Query>& subQueries, WorkerId workerId) {
    NES_DEBUG("Query: add sink operator to query");
    auto queryPlan = QueryPlan::create();

    OperatorPtr op = LogicalOperatorFactory::createSinkOperator(sinkDescriptor, workerId);
    for (auto& subQuery : subQueries) {
        for (auto& rootOperator : subQuery.getQueryPlan()->getRootOperators()) {
            op->addChild(rootOperator);
        }
    }
    queryPlan->appendOperatorAsNewRoot(op);
    return {queryPlan};
}

Query& Query::assignWatermark(const Windowing::WatermarkStrategyDescriptorPtr& watermarkStrategyDescriptor) {
    NES_DEBUG("Query: add assignWatermark operator to query");
    this->queryPlan = QueryPlanBuilder::assignWatermark(this->queryPlan, watermarkStrategyDescriptor);
    return *this;
}

QueryPlanPtr Query::getQueryPlan() const { return queryPlan; }

Join::LogicalJoinDescriptor::JoinType Query::identifyJoinType(ExpressionNodePtr joinExpression) {
    NES_DEBUG("Query: identify Join Type; default: CARTESIAN PRODUCT");
    auto joinType = Join::LogicalJoinDescriptor::JoinType::CARTESIAN_PRODUCT;
    NES_DEBUG("Query: Iterate over all ExpressionNode to check join field.");
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
                if ((*itr)->instanceOf<EqualsExpressionNode>()) {
                    NES_DEBUG("Query: identify Join Type: INNER JOIN");
                    joinType = Join::LogicalJoinDescriptor::JoinType::INNER_JOIN;
                    break;
                }
            }
        }
    }
    return joinType;
}

}// namespace NES
