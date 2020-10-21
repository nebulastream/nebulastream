
#include <API/Expressions/Expressions.hpp>
#include <API/Query.hpp>
#include <Nodes/Expressions/ExpressionNode.hpp>
#include <Nodes/Expressions/FieldAssignmentExpressionNode.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/LogicalStreamSourceDescriptor.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/UtilityFunctions.hpp>
#include <Windowing/DistributionCharacteristic.hpp>
#include <Windowing/LogicalWindowDefinition.hpp>
#include <iostream>

namespace NES {

Query::Query(QueryPlanPtr queryPlan) : queryPlan(queryPlan) {}

Query::Query(const Query& query)
    : queryPlan(query.queryPlan) {
}

Query Query::from(const std::string sourceStreamName) {
    NES_DEBUG("Query: create query for input stream " << sourceStreamName);
    auto sourceOperator = LogicalOperatorFactory::createSourceOperator(LogicalStreamSourceDescriptor::create(sourceStreamName));
    auto queryPlan = QueryPlan::create(sourceOperator);
    return Query(queryPlan);
}

Query& Query::merge(Query* subQuery) {
    NES_DEBUG("Query: merge the subQuery to current query");
    OperatorNodePtr op = LogicalOperatorFactory::createMergeOperator();
    op->setId(UtilityFunctions::getNextOperatorId());
    queryPlan->addRootOperator(subQuery->getQueryPlan()->getRootOperators()[0]);
    queryPlan->appendOperatorAsNewRoot(op);
    return *this;
}

Query& Query::window(const Windowing::WindowTypePtr, const Windowing::WindowAggregationPtr) {
    NES_THROW_RUNTIME_ERROR("Query: Global windowing is currently not supported");
    /*
    NES_DEBUG("Query: add window operator");
    auto windowDefinition = LogicalWindowDefinition::create(aggregation, windowType, DistributionCharacteristic::createCompleteWindowType());
    auto windowOperator = LogicalOperatorFactory::createWindowOperator(windowDefinition);
    windowOperator->setId(UtilityFunctions::getNextOperatorId());
    queryPlan->appendOperatorAsNewRoot(windowOperator);
    return *this;*/
}

Query& Query::windowByKey(ExpressionItem onKey, const Windowing::WindowTypePtr windowType, const Windowing::WindowAggregationPtr aggregation) {
    NES_DEBUG("Query: add keyed window operator");
    auto keyExpression = onKey.getExpressionNode();
    if (!keyExpression->instanceOf<FieldAccessExpressionNode>()) {
        NES_ERROR("Query: window key has to be an FieldAccessExpression but it was a " + keyExpression->toString());
    }
    auto fieldAccess = keyExpression->as<FieldAccessExpressionNode>();
    auto windowDefinition = Windowing::LogicalWindowDefinition::create(fieldAccess, aggregation, windowType, Windowing::DistributionCharacteristic::createCompleteWindowType(), 1);
    auto windowOperator = LogicalOperatorFactory::createWindowOperator(windowDefinition);
    windowOperator->setId(UtilityFunctions::getNextOperatorId());
    queryPlan->appendOperatorAsNewRoot(windowOperator);
    return *this;
}

Query& Query::filter(const ExpressionNodePtr filterExpression) {
    NES_DEBUG("Query: add filter operator to query");
    OperatorNodePtr op = LogicalOperatorFactory::createFilterOperator(filterExpression);
    op->setId(UtilityFunctions::getNextOperatorId());
    queryPlan->appendOperatorAsNewRoot(op);
    return *this;
}

Query& Query::map(const FieldAssignmentExpressionNodePtr mapExpression) {
    NES_DEBUG("Query: add map operator to query");
    OperatorNodePtr op = LogicalOperatorFactory::createMapOperator(mapExpression);
    op->setId(UtilityFunctions::getNextOperatorId());
    queryPlan->appendOperatorAsNewRoot(op);
    return *this;
}

Query& Query::sink(const SinkDescriptorPtr sinkDescriptor) {
    NES_DEBUG("Query: add sink operator to query");
    OperatorNodePtr op = LogicalOperatorFactory::createSinkOperator(sinkDescriptor);
    op->setId(UtilityFunctions::getNextOperatorId());
    queryPlan->appendOperatorAsNewRoot(op);
    return *this;
}

QueryPlanPtr Query::getQueryPlan() {
    return queryPlan;
}

}// namespace NES
