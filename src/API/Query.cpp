#include <API/Query.hpp>
#include <Nodes/Expressions/FieldAssignmentExpressionNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/LogicalStreamSourceDescriptor.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <iostream>

namespace NES {

Query::Query(QueryPlanPtr queryPlan) : queryPlan(queryPlan) {}

Query::Query(const Query& query)
    : queryPlan(query.queryPlan) {
}

Query Query::from(const std::string sourceStreamName) {
    NES_DEBUG("Query: create query for input stream " << sourceStreamName);
    auto sourceOperator = createSourceLogicalOperatorNode(LogicalStreamSourceDescriptor::create(sourceStreamName));
    auto queryPlan = QueryPlan::create(sourceOperator);
    return Query(queryPlan);
}

Query& Query::merge(Query* subQuery) {
    NES_DEBUG("Query: merge the subQuery to current query");
    OperatorNodePtr op = createMergeLogicalOperatorNode();
    getQueryPlan()->addRootOperator(subQuery->getQueryPlan()->getRootOperators()[0]);
    queryPlan->appendOperator(op);
    return *this;
}

Query& Query::filter(const ExpressionNodePtr filterExpression) {
    NES_DEBUG("Query: add filter operator to query");
    OperatorNodePtr op = createFilterLogicalOperatorNode(filterExpression);
    queryPlan->appendOperator(op);
    return *this;
}

Query& Query::map(const FieldAssignmentExpressionNodePtr mapExpression) {
    NES_DEBUG("Query: add map operator to query");
    OperatorNodePtr op = createMapLogicalOperatorNode(mapExpression);
    queryPlan->appendOperator(op);
    return *this;
}

Query& Query::sink(const SinkDescriptorPtr sinkDescriptor) {
    NES_DEBUG("Query: add sink operator to query");
    OperatorNodePtr op = createSinkLogicalOperatorNode(sinkDescriptor);
    queryPlan->appendOperator(op);
    return *this;
}

QueryPlanPtr Query::getQueryPlan() {
    return queryPlan;
}

const std::string& Query::getQueryName() const {
    return queryName;
}
void Query::setQueryName(const std::string& queryName) {
    this->queryName = queryName;
}

}// namespace NES
