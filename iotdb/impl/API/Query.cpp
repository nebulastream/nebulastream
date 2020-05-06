#include <API/Query.hpp>
#include <Catalogs/StreamCatalog.hpp>
#include <Nodes/Expressions/FieldAssignmentExpressionNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/LogicalStreamSourceDescriptor.hpp>
#include <Nodes/Operators/QueryPlan.hpp>
#include <Util/Logger.hpp>
#include <cstddef>
#include <iostream>

namespace NES {

Query::Query(QueryPlanPtr queryPlan) : queryPlan(queryPlan) {}

Query::Query(const Query& query)
    : queryPlan(query.queryPlan) {}

Query Query::from(Stream& stream) {

    auto streamPtr = std::make_shared<Stream>(stream);
    auto sourceOperator = createSourceLogicalOperatorNode(LogicalStreamSourceDescriptor::create(stream.getName()));
    QueryPlanPtr queryPlan = QueryPlan::create(sourceOperator, streamPtr);
    Query query(queryPlan);
    return query;
}

/*
 * Relational Operators
 */

Query& Query::filter(const ExpressionNodePtr filterExpression) {
    OperatorNodePtr op = createFilterLogicalOperatorNode(filterExpression);
    queryPlan->appendOperator(op);
    return *this;
}

Query& Query::map(const FieldAssignmentExpressionNodePtr mapExpression) {
    OperatorNodePtr op = createMapLogicalOperatorNode(mapExpression);
    queryPlan->appendOperator(op);
    return *this;
}

Query& Query::combine(const Query& subQuery) { NES_NOT_IMPLEMENTED(); }

Query& Query::join(const Query& subQuery, const JoinPredicatePtr joinPred) { NES_NOT_IMPLEMENTED(); }

Query& Query::windowByKey() { NES_NOT_IMPLEMENTED(); }

Query& Query::window() { NES_NOT_IMPLEMENTED(); }

Query& Query::to(const std::string& name) { NES_NOT_IMPLEMENTED(); }

Query& Query::sink(const SinkDescriptorPtr sinkDescriptor) {
    OperatorNodePtr op = createSinkLogicalOperatorNode(sinkDescriptor);
    queryPlan->appendOperator(op);
    return *this;
}

QueryPlanPtr Query::getQueryPlan() {
    return queryPlan;
}

std::string Query::toString() {
    stringstream ss;
    ss << queryPlan->toString();
    return ss.str();
}

}// namespace NES
