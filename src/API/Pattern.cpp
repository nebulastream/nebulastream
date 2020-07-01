#include <API/Pattern.hpp>
#include <Nodes/Expressions/FieldAssignmentExpressionNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/LogicalStreamSourceDescriptor.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <iostream>


namespace NES {

Pattern::Pattern(const Pattern& query) : Query (query)  {

}

Pattern::Pattern(QueryPlanPtr queryplan) :Query(queryPlan){}

Pattern Pattern::from(const std::string sourceStreamName) {
    NES_DEBUG("Pattern: create query for input stream " << sourceStreamName);
    auto sourceOperator = createSourceLogicalOperatorNode(LogicalStreamSourceDescriptor::create(sourceStreamName));
    auto queryPlan = QueryPlan::create(sourceStreamName, sourceOperator);
    //TODO queryPlan.isCEP(true)
    return Pattern(queryPlan);
}

Pattern& Pattern::filter(const ExpressionNodePtr filterExpression) {
    NES_DEBUG("Pattern: add filter operator to query");
    OperatorNodePtr op = createFilterLogicalOperatorNode(filterExpression);
    queryPlan->appendOperator(op);
    return *this;
}

Pattern& Pattern::map(const FieldAssignmentExpressionNodePtr mapExpression) {
    NES_DEBUG("Pattern: add map operator to query");
    OperatorNodePtr op = createMapLogicalOperatorNode(mapExpression);
    queryPlan->appendOperator(op);
    return *this;
}

Pattern& Pattern::sink(const SinkDescriptorPtr sinkDescriptor) {
    NES_DEBUG("Pattern: add sink operator to query");
    OperatorNodePtr op = createSinkLogicalOperatorNode(sinkDescriptor);
    queryPlan->appendOperator(op);
    return *this;
}

QueryPlanPtr Pattern::getQueryPlan() {
    return queryPlan;
}
const std::string& Pattern::getPatternName() const {
    return patternName;
}
void Pattern::setPatternName(const std::string& patternName) {
    Pattern::patternName = patternName;
}

}// namespace NES
