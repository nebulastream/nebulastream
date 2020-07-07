#include <API/Pattern.hpp>
#include <Nodes/Expressions/FieldAssignmentExpressionNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/LogicalStreamSourceDescriptor.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <iostream>
#include <utility>

namespace NES {

Pattern::Pattern(const Pattern& query) : Query(query.queryPlan) {
    NES_DEBUG("Pattern: copy constructor: handover Pattern to Query");
}

Pattern::Pattern(QueryPlanPtr queryPlan) : Query(queryPlan) {
    NES_DEBUG("Pattern: copy constructor: handover Pattern to Query");
}

Pattern Pattern::from(const std::string sourceStreamName) {
    NES_DEBUG("Pattern: create query for input stream " << sourceStreamName);
    auto sourceOperator = createSourceLogicalOperatorNode(LogicalStreamSourceDescriptor::create(sourceStreamName));
    auto queryPlan = QueryPlan::create(sourceOperator);
    return Pattern(queryPlan);
}

//TODO: add map function before sink
/*Pattern& Pattern::sink(const SinkDescriptorPtr sinkDescriptor) {
    NES_DEBUG("Pattern: add sink operator to query");
    OperatorNodePtr op = createSinkLogicalOperatorNode(sinkDescriptor);
    queryPlan->appendOperator(op);
    return *this;
}*/

const std::string& Pattern::getPatternName() const {
    return patternName;
}
void Pattern::setPatternName(const std::string& patternName) {
    this->patternName = patternName;
}

}// namespace NES
