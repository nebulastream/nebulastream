#ifndef LOGICAL_OPERATOR_NODE_HPP
#define LOGICAL_OPERATOR_NODE_HPP

#include <OperatorNodes/Node.hpp>
#include <API/Window/WindowDefinition.hpp>
#include <API/ParameterTypes.hpp>
#include <API/Schema.hpp>

namespace NES {

class LogicalOperatorNode : public Node {
  public:
    LogicalOperatorNode();

    virtual bool equals(const Node&) {};
    virtual bool equal(const NodePtr& rhs) const { return false; };
    virtual bool operator==(const Node&) {};
  private:
    Schema inputSchema;
    Schema outputSchema;
};

typedef std::shared_ptr<LogicalOperatorNode> LogicalOperatorNodePtr;

const NodePtr createAggregationLogicalOperatorNode(const AggregationSpec& aggrSpec);
NodePtr createFilterLogicalOperatorNode(const PredicatePtr& predicate);
const NodePtr createJoinLogicalOperatorNode(const JoinPredicatePtr& joinSpec);
const NodePtr createKeyByLogicalOperatorNode(const Attributes& keybySpec);
const NodePtr createMapLogicalOperatorNode(const AttributeFieldPtr&, const PredicatePtr&);
const NodePtr createSinkLogicalOperatorNode(const DataSinkPtr& sink);
const NodePtr createSourceLogicalOperatorNode(const DataSourcePtr& source);
const NodePtr createSortLogicalOperatorNode(const Sort& sortSpec);
const NodePtr createWindowLogicalOperatorNode(const WindowDefinitionPtr& windowDefinition);


// #include <OperatorNodes/Impl/FilterLogicalOperatorNode.hpp>
// #include <OperatorNodes/Impl/SourceLogicalOperatorNode.hpp>
}

#endif  // LOGICAL_OPERATOR_NODE_HPP
