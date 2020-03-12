#ifndef LOGICAL_OPERATOR_NODE_HPP
#define LOGICAL_OPERATOR_NODE_HPP

#include <Nodes/Operators/OperatorNode.hpp>
#include <API/ParameterTypes.hpp>
namespace NES {

class LogicalOperatorNode : public OperatorNode {
  public:
    LogicalOperatorNode();
};

typedef std::shared_ptr<LogicalOperatorNode> LogicalOperatorNodePtr;

class WindowDefinition;
typedef std::shared_ptr<WindowDefinition> WindowDefinitionPtr;

NodePtr createFilterLogicalOperatorNode(const PredicatePtr& predicate);
NodePtr createMapLogicalOperatorNode(const AttributeFieldPtr&, const PredicatePtr&);
NodePtr createSinkLogicalOperatorNode(const DataSinkPtr& sink);
NodePtr createSourceLogicalOperatorNode(const DataSourcePtr& source);
NodePtr createWindowLogicalOperatorNode(const WindowDefinitionPtr& windowDefinition);
}

#endif  // LOGICAL_OPERATOR_NODE_HPP
