#ifndef LOGICAL_OPERATOR_NODE_HPP
#define LOGICAL_OPERATOR_NODE_HPP

#include <Nodes/Expressions/ExpressionNode.hpp>
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

LogicalOperatorNodePtr createFilterLogicalOperatorNode(const ExpressionNodePtr predicate);
LogicalOperatorNodePtr createMapLogicalOperatorNode(const ExpressionNodePtr mapExpression);
LogicalOperatorNodePtr createSinkLogicalOperatorNode(const DataSinkPtr sink);
LogicalOperatorNodePtr createSourceLogicalOperatorNode(const DataSourcePtr source);
LogicalOperatorNodePtr createWindowLogicalOperatorNode(const WindowDefinitionPtr windowDefinition);
}

#endif  // LOGICAL_OPERATOR_NODE_HPP
