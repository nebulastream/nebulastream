#ifndef LOGICAL_OPERATOR_NODE_HPP
#define LOGICAL_OPERATOR_NODE_HPP

#include <Nodes/Expressions/ExpressionNode.hpp>
#include <Nodes/Operators/OperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/SinkDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/SourceDescriptor.hpp>
#include <API/ParameterTypes.hpp>

namespace NES {

class LogicalOperatorNode : public OperatorNode {
  public:
    LogicalOperatorNode();
};

typedef std::shared_ptr<LogicalOperatorNode> LogicalOperatorNodePtr;

class FieldAssignmentExpressionNode;
typedef std::shared_ptr<FieldAssignmentExpressionNode> FieldAssignmentExpressionNodePtr;

class WindowDefinition;
typedef std::shared_ptr<WindowDefinition> WindowDefinitionPtr;

LogicalOperatorNodePtr createFilterLogicalOperatorNode(const ExpressionNodePtr predicate);
LogicalOperatorNodePtr createSinkLogicalOperatorNode(const SinkDescriptorPtr sinkDescriptor);
LogicalOperatorNodePtr createMapLogicalOperatorNode(const FieldAssignmentExpressionNodePtr mapExpression);
LogicalOperatorNodePtr createSourceLogicalOperatorNode(const SourceDescriptorPtr sourceDescriptor);
LogicalOperatorNodePtr createWindowLogicalOperatorNode(const WindowDefinitionPtr windowDefinition);
}

#endif  // LOGICAL_OPERATOR_NODE_HPP
