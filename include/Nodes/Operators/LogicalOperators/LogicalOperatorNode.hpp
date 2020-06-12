#ifndef LOGICAL_OPERATOR_NODE_HPP
#define LOGICAL_OPERATOR_NODE_HPP

#include <API/ParameterTypes.hpp>
#include <Nodes/Expressions/ExpressionNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/SinkDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/SourceDescriptor.hpp>
#include <Nodes/Operators/OperatorNode.hpp>

namespace NES {

class LogicalOperatorNode;
typedef std::shared_ptr<LogicalOperatorNode> LogicalOperatorNodePtr;

class LogicalOperatorNode : public OperatorNode {
  public:
    LogicalOperatorNode();
};

class FieldAssignmentExpressionNode;
typedef std::shared_ptr<FieldAssignmentExpressionNode> FieldAssignmentExpressionNodePtr;

class WindowDefinition;
typedef std::shared_ptr<WindowDefinition> WindowDefinitionPtr;

//FIXME: these Ids are used at deployment phase to alter the port info. Once we have the network sink and source we can get rid of them.
static uint64_t SYS_SOURCE_OPERATOR_ID = UINT64_MAX - 2;
static uint64_t SYS_SINK_OPERATOR_ID = UINT64_MAX - 1;

LogicalOperatorNodePtr createFilterLogicalOperatorNode(const ExpressionNodePtr predicate);
LogicalOperatorNodePtr createSinkLogicalOperatorNode(const SinkDescriptorPtr sinkDescriptor);
LogicalOperatorNodePtr createMapLogicalOperatorNode(const FieldAssignmentExpressionNodePtr mapExpression);
LogicalOperatorNodePtr createSourceLogicalOperatorNode(const SourceDescriptorPtr sourceDescriptor);
LogicalOperatorNodePtr createWindowLogicalOperatorNode(const WindowDefinitionPtr windowDefinition);
}// namespace NES

#endif// LOGICAL_OPERATOR_NODE_HPP
