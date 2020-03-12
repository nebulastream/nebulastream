#ifndef LOGICAL_OPERATOR_NODE_HPP
#define LOGICAL_OPERATOR_NODE_HPP

#include <Nodes/Operators/OperatorNode.hpp>
#include <API/Window/WindowDefinition.hpp>
#include <API/ParameterTypes.hpp>
#include <API/Schema.hpp>

namespace NES {

class LogicalOperatorNode : public OperatorNode {
  public:
    LogicalOperatorNode();
};

typedef std::shared_ptr<LogicalOperatorNode> LogicalOperatorNodePtr;

NodePtr createFilterLogicalOperatorNode(const PredicatePtr& predicate);
NodePtr createMapLogicalOperatorNode(const AttributeFieldPtr&, const PredicatePtr&);
NodePtr createSinkLogicalOperatorNode(const DataSinkPtr& sink);
NodePtr createSourceLogicalOperatorNode(const DataSourcePtr& source);
NodePtr createWindowLogicalOperatorNode(const WindowDefinitionPtr& windowDefinition);


// #include <Nodes/Impl/FilterLogicalOperatorNode.hpp>
// #include <Nodes/Impl/SourceLogicalOperatorNode.hpp>
}

#endif  // LOGICAL_OPERATOR_NODE_HPP
