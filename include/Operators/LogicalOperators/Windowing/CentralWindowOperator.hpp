#ifndef WINDOW_REFINEMENT_OPERATOR_CENTRAL_WINDOW_NODE_HPP
#define WINDOW_REFINEMENT_OPERATOR_CENTRAL_WINDOW_NODE_HPP

#include <Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Windowing/WindowLogicalOperatorNode.hpp>

namespace NES {

class LogicalWindowDefinition;
typedef std::shared_ptr<LogicalWindowDefinition> LogicalWindowDefinitionPtr;

class CentralWindowOperator : public WindowLogicalOperatorNode {
  public:
    CentralWindowOperator(const LogicalWindowDefinitionPtr windowDefinition);

    bool equal(const NodePtr rhs) const override;
    const std::string toString() const override;
    OperatorNodePtr copy() override;
    bool isIdentical(NodePtr rhs) const override;
    bool inferSchema() override;
};

}// namespace NES
#endif// WINDOW_LOGICAL_OPERATOR_NODE_HPP
