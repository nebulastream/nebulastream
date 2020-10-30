#ifndef WINDOW_REFINEMENT_OPERATOR_CENTRAL_WINDOW_NODE_HPP
#define WINDOW_REFINEMENT_OPERATOR_CENTRAL_WINDOW_NODE_HPP

#include <Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Windowing/WindowOperatorNode.hpp>

namespace NES {

class CentralWindowOperator : public WindowOperatorNode {
  public:
    CentralWindowOperator(const Windowing::LogicalWindowDefinitionPtr windowDefinition, OperatorId id);

    bool equal(const NodePtr rhs) const override;
    const std::string toString() const override;
    OperatorNodePtr copy() override;
    bool isIdentical(NodePtr rhs) const override;
    bool inferSchema() override;
    z3::expr getFOL() override;
};

}// namespace NES
#endif// WINDOW_LOGICAL_OPERATOR_NODE_HPP
