#ifndef NES_INCLUDE_NODES_OPERATORS_SPECIALIZEDWINDOWOPERATORS_WINDOWCOMPUTATIONOPERATOR_HPP_
#define NES_INCLUDE_NODES_OPERATORS_SPECIALIZEDWINDOWOPERATORS_WINDOWCOMPUTATIONOPERATOR_HPP_
#include <Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Windowing/WindowLogicalOperatorNode.hpp>

namespace NES {

class LogicalWindowDefinition;
typedef std::shared_ptr<LogicalWindowDefinition> LogicalWindowDefinitionPtr;

/**
 * @brief this class represents the computation operator for distributed windowing that is deployed on the sink node and which merges all slices
 */
class WindowComputationOperator : public WindowLogicalOperatorNode {
  public:
    WindowComputationOperator(const LogicalWindowDefinitionPtr windowDefinition);

    bool equal(const NodePtr rhs) const override;
    const std::string toString() const override;
    OperatorNodePtr copy() override;
    bool isIdentical(NodePtr rhs) const override;
};

}// namespace NES
#endif//NES_INCLUDE_NODES_OPERATORS_SPECIALIZEDWINDOWOPERATORS_WINDOWCOMPUTATIONOPERATOR_HPP_
